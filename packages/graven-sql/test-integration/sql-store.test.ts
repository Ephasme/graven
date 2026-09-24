import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { decide, defineAggregate, defineEvent, defineProjection, DuplicateEventIdError, GravenError, rebuild } from "@les-sherpas/graven";
import { createSqlEventStore, type SqlEventStore } from "@les-sherpas/graven-sql";
import { Kysely, MysqlDialect, PostgresDialect, sql } from "kysely";
import mysql2 from "mysql2";
import pg from "pg";

const POSTGRES_URL = process.env.GRAVEN_POSTGRES_URL;
const MYSQL_URL = process.env.GRAVEN_MYSQL_URL;

if (!POSTGRES_URL) {
  throw new Error("GRAVEN_POSTGRES_URL must be set for graven-sql integration tests");
}
if (!MYSQL_URL) {
  throw new Error("GRAVEN_MYSQL_URL must be set for graven-sql integration tests");
}

const pgDb = new Kysely<any>({ dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: POSTGRES_URL }) }) });
// No `timezone` option: the store must keep `occurred_at` right whatever the pool converts.
const mysqlDb = new Kysely<any>({ dialect: new MysqlDialect({ pool: mysql2.createPool({ uri: MYSQL_URL }) }) });

const TABLES = ["graven_events", "graven_streams", "graven_checkpoints", "graven_locks"];

const cases = [
  {
    name: "postgres (pg)",
    db: pgDb,
    truncate: () => sql`truncate ${sql.join(TABLES.map((t) => sql.table(t)))}`.execute(pgDb),
  },
  {
    name: "mariadb (mysql2)",
    db: mysqlDb,
    truncate: async () => {
      for (const t of TABLES) await mysqlDb.deleteFrom(t).execute();
    },
  },
];

describe.each(cases)("SqlEventStore ($name)", ({ db, truncate }) => {
  let store: SqlEventStore<any>;

  beforeEach(async () => {
    store = createSqlEventStore({ db });
    await store.migrate();
    await truncate();
    await db.updateTable("graven_positions").set({ position: 0 }).where("id", "=", 1).execute();
  });

  function event(eventId: string) {
    return { eventId, eventType: "widget.created", eventRevision: 1, payload: { note: "hi" }, occurredAt: new Date() };
  }

  const scratchRow = (key: string) =>
    db.selectFrom("graven_locks").select("lock_key").where("lock_key", "=", key).executeTakeFirst();

  it("migrate() twice in a row is a no-op the second time", async () => {
    await store.migrate();
    expect(await store.currentPosition()).toBe(0);
  });

  it("appends and reads a stream in version order", async () => {
    await store.append({
      streamId: "s1",
      aggregateType: "widget",
      expected: { kind: "no-stream" },
      events: [event("e1"), event("e2")],
      metadata: { correlationId: "c1" },
    });

    const rows = await store.readStream("s1");
    expect(rows.map((r) => r.streamVersion)).toEqual([1, 2]);
    expect(await store.currentPosition()).toBe(2);
  });

  it("a repeated event id is reported as DuplicateEventIdError, not a raw driver error", async () => {
    const append = (streamId: string) =>
      store.append({
        streamId,
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("same-id")],
        metadata: { correlationId: streamId },
      });

    await append("s1");
    await expect(append("s2")).rejects.toThrow(DuplicateEventIdError);
  });

  it("20 concurrent decide-style appends on one stream leave 20 gap-free positions", async () => {
    // Retries only a GravenError, the expected outcome of racing one optimistic version. A
    // raw driver error such as a deadlock is rethrown, so 20/20 here is a liveness proof.
    const append = async (index: number) => {
      for (let attempt = 1; ; attempt++) {
        try {
          await store.transaction(async (tx) => {
            await tx.lockStream("s1");
            const existing = await tx.readStream("s1");
            await tx.append({
              streamId: "s1",
              aggregateType: "widget",
              expected: existing.length === 0 ? { kind: "no-stream" } : { kind: "exact", version: existing.length },
              events: [event(`e${index}`)],
              metadata: { correlationId: `c${index}` },
            });
          });
          return;
        } catch (error) {
          if (!(error instanceof GravenError) || attempt > 50) throw error;
        }
      }
    };

    await Promise.all(Array.from({ length: 20 }, (_, i) => append(i)));

    const expected = Array.from({ length: 20 }, (_, i) => i + 1);
    expect((await store.readStream("s1")).map((r) => r.streamVersion)).toEqual(expected);
    expect((await store.readGlobal({ fromPosition: 0, limit: 100 })).map((r) => r.globalPosition)).toEqual(expected);
  });

  it("lockStream blocks a concurrent decide until the holder commits", async () => {
    const order: string[] = [];
    const { promise: gate, resolve: openGate } = Promise.withResolvers<void>();

    const first = store.transaction(async (tx) => {
      await tx.lockStream("s1");
      await gate;
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
      order.push("first-committed");
    });
    await new Promise((resolve) => setTimeout(resolve, 50));

    const second = store.transaction(async (tx) => {
      await tx.lockStream("s1");
      order.push("second-locked");
      const existing = await tx.readStream("s1");
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "exact", version: existing.length },
        events: [event("e2")],
        metadata: { correlationId: "c2" },
      });
    });

    openGate();
    await Promise.all([first, second]);

    expect(order).toEqual(["first-committed", "second-locked"]);
    expect((await store.readStream("s1")).map((r) => r.streamVersion)).toEqual([1, 2]);
  });

  it("readGlobal orders by position, and readStreams handles an unknown id", async () => {
    await store.append({
      streamId: "s1",
      aggregateType: "widget",
      expected: { kind: "no-stream" },
      events: [event("e1"), event("e2")],
      metadata: { correlationId: "c1" },
    });

    expect((await store.readGlobal({ fromPosition: 0, limit: 100 })).map((e) => e.eventId)).toEqual(["e1", "e2"]);
    const map = await store.readStreams(["s1", "unknown"]);
    expect(map.get("s1")?.length).toBe(2);
    expect(map.get("unknown")).toEqual([]);
  });

  it("read filters: version range, aggregate type, stream ids by type, and an empty id list", async () => {
    await store.append({
      streamId: "w1",
      aggregateType: "widget",
      expected: { kind: "no-stream" },
      events: [event("e1"), event("e2"), event("e3")],
      metadata: { correlationId: "c1" },
    });
    await store.append({
      streamId: "g1",
      aggregateType: "gadget",
      expected: { kind: "no-stream" },
      events: [event("e4")],
      metadata: { correlationId: "c2" },
    });

    expect((await store.readStream("w1", { fromVersion: 2, toVersion: 2 })).map((e) => e.eventId)).toEqual(["e2"]);
    expect(
      (await store.readGlobal({ fromPosition: 0, limit: 10, aggregateType: "gadget" })).map((e) => e.eventId),
    ).toEqual(["e4"]);
    expect((await store.readGlobal({ fromPosition: 1, limit: 2 })).map((e) => e.eventId)).toEqual(["e2", "e3"]);
    expect(await store.readStreamIds("widget")).toEqual(["w1"]);
    expect((await store.readStreams([])).size).toBe(0);
  });

  it("decide projects inline and rebuild restores the read table from the log", async () => {
    await db.schema.dropTable("widgets_read").ifExists().execute();
    await db.schema
      .createTable("widgets_read")
      .addColumn("id", "varchar(255)", (c) => c.primaryKey())
      .addColumn("name", "varchar(255)", (c) => c.notNull())
      .execute();

    const created = defineEvent({ eventType: "widget.created" as const, latestRevision: 1, latest: z.object({ name: z.string() }) });
    const widget = defineAggregate({
      type: "widget",
      events: [created],
      initialState: null as { name: string } | null,
      evolve: (_state, e) => ({ name: e.payload.name }),
    });
    const projection = defineProjection<{ name: string } | null, [typeof created], Kysely<any>>({
      name: "widgets",
      aggregate: widget,
      apply: async ({ streamId, state, writer }) => {
        await writer.deleteFrom("widgets_read").where("id", "=", streamId).execute();
        if (state) await writer.insertInto("widgets_read").values({ id: streamId, name: state.name }).execute();
      },
      wipe: async (writer) => {
        await writer.deleteFrom("widgets_read").execute();
      },
    });
    const projected = createSqlEventStore({ db, projections: [projection] });
    const readTable = () => db.selectFrom("widgets_read").select(["id", "name"]).orderBy("id").execute();

    for (const [id, name] of [["w1", "a"], ["w2", "b"]] as const) {
      await decide(projected, widget, id, () => ({ emit: [{ eventType: "widget.created" as const, payload: { name } }], result: null }));
    }
    expect(await readTable()).toEqual([{ id: "w1", name: "a" }, { id: "w2", name: "b" }]);

    await db.deleteFrom("widgets_read").where("id", "=", "w1").execute();
    await db.insertInto("widgets_read").values({ id: "stale", name: "x" }).execute();
    expect(await rebuild(projected)).toEqual({ projections: ["widgets"], streams: 2 });
    expect(await readTable()).toEqual([{ id: "w1", name: "a" }, { id: "w2", name: "b" }]);

    await db.schema.dropTable("widgets_read").execute();
  });

  it("a throw inside a savepoint rolls back its writes while the append survives", async () => {
    await store.transaction(async (tx) => {
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
      await expect(
        tx.savepoint(async () => {
          await tx.writer.insertInto("graven_locks").values({ lock_key: "partial" }).execute();
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");
    });

    expect((await store.readStream("s1")).length).toBe(1);
    expect(await scratchRow("partial")).toBeUndefined();
  });

  it("a store built over a caller's Transaction joins it and rolls back with it", async () => {
    await expect(
      db.transaction().execute(async (trx) => {
        await createSqlEventStore({ db: trx }).transaction(async (tx) => {
          await tx.lockStream("s1");
          await tx.append({
            streamId: "s1",
            aggregateType: "widget",
            expected: { kind: "no-stream" },
            events: [event("e1")],
            metadata: { correlationId: "c1" },
          });
        });
        throw new Error("caller rollback");
      }),
    ).rejects.toThrow("caller rollback");

    expect(await store.readStream("s1")).toEqual([]);
  });

  it("a Date with milliseconds round-trips through occurred_at byte-identical", async () => {
    const occurredAt = new Date("2026-03-01T12:34:56.789Z");
    await store.append({
      streamId: "s1",
      aggregateType: "widget",
      expected: { kind: "no-stream" },
      events: [{ ...event("e1"), occurredAt }],
      metadata: { correlationId: "c1" },
    });

    const [row] = await store.readStream("s1");
    expect(row?.event.occurredAt.toISOString()).toBe(occurredAt.toISOString());
  });

  it.each([
    ["a string", "it's $1 ? --"],
    ["a numeric string", "42"],
    ["an array", [1, "a", { b: null }]],
    ["a number", 42],
    ["null", null],
    ["an object", { note: "it's", nested: { a: [1] } }],
  ])("a payload that is %s round-trips unchanged", async (_label, payload) => {
    await store.append({
      streamId: "s1",
      aggregateType: "widget",
      expected: { kind: "no-stream" },
      events: [{ ...event("e1"), payload }],
      metadata: { correlationId: "c1" },
    });

    const [row] = await store.readStream("s1");
    expect(row?.event.payload).toEqual(payload);
  });

  it("checkpoints default to 0 and an overwrite keeps the latest position", async () => {
    expect(await store.readCheckpoint("proj")).toBe(0);
    await store.writeCheckpoint("proj", 3);
    await store.writeCheckpoint("proj", 7);
    expect(await store.readCheckpoint("proj")).toBe(7);
  });

  it("a rebuild waits for a decide holding the shared lock, and the decide's write survives it", async () => {
    const order: string[] = [];
    const { promise: decideHolds, resolve: markHolds } = Promise.withResolvers<void>();
    const { promise: releaseDecide, resolve: letDecideCommit } = Promise.withResolvers<void>();

    const deciding = store.transaction(async (tx) => {
      await tx.lockProjectionShared("widgets");
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
      markHolds();
      await releaseDecide;
      await tx.writer.insertInto("graven_locks").values({ lock_key: "row:s1" }).execute();
      order.push("decide-projected");
    });
    await decideHolds;

    const rebuilding = store.transaction(async (tx) => {
      await tx.lockProjectionExclusive("widgets");
      order.push("rebuild-locked");
      await tx.writer.deleteFrom("graven_locks").where("lock_key", "=", "row:s1").execute();
      if ((await tx.readStream("s1")).length > 0) {
        await tx.writer.insertInto("graven_locks").values({ lock_key: "row:s1" }).execute();
      }
    });

    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(order).toEqual([]);
    letDecideCommit();
    await Promise.all([deciding, rebuilding]);

    expect(order).toEqual(["decide-projected", "rebuild-locked"]);
    expect(await scratchRow("row:s1")).toBeDefined();
  });
});

afterAll(async () => {
  await pgDb.destroy();
  await mysqlDb.destroy();
});
