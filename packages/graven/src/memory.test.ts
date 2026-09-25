import { describe, expect, it } from "vitest";
import { defineAggregate } from "./aggregate.js";
import { decide } from "./decide.js";
import { defineEvent } from "./events.js";
import { StaleStreamVersionError, DuplicateEventIdError } from "./errors.js";
import { createInMemoryStore } from "./memory.js";
import { defineProjection } from "./projection.js";
import { z } from "zod";

type Tables = { readonly t: number };

function event(eventId: string, eventType = "widget.created") {
  return { eventId, eventType, eventRevision: 1, payload: {}, occurredAt: new Date() };
}

describe("createInMemoryStore", () => {
  it("appends and reads a stream in version order", async () => {
    const store = createInMemoryStore();

    const result = await store.transaction(async (tx) => {
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
      return tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "exact", version: 1 },
        events: [event("e2")],
        metadata: { correlationId: "c2" },
      });
    });

    expect(result.newVersion).toBe(2);

    const events = await store.readStream("s1");
    expect(events.map((e) => e.streamVersion)).toEqual([1, 2]);
    expect(await store.currentPosition()).toBe(2);
  });

  it("throws StaleStreamVersionError on a version mismatch", async () => {
    const store = createInMemoryStore();
    await store.transaction((tx) =>
      tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      }),
    );

    await expect(
      store.transaction((tx) =>
        tx.append({
          streamId: "s1",
          aggregateType: "widget",
          expected: { kind: "exact", version: 0 },
          events: [event("e2")],
          metadata: { correlationId: "c2" },
        }),
      ),
    ).rejects.toThrow(StaleStreamVersionError);
  });

  it("throws DuplicateEventIdError on a repeated event id", async () => {
    const store = createInMemoryStore();
    await store.transaction((tx) =>
      tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("dup")],
        metadata: { correlationId: "c1" },
      }),
    );

    await expect(
      store.transaction((tx) =>
        tx.append({
          streamId: "s2",
          aggregateType: "widget",
          expected: { kind: "no-stream" },
          events: [event("dup")],
          metadata: { correlationId: "c2" },
        }),
      ),
    ).rejects.toThrow(DuplicateEventIdError);
  });

  it("rolls back an append when the transaction throws", async () => {
    const store = createInMemoryStore();

    await expect(
      store.transaction(async (tx) => {
        await tx.append({
          streamId: "s1",
          aggregateType: "widget",
          expected: { kind: "no-stream" },
          events: [event("e1")],
          metadata: { correlationId: "c1" },
        });
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    expect(await store.readStream("s1")).toEqual([]);
    expect(await store.currentPosition()).toBe(0);
  });

  it("serialises two concurrent appends on the same stream", async () => {
    const store = createInMemoryStore();

    const decide = async (eventId: string) =>
      store.transaction(async (tx) => {
        await tx.lockStream("s1");
        const existing = await tx.readStream("s1");
        const version = existing.length === 0 ? { kind: "no-stream" as const } : { kind: "exact" as const, version: existing.length };
        return tx.append({
          streamId: "s1",
          aggregateType: "widget",
          expected: version,
          events: [event(eventId)],
          metadata: { correlationId: eventId },
        });
      });

    await Promise.all([decide("a"), decide("b")]);

    const events = await store.readStream("s1");
    expect(events.map((e) => e.streamVersion).sort((a, b) => a - b)).toEqual([1, 2]);
    expect(await store.currentPosition()).toBe(2);
  });

  it("readGlobal filters by aggregateType and respects fromPosition/limit", async () => {
    const store = createInMemoryStore();
    await store.transaction(async (tx) => {
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1"), event("e2")],
        metadata: { correlationId: "c1" },
      });
      await tx.append({
        streamId: "s2",
        aggregateType: "gadget",
        expected: { kind: "no-stream" },
        events: [event("e3", "gadget.created")],
        metadata: { correlationId: "c2" },
      });
    });

    const widgets = await store.readGlobal({ fromPosition: 0, limit: 10, aggregateType: "widget" });
    expect(widgets.map((e) => e.eventId)).toEqual(["e1", "e2"]);

    const afterOne = await store.readGlobal({ fromPosition: 1, limit: 10 });
    expect(afterOne.map((e) => e.eventId)).toEqual(["e2", "e3"]);
  });

  it("checkpoints default to 0 and persist across transactions", async () => {
    const store = createInMemoryStore();
    expect(await store.readCheckpoint("proj")).toBe(0);

    await store.transaction((tx) => tx.writeCheckpoint("proj", 5));
    expect(await store.readCheckpoint("proj")).toBe(5);
  });

  it("savepoint rolls back writer table mutations on throw without rolling back the transaction", async () => {
    const store = createInMemoryStore<Tables>();

    await store.transaction(async (tx) => {
      tx.writer.table("t").set("a", 1);
      await expect(
        tx.savepoint(async () => {
          tx.writer.table("t").set("b", 2);
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");

      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
    });

    // The savepoint's "b" write was rolled back; "a" (written before the savepoint) and the
    // append that ran after it both survive because the outer transaction committed.
    expect(store.writer.table("t").has("b")).toBe(false);
    expect(store.writer.table("t").get("a")).toBe(1);
    expect((await store.readStream("s1")).length).toBe(1);
  });

  it("a transaction that throws leaves rows another transaction committed meanwhile intact", async () => {
    const store = createInMemoryStore<Tables>();
    const { promise: bWrote, resolve: markBWrote } = Promise.withResolvers<void>();
    const { promise: aCommitted, resolve: markACommitted } = Promise.withResolvers<void>();

    const b = store.transaction(async (tx) => {
      tx.writer.table("t").set("b", 2);
      markBWrote();
      await aCommitted;
      throw new Error("boom");
    });

    await bWrote;
    await store.transaction(async (tx) => {
      tx.writer.table("t").set("a", 1);
    });
    markACommitted();
    await expect(b).rejects.toThrow("boom");

    expect(store.writer.table("t").get("a")).toBe(1);
    expect(store.writer.table("t").has("b")).toBe(false);
  });

  it("two transactions writing different rows of one table both keep their rows on commit", async () => {
    const store = createInMemoryStore<Tables>();
    const { promise: bothStaged, resolve: markStaged } = Promise.withResolvers<void>();
    let staged = 0;
    const stage = () => {
      staged++;
      if (staged === 2) markStaged();
    };

    const write = (key: string, value: number) =>
      store.transaction(async (tx) => {
        tx.writer.table("t").set(key, value);
        stage();
        await bothStaged;
      });

    await Promise.all([write("a", 1), write("b", 2)]);

    expect(store.writer.table("t").get("a")).toBe(1);
    expect(store.writer.table("t").get("b")).toBe(2);
  });

  it("an uncommitted write is invisible outside its transaction", async () => {
    const store = createInMemoryStore<Tables>();
    const { promise: written, resolve: markWritten } = Promise.withResolvers<void>();
    const { promise: checked, resolve: markChecked } = Promise.withResolvers<void>();

    const tx = store.transaction(async (tx) => {
      tx.writer.table("t").set("a", 1);
      markWritten();
      await checked;
    });

    await written;
    expect(store.writer.table("t").has("a")).toBe(false);
    markChecked();
    await tx;
    expect(store.writer.table("t").get("a")).toBe(1);
  });

  it("append called directly on the unbound store wraps itself in a transaction", async () => {
    const store = createInMemoryStore();

    const result = await store.append({
      streamId: "s1",
      aggregateType: "widget",
      expected: { kind: "no-stream" },
      events: [event("e1")],
      metadata: { correlationId: "c1" },
    });

    expect(result.newVersion).toBe(1);
    expect((await store.readStream("s1")).length).toBe(1);
  });

  it("writeCheckpoint called directly on the unbound store wraps itself in a transaction", async () => {
    const store = createInMemoryStore();

    await store.writeCheckpoint("proj", 7);

    expect(await store.readCheckpoint("proj")).toBe(7);
  });

  it("readGlobal with no aggregateType filter returns events across every aggregate type", async () => {
    const store = createInMemoryStore();
    await store.transaction(async (tx) => {
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
      await tx.append({
        streamId: "s2",
        aggregateType: "gadget",
        expected: { kind: "no-stream" },
        events: [event("e2", "gadget.created")],
        metadata: { correlationId: "c2" },
      });
    });

    const all = await store.readGlobal({ fromPosition: 0, limit: 10 });
    expect(all.map((e) => e.eventId)).toEqual(["e1", "e2"]);
  });

  it("effectiveEvents with no streamId (via readGlobal) includes both committed and pending events in one transaction", async () => {
    const store = createInMemoryStore();
    await store.transaction(async (tx) => {
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
    });

    await store.transaction(async (tx) => {
      await tx.append({
        streamId: "s2",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e2")],
        metadata: { correlationId: "c2" },
      });
      // Inside this same transaction, readGlobal must see both the already-committed e1
      // and the not-yet-committed e2.
      const seen = await tx.readGlobal({ fromPosition: 0, limit: 10 });
      expect(seen.map((e) => e.eventId)).toEqual(["e1", "e2"]);
    });
  });

  it("throws StaleStreamVersionError with expected=0 when appending no-stream against an existing stream", async () => {
    const store = createInMemoryStore();
    await store.transaction((tx) =>
      tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      }),
    );

    await expect(
      store.transaction((tx) =>
        tx.append({
          streamId: "s1",
          aggregateType: "widget",
          expected: { kind: "no-stream" },
          events: [event("e2")],
          metadata: { correlationId: "c2" },
        }),
      ),
    ).rejects.toThrow(StaleStreamVersionError);
  });

  it("readStream inside a transaction sees pending (uncommitted) events for that stream", async () => {
    const store = createInMemoryStore();
    await store.transaction(async (tx) => {
      await tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1")],
        metadata: { correlationId: "c1" },
      });
      // Not yet committed: only visible via this same transaction's pendingEvents.
      const seen = await tx.readStream("s1");
      expect(seen.map((e) => e.eventId)).toEqual(["e1"]);
    });
  });

  it("transaction() called from inside an already-bound transaction runs fn against the same tx, without nesting", async () => {
    const store = createInMemoryStore();

    const result = await store.transaction(async (tx) => {
      return tx.transaction(async (innerTx) => {
        expect(innerTx).toBe(tx);
        return innerTx.append({
          streamId: "s1",
          aggregateType: "widget",
          expected: { kind: "no-stream" },
          events: [event("e1")],
          metadata: { correlationId: "c1" },
        });
      });
    });

    expect(result.newVersion).toBe(1);
    expect((await store.readStream("s1")).length).toBe(1);
  });

  it("a throwing projection with no custom onProjectionError falls through to the default handler without crashing decide", async () => {
    const created = defineEvent({ eventType: "widget.created" as const, latestRevision: 1, latest: z.object({}) });
    const widget = defineAggregate({
      type: "widget",
      events: [created],
      initialState: null,
      evolve: (_state: { readonly seen: true } | null): { readonly seen: true } | null => ({ seen: true }),
    });
    const projection = defineProjection({
      name: "widgets",
      aggregate: widget,
      apply: async () => {
        throw new Error("boom");
      },
    });

    // Exercises the default onProjectionError (console.error), not a custom one: proves
    // createInMemoryStore's default handler doesn't itself throw or block the append.
    const store = createInMemoryStore({ projections: [projection] });
    const result = await decide(store, widget, "s1", () => ({ emit: [{ eventType: "widget.created" as const, payload: {} }], result: null }));

    expect(result.appended).toBe(true);
  });

  it("a shared projection lock upgraded to exclusive blocks another transaction's shared lock until commit", async () => {
    const store = createInMemoryStore();
    const order: string[] = [];
    const { promise: upgraded, resolve: markUpgraded } = Promise.withResolvers<void>();
    const holder = store.transaction(async (tx) => {
      await tx.lockProjectionShared("p");
      await tx.lockProjectionExclusive("p");
      markUpgraded();
      await new Promise((resolve) => setTimeout(resolve, 20));
      order.push("holder-commits");
    });
    await upgraded;
    const reader = store.transaction(async (tx) => {
      await tx.lockProjectionShared("p");
      order.push("reader-locked");
    });
    await Promise.all([holder, reader]);
    expect(order).toEqual(["holder-commits", "reader-locked"]);
  });
});
