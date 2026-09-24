import { describe, expect, it } from "vitest";
import { defineAggregate } from "./aggregate.js";
import { decide } from "./decide.js";
import { defineEvent } from "./events.js";
import { ProjectionAggregateMismatchError, ProjectionNotRebuildableError } from "./errors.js";
import { createInMemoryStore, type InMemoryWriter } from "./memory.js";
import { defineProjection, rebuild } from "./projection.js";
import { z } from "zod";

const created = defineEvent({ eventType: "widget.created" as const, latestRevision: 1, latest: z.object({ name: z.string() }) });

type State = { name: string } | null;
type Tables = { readonly widgets: State; readonly first: State; readonly second: State };

const widget = defineAggregate({
  type: "widget",
  events: [created],
  initialState: null,
  evolve: (_state: State, event): State => ({ name: event.payload.name }),
});

describe("projections", () => {
  it("apply receives the state folded after the append and writes the projection table", async () => {
    const projection = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "widgets",
      aggregate: widget,
      apply: async (ctx) => {
        ctx.writer.table("widgets").set(ctx.streamId, ctx.state);
      },
      wipe: async (writer) => {
        writer.table("widgets").clear();
      },
    });

    const store = createInMemoryStore<Tables>({ projections: [projection] });
    await decide(store, widget, "w1", () => ({ emit: [{ eventType: "widget.created" as const, payload: { name: "a" } }], result: null }));

    expect(store.writer.table("widgets").get("w1")).toEqual({ name: "a" });
  });

  it("decide rejects with ProjectionAggregateMismatchError when a registered projection is bound to another aggregate object", async () => {
    const twin = defineAggregate({
      type: "widget",
      events: [created],
      initialState: null,
      evolve: (_state: State, event): State => ({ name: event.payload.name }),
    });

    const projection = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "widgets",
      aggregate: twin,
      apply: async (ctx) => {
        ctx.writer.table("widgets").set(ctx.streamId, ctx.state);
      },
    });

    const store = createInMemoryStore<Tables>({ projections: [projection] });

    await expect(
      decide(store, widget, "w1", () => ({
        emit: [{ eventType: "widget.created" as const, payload: { name: "a" } }],
        result: null,
      })),
    ).rejects.toThrow(ProjectionAggregateMismatchError);

    expect(await store.readStream("w1")).toHaveLength(0);
  });

  it("a throwing projection rolls back its own writes and calls onProjectionError, without blocking the append", async () => {
    const errors: unknown[] = [];
    const projection = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "widgets",
      aggregate: widget,
      apply: async (ctx) => {
        ctx.writer.table("widgets").set("partial", { name: "x" });
        throw new Error("boom");
      },
    });

    const store = createInMemoryStore<Tables>({
      projections: [projection],
      onProjectionError: (error) => errors.push(error),
    });

    const result = await decide(store, widget, "w1", () => ({
      emit: [{ eventType: "widget.created" as const, payload: { name: "a" } }],
      result: null,
    }));

    expect(result.appended).toBe(true);
    expect(errors.length).toBe(1);
    expect(store.writer.table("widgets").has("partial")).toBe(false);
    expect((await store.readStream("w1")).length).toBe(1);
  });

  it("a second projection still runs when the first throws", async () => {
    const first = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "first",
      aggregate: widget,
      apply: async () => {
        throw new Error("boom");
      },
    });
    const second = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "second",
      aggregate: widget,
      apply: async (ctx) => {
        ctx.writer.table("second").set(ctx.streamId, ctx.state);
      },
    });

    const store = createInMemoryStore<Tables>({ projections: [first, second], onProjectionError: () => {} });
    await decide(store, widget, "w1", () => ({ emit: [{ eventType: "widget.created" as const, payload: { name: "a" } }], result: null }));

    expect(store.writer.table("second").get("w1")).toEqual({ name: "a" });
  });
});

describe("rebuild", () => {
  it("restores every row via wipe, and rebuilding twice is idempotent", async () => {
    const projection = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "widgets",
      aggregate: widget,
      apply: async (ctx) => {
        ctx.writer.table("widgets").set(ctx.streamId, ctx.state);
      },
      wipe: async (writer) => {
        writer.table("widgets").clear();
      },
    });

    const store = createInMemoryStore<Tables>({ projections: [projection] });
    await decide(store, widget, "w1", () => ({ emit: [{ eventType: "widget.created" as const, payload: { name: "a" } }], result: null }));
    await decide(store, widget, "w2", () => ({ emit: [{ eventType: "widget.created" as const, payload: { name: "b" } }], result: null }));

    // Corrupt the read table to prove rebuild restores it.
    store.writer.table("widgets").clear();
    store.writer.table("widgets").set("stale", { name: "garbage" });

    const result = await rebuild(store);
    expect(result).toEqual({ projections: ["widgets"], streams: 2 });
    expect(store.writer.table("widgets").get("w1")).toEqual({ name: "a" });
    expect(store.writer.table("widgets").get("w2")).toEqual({ name: "b" });
    expect(store.writer.table("widgets").has("stale")).toBe(false);

    const secondResult = await rebuild(store);
    expect(secondResult.streams).toBe(2);
    expect(store.writer.table("widgets").get("w1")).toEqual({ name: "a" });
  });

  it("throws ProjectionNotRebuildableError for a wipe-less projection before wiping anything", async () => {
    const wipeless = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "wipeless",
      aggregate: widget,
      apply: async () => {},
    });
    const store = createInMemoryStore<Tables>({ projections: [wipeless] });

    await expect(rebuild(store)).rejects.toThrow(ProjectionNotRebuildableError);
  });

  it("rebuild with { projection: name } rebuilds only the named projection", async () => {
    const first = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "first",
      aggregate: widget,
      apply: async (ctx) => {
        ctx.writer.table("first").set(ctx.streamId, ctx.state);
      },
      wipe: async (writer) => {
        writer.table("first").clear();
      },
    });
    const second = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "second",
      aggregate: widget,
      apply: async (ctx) => {
        ctx.writer.table("second").set(ctx.streamId, ctx.state);
      },
      wipe: async (writer) => {
        writer.table("second").clear();
      },
    });

    const store = createInMemoryStore<Tables>({ projections: [first, second] });
    await decide(store, widget, "w1", () => ({ emit: [{ eventType: "widget.created" as const, payload: { name: "a" } }], result: null }));

    // Corrupt both tables; only "first" should be restored by a scoped rebuild.
    store.writer.table("first").clear();
    store.writer.table("first").set("stale", { name: "garbage" });
    store.writer.table("second").clear();
    store.writer.table("second").set("stale", { name: "garbage" });

    const result = await rebuild(store, { projection: "first" });

    expect(result).toEqual({ projections: ["first"], streams: 1 });
    expect(store.writer.table("first").get("w1")).toEqual({ name: "a" });
    expect(store.writer.table("second").has("stale")).toBe(true);
  });

  it("a rebuild started while a decide holds the shared lock waits for it, and neither loses the other's work", async () => {
    const log: string[] = [];
    const { promise: decideInApply, resolve: markInApply } = Promise.withResolvers<void>();
    const { promise: releaseApply, resolve: letApplyFinish } = Promise.withResolvers<void>();
    let gateNextApply = false;

    const projection = defineProjection<State, readonly [typeof created], InMemoryWriter<Tables>>({
      name: "widgets",
      aggregate: widget,
      apply: async (ctx) => {
        if (gateNextApply) {
          gateNextApply = false;
          markInApply();
          await releaseApply;
          log.push("decide-applied");
        }
        ctx.writer.table("widgets").set(ctx.streamId, ctx.state);
      },
      wipe: async (writer) => {
        log.push("rebuild-wiped");
        writer.table("widgets").clear();
      },
    });

    const store = createInMemoryStore<Tables>({ projections: [projection] });
    await decide(store, widget, "w1", () => ({ emit: [{ eventType: "widget.created" as const, payload: { name: "a" } }], result: null }));

    gateNextApply = true;
    const pending = decide(store, widget, "w2", () => ({ emit: [{ eventType: "widget.created" as const, payload: { name: "b" } }], result: null }));
    await decideInApply;

    const rebuilding = rebuild(store);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(log).toEqual([]);

    letApplyFinish();
    await Promise.all([pending, rebuilding]);

    expect(log).toEqual(["decide-applied", "rebuild-wiped"]);
    expect(store.writer.table("widgets").get("w1")).toEqual({ name: "a" });
    expect(store.writer.table("widgets").get("w2")).toEqual({ name: "b" });
  });

  it("shared holders overlap, and an exclusive request waits for all of them", async () => {
    const store = createInMemoryStore();
    const log: string[] = [];
    const { promise: release, resolve: releaseShared } = Promise.withResolvers<void>();
    const { promise: bothHeld, resolve: markBothHeld } = Promise.withResolvers<void>();
    let held = 0;

    const shared = (name: string) =>
      store.transaction(async (tx) => {
        await tx.lockProjectionShared("widgets");
        log.push(`${name}-held`);
        if (++held === 2) markBothHeld();
        await release;
      });

    const a = shared("a");
    const b = shared("b");
    await bothHeld;

    const exclusive = store.transaction(async (tx) => {
      await tx.lockProjectionExclusive("widgets");
      log.push("exclusive-held");
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(log).toEqual(["a-held", "b-held"]);

    releaseShared();
    await Promise.all([a, b, exclusive]);
    expect(log).toEqual(["a-held", "b-held", "exclusive-held"]);
  });
});
