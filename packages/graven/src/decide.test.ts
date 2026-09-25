import { describe, expect, it } from "vitest";
import { defineAggregate } from "./aggregate.js";
import { decide } from "./decide.js";
import { defineEvent } from "./events.js";
import { ConcurrentModificationError, StaleStreamVersionError } from "./errors.js";
import { createInMemoryStore } from "./memory.js";
import { defineProjection } from "./projection.js";
import { interceptAppend } from "../test/intercept-append.js";
import { z } from "zod";

const created = defineEvent({ eventType: "counter.created" as const, latestRevision: 1, latest: z.object({}) });
const incremented = defineEvent({
  eventType: "counter.incremented" as const,
  latestRevision: 1,
  latest: z.object({ by: z.number() }),
});

type State = { count: number } | null;

function makeCounter(elideNoOps = false) {
  return defineAggregate({
    type: "counter",
    events: [created, incremented],
    initialState: null,
    elideNoOps,
    evolve: (state: State, event): State => {
      switch (event.eventType) {
        case "counter.created":
          return { count: 0 };
        case "counter.incremented":
          return event.payload.by === 0 ? (state ?? { count: 0 }) : { count: (state?.count ?? 0) + event.payload.by };
      }
    },
  });
}

describe("decide", () => {
  it("appends and increments the version across two calls", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter();

    const first = await decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: "created" }));
    expect(first).toMatchObject({ appended: true, version: 1 });

    const second = await decide(store, counter, "c1", (state) => ({
      emit: [{ eventType: "counter.incremented" as const, payload: { by: 5 } }],
      result: state,
    }));
    expect(second).toMatchObject({ appended: true, version: 2 });

    const events = await store.readStream("c1");
    expect(events.map((e) => e.streamVersion)).toEqual([1, 2]);
  });

  it("returns refused and appends nothing when the decision returns null", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter();

    const result = await decide(store, counter, "c1", () => null);
    expect(result).toEqual({ appended: false, reason: "refused" });
    expect(await store.currentPosition()).toBe(0);
  });

  it("elides a no-op event and returns reason no-op, leaving position unchanged", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter(true);

    await decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null }));
    const positionAfterCreate = await store.currentPosition();

    const result = await decide(store, counter, "c1", () => ({
      emit: [{ eventType: "counter.incremented" as const, payload: { by: 0 } }],
      result: null,
    }));

    expect(result).toEqual({ appended: false, reason: "no-op" });
    expect(await store.currentPosition()).toBe(positionAfterCreate);
  });

  it("runs two concurrent decides on one stream without losing either event", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter();

    await decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null }));

    await Promise.all([
      decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.incremented" as const, payload: { by: 1 } }], result: null })),
      decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.incremented" as const, payload: { by: 1 } }], result: null })),
    ]);

    const events = await store.readStream("c1");
    expect(events.length).toBe(3);
    expect(await store.currentPosition()).toBe(3);
  });

  it("throws StaleStreamVersionError from a direct append against a stale expected version", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter();

    await decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null }));
    await decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.incremented" as const, payload: { by: 1 } }], result: null }));

    await expect(
      store.transaction((tx) =>
        tx.append({
          streamId: "c1",
          aggregateType: "counter",
          expected: { kind: "exact", version: 0 },
          events: [{ eventId: "x", eventType: "counter.incremented" as const, eventRevision: 1, payload: { by: 1 }, occurredAt: new Date() }],
          metadata: { correlationId: "x" },
        }),
      ),
    ).rejects.toThrow(StaleStreamVersionError);
  });

  it("passes a synthetic clock's occurredAt through to evolve", async () => {
    const store = createInMemoryStore();
    const fixed = new Date("2026-01-01T00:00:00.000Z");
    const seenTimes: Date[] = [];
    const counter = defineAggregate({
      type: "counter",
      events: [created],
      initialState: null,
      evolve: (state: State, event): State => {
        seenTimes.push(event.occurredAt);
        return state ?? { count: 0 };
      },
    });

    await decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null }), {
      clock: () => fixed,
    });

    const stored = await store.readStream("c1");
    expect(stored[0]?.event.occurredAt).toEqual(fixed);
  });

  it("retries on StaleStreamVersionError and succeeds within maxAttempts", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter();

    await decide(store, counter, "c1", () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null }));

    // Wraps the real store so the first append attempt fails with StaleStreamVersionError
    // (simulating a concurrent writer), forcing `decide`'s retry loop to run a second attempt
    // against the same store, which then succeeds.
    let appendCalls = 0;
    const flaky = interceptAppend(store, async (request, tx) => {
      appendCalls++;
      if (appendCalls === 1) {
        throw new StaleStreamVersionError(request.streamId, 0, 1);
      }
      return tx.append(request);
    });

    const result = await decide(flaky, counter, "c1", () => ({
      emit: [{ eventType: "counter.incremented" as const, payload: { by: 1 } }],
      result: null,
    }), { initialDelayMs: 1 });

    expect(result.appended).toBe(true);
    expect(appendCalls).toBe(2);
  });

  it("throws ConcurrentModificationError after exhausting maxAttempts on persistent StaleStreamVersionError", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter();

    const alwaysStale = interceptAppend(store, async (request) => {
      throw new StaleStreamVersionError(request.streamId, 0, 1);
    });

    await expect(
      decide(
        alwaysStale,
        counter,
        "c1",
        () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null }),
        { maxAttempts: 2, initialDelayMs: 1 },
      ),
    ).rejects.toThrow(ConcurrentModificationError);
  });

  it("propagates a non-retryable error from append without retrying", async () => {
    const store = createInMemoryStore();
    const counter = makeCounter();

    let attempts = 0;
    const broken = interceptAppend(store, async () => {
      attempts++;
      throw new Error("boom");
    });

    await expect(
      decide(broken, counter, "c1", () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null })),
    ).rejects.toThrow("boom");
    expect(attempts).toBe(1);
  });

  it("skips inline projections entirely when options.runInlineProjections is false, even for a projection built over a different aggregate object", async () => {
    const rows: string[] = [];
    const counter = makeCounter();
    const otherCounter = makeCounter();
    const projection = defineProjection({
      name: "widgets",
      aggregate: otherCounter,
      apply: async (ctx) => {
        rows.push(ctx.streamId);
      },
    });
    const store = createInMemoryStore({ projections: [projection] });

    const result = await decide(
      store,
      counter,
      "c1",
      () => ({ emit: [{ eventType: "counter.created" as const, payload: {} }], result: null }),
      { runInlineProjections: false },
    );

    expect(result.appended).toBe(true);
    expect(rows).toEqual([]);
  });
});
