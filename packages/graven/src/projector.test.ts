import { describe, expect, it, vi } from "vitest";
import { createInMemoryStore } from "./memory.js";
import type { InMemoryWriter } from "./memory-tables.js";
import { defineProjector, runProjector, startProjectors } from "./projector.js";
import { DuplicateProjectorError } from "./errors.js";
import { wallet, type WalletState } from "../test/fixtures/wallet/aggregate.js";
import { walletEvents } from "../test/fixtures/wallet/events.js";

type W = InMemoryWriter<Record<never, never>>;

function event(eventId: string) {
  return { eventId, eventType: "widget.created", eventRevision: 1, payload: {}, occurredAt: new Date() };
}

describe("runProjector", () => {
  it("consumes every appended event and advances the checkpoint with zero lag", async () => {
    const store = createInMemoryStore();
    await store.transaction((tx) =>
      tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1"), event("e2"), event("e3")],
        metadata: { correlationId: "c1" },
      }),
    );

    const rows: string[] = [];
    const projector = defineProjector<W>({
      name: "collector",
      apply: async ({ stored: e }) => {
        rows.push(e.eventId);
      },
    });

    const consumed = await runProjector(store, projector);
    expect(consumed).toBe(3);
    expect(rows).toEqual(["e1", "e2", "e3"]);
    expect(await store.readCheckpoint("collector")).toBe(3);

    const second = await runProjector(store, projector);
    expect(second).toBe(0);
  });

  it("two concurrent runners of one projector apply each event exactly once", async () => {
    const store = createInMemoryStore();
    await store.transaction((tx) =>
      tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1"), event("e2"), event("e3")],
        metadata: { correlationId: "c1" },
      }),
    );

    const rows: string[] = [];
    const projector = defineProjector<W>({
      name: "collector",
      apply: async ({ stored }) => {
        rows.push(stored.eventId);
        await new Promise((resolve) => setTimeout(resolve, 1));
      },
    });

    const [a, b] = await Promise.all([runProjector(store, projector), runProjector(store, projector)]);
    expect(rows).toEqual(["e1", "e2", "e3"]);
    expect(a + b).toBe(3);
    expect(await store.readCheckpoint("collector")).toBe(3);
  });

  it("leaves the checkpoint at the last successful event when apply throws", async () => {
    const store = createInMemoryStore();
    await store.transaction((tx) =>
      tx.append({
        streamId: "s1",
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event("e1"), event("e2"), event("e3")],
        metadata: { correlationId: "c1" },
      }),
    );

    const rows: string[] = [];
    const projector = defineProjector<W>({
      name: "collector",
      apply: async ({ stored: e }) => {
        if (e.eventId === "e2") throw new Error("boom");
        rows.push(e.eventId);
      },
    });

    await expect(runProjector(store, projector)).rejects.toThrow("boom");
    expect(rows).toEqual(["e1"]);
    expect(await store.readCheckpoint("collector")).toBe(1);
  });

  it("a projector bound to an aggregate receives the upcast, parsed payload", async () => {
    const store = createInMemoryStore();
    await store.transaction((tx) =>
      tx.append({
        streamId: "wallet-legacy",
        aggregateType: "wallet",
        expected: { kind: "no-stream" },
        events: [
          {
            eventId: "evt-1",
            eventType: "wallet.provisioned",
            eventRevision: 1,
            payload: { minutes: 50 },
            occurredAt: new Date("2025-01-01T00:00:00.000Z"),
          },
        ],
        metadata: { correlationId: "c1" },
      }),
    );

    const payloads: Array<{ readonly minutes: number; readonly source: string }> = [];
    const projector = defineProjector<WalletState, typeof walletEvents, W>({
      name: "wallet-provisioned",
      aggregate: wallet,
      apply: async ({ stored }) => {
        if (stored.event.eventType !== "wallet.provisioned") {
          throw new Error(`unexpected event ${stored.event.eventType}`);
        }
        payloads.push(stored.event.payload);
      },
    });

    expect(await runProjector(store, projector)).toBe(1);
    expect(payloads).toEqual([{ minutes: 50, source: "legacy" }]);
  });
});

describe("startProjectors", () => {
  it("throws DuplicateProjectorError for two projectors sharing a name", () => {
    const store = createInMemoryStore();
    const a = defineProjector<W>({ name: "dup", apply: async () => {} });
    const b = defineProjector<W>({ name: "dup", apply: async () => {} });

    expect(() => startProjectors(store, [a, b])).toThrow(DuplicateProjectorError);
  });

  it("wake() runs a pass immediately and status() reports lag", async () => {
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

    const rows: string[] = [];
    const { promise: applied, resolve: onApplied } = Promise.withResolvers<void>();
    const projector = defineProjector<W>({
      name: "collector",
      apply: async ({ stored: e }) => {
        rows.push(e.eventId);
        onApplied();
      },
    });

    // intervalMs far beyond the test's lifetime: only wake() drives a pass here.
    const handle = startProjectors(store, [projector], { intervalMs: 1_000_000 });
    handle.wake();
    await applied;
    // `apply` resolves before its transaction's checkpoint write commits; stop() awaits the
    // in-flight pass to completion, which is the deterministic signal for the checkpoint.
    await handle.stop();

    expect(rows).toEqual(["e1"]);
    const status = await handle.status();
    expect(status).toEqual([{ name: "collector", position: 1, lag: 0 }]);
  });

  it("a natural interval tick runs a pass and calls a custom onError for a failing projector", async () => {
    vi.useFakeTimers();
    try {
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

      const errors: Array<{ error: unknown; name: string }> = [];
      const failing = defineProjector<W>({
        name: "failing",
        apply: async () => {
          throw new Error("boom");
        },
      });

      const handle = startProjectors(store, [failing], {
        intervalMs: 100,
        onError: (error, name) => errors.push({ error, name }),
      });

      // The initial `tick()` call inside `startProjectors` runs synchronously on the
      // microtask queue; flush it before advancing the timer for the next scheduled tick.
      await vi.advanceTimersByTimeAsync(0);
      expect(errors).toHaveLength(1);
      expect(errors[0]?.error).toBeInstanceOf(Error);
      expect(errors[0]?.name).toBe("failing");

      errors.length = 0;
      await vi.advanceTimersByTimeAsync(100);
      expect(errors).toHaveLength(1);
      expect(errors[0]?.error).toBeInstanceOf(Error);
      expect(errors[0]?.name).toBe("failing");

      await handle.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it("wake() during an in-flight pass runs one more pass after it, never alongside it", async () => {
    const store = createInMemoryStore();
    const { promise: firstApplyStarted, resolve: markStarted } = Promise.withResolvers<void>();
    const { promise: releaseFirstApply, resolve: releaseFirst } = Promise.withResolvers<void>();
    const applied: string[] = [];
    let inFlight = 0;
    let maxInFlight = 0;

    const append = (eventId: string) =>
      store.append({
        streamId: eventId,
        aggregateType: "widget",
        expected: { kind: "no-stream" },
        events: [event(eventId)],
        metadata: { correlationId: eventId },
      });

    const slow = defineProjector<W>({
      name: "slow",
      apply: async ({ stored }) => {
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        if (stored.eventId === "e1") {
          markStarted();
          await releaseFirstApply;
        }
        applied.push(stored.eventId);
        inFlight--;
      },
    });

    await append("e1");
    // intervalMs far beyond the test: only the initial pass and wake() can run a pass.
    const handle = startProjectors(store, [slow], { intervalMs: 1_000_000 });
    await firstApplyStarted;

    // e2 lands after the first pass read its batch, so only a pass woken here can apply it.
    await append("e2");
    handle.wake();
    handle.wake();
    releaseFirst();

    await vi.waitFor(() => expect(applied).toEqual(["e1", "e2"]));
    await handle.stop();
    expect(maxInFlight).toBe(1);
  });
});
