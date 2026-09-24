import { describe, expect, it } from "vitest";
import { decide } from "../../../src/decide.js";
import { fold } from "../../../src/aggregate.js";
import { createInMemoryStore } from "../../../src/memory.js";
import { wallet } from "./aggregate.js";

describe("wallet aggregate (upcasting fixture)", () => {
  it("decide with a synthetic clock creates, provisions, and books minutes", async () => {
    const clockValues = [
      new Date("2026-01-01T00:00:00.000Z"),
      new Date("2026-01-02T00:00:00.000Z"),
      new Date("2026-01-03T00:00:00.000Z"),
    ];
    let clockIndex = 0;
    const clock = () => clockValues[clockIndex++]!;

    const store = createInMemoryStore();
    const streamId = "wallet-1";

    await decide(store, wallet, streamId, () => ({ emit: [{ eventType: "wallet.created" as const, payload: { ownerId: "user-1" } }], result: null }), { clock });
    await decide(store, wallet, streamId, () => ({ emit: [{ eventType: "wallet.provisioned" as const, payload: { minutes: 100, source: "purchase" } }], result: null }), { clock });
    await decide(store, wallet, streamId, () => ({ emit: [{ eventType: "wallet.booked" as const, payload: { minutes: 30 } }], result: null }), { clock });

    const rows = await store.readStream(streamId);
    expect(rows.map((e) => e.event.occurredAt)).toEqual(clockValues);

    const events = rows.map((row) =>
      wallet.events.parseStored({
        eventType: row.event.eventType,
        eventRevision: row.eventRevision,
        payload: row.event.payload,
        occurredAt: row.event.occurredAt,
      }),
    );
    const finalState = fold(wallet, events);
    expect(finalState).toEqual({ ownerId: "user-1", balanceMinutes: 70, provisionedMinutes: 100 });
  });

  it("appends a revision-1 wallet.provisioned row directly and upcasts it on read", async () => {
    const store = createInMemoryStore();
    const streamId = "wallet-legacy";

    // Bypasses `decide`/`parseInsertable` deliberately: the store accepts any `EventToAppend`,
    // which is what lets a revision-1 row exist in the log for `parseStored` to upcast.
    await store.transaction((tx) =>
      tx.append({
        streamId,
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

    const rows = await store.readStream(streamId);
    expect(rows).toHaveLength(1);

    const parsed = wallet.events.parseStored({
      eventType: rows[0]!.event.eventType,
      eventRevision: rows[0]!.eventRevision,
      payload: rows[0]!.event.payload,
      occurredAt: rows[0]!.event.occurredAt,
    });

    expect(rows[0]!.eventRevision).toBe(1);
    expect(parsed.payload).toEqual({ minutes: 50, source: "legacy" });
  });
});
