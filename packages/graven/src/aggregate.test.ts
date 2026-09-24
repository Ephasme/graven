import { describe, expect, it } from "vitest";
import { defineAggregate, fold } from "./aggregate.js";
import { defineEvent } from "./events.js";
import { z } from "zod";

const created = defineEvent({
  eventType: "counter.created",
  latestRevision: 1,
  latest: z.object({}),
});
const incremented = defineEvent({
  eventType: "counter.incremented",
  latestRevision: 1,
  latest: z.object({ by: z.number() }),
});

type State = { count: number } | null;

const counter = defineAggregate({
  type: "counter",
  events: [created, incremented],
  initialState: null as State,
  evolve: (state, event) => {
    switch (event.eventType) {
      case "counter.created":
        return { count: 0 };
      case "counter.incremented":
        return { count: (state?.count ?? 0) + event.payload.by };
    }
  },
});

describe("fold", () => {
  it("reduces events over the initial state in order", () => {
    const events = [
      { eventType: "counter.created" as const, payload: {} as Record<string, never>, occurredAt: new Date() },
      { eventType: "counter.incremented" as const, payload: { by: 3 }, occurredAt: new Date() },
      { eventType: "counter.incremented" as const, payload: { by: 4 }, occurredAt: new Date() },
    ];

    const state = fold(counter, events);
    expect(state).toEqual({ count: 7 });
  });

  it("returns the initial state for an empty stream", () => {
    expect(fold(counter, [])).toBeNull();
  });
});
