// Compile-time contract, checked by `bun run typecheck` and never executed: an unused
// `@ts-expect-error` is itself an error, so each line breaks the build if the misuse it names
// ever starts typechecking.
import { z } from "zod";
import { createInMemoryStore, decide, defineAggregate, defineEvent } from "../src/index.js";

const created = defineEvent({
  eventType: "widget.created" as const,
  latestRevision: 1,
  latest: z.object({ name: z.string() }),
});
const renamed = defineEvent({
  eventType: "widget.renamed" as const,
  latestRevision: 1,
  latest: z.object({ to: z.string() }),
});

const widget = defineAggregate({
  type: "widget",
  events: [created, renamed],
  initialState: null as { name: string } | null,
  evolve: (_state, event) => ({ name: event.eventType === "widget.created" ? event.payload.name : event.payload.to }),
});

const store = createInMemoryStore();

export const misuses = [
  () =>
    decide(store, widget, "w1", () => ({
      // @ts-expect-error the store stamps eventRevision; a decision cannot choose it
      emit: [{ eventType: "widget.created" as const, eventRevision: 2, payload: { name: "a" } }],
      result: null,
    })),
  () =>
    decide(store, widget, "w2", () => ({
      // @ts-expect-error `to` belongs to widget.renamed, not widget.created
      emit: [{ eventType: "widget.created" as const, payload: { to: "a" } }],
      result: null,
    })),
  // @ts-expect-error a decision is synchronous: a retry would repeat an awaited side effect
  () => decide(store, widget, "w3", async () => ({ emit: [], result: null })),
];
