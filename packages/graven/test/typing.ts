// Compile-time contract, checked by `bun run typecheck` and never executed: an unused
// `@ts-expect-error` is itself an error, so each line breaks the build if the misuse it names
// ever starts typechecking.
import { z } from "zod";
import { createInMemoryStore, decide, defineAggregate, defineEvent, defineEvents } from "../src/index.js";

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

type State = { name: string } | null;

const widget = defineAggregate({
  type: "widget",
  events: [created, renamed],
  initialState: null,
  evolve: (_state: State, event): State =>
    ({ name: event.eventType === "widget.created" ? event.payload.name : event.payload.to }),
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
  // Proves `emit` no longer needs `as const` on `eventType` to typecheck: `const P` on `decide`
  // infers the literal from the decision's return position instead.
  () => decide(store, widget, "w4", () => ({ emit: [{ eventType: "widget.renamed", payload: { to: "b" } }], result: null })),
  () =>
    defineEvent({
      eventType: "widget.timestamped" as const,
      latestRevision: 1,
      // @ts-expect-error a payload must survive JSON: z.date() is not a JsonValue schema
      latest: z.object({ at: z.date() }),
    }),
  () =>
    defineEvent({
      eventType: "widget.upcast-mismatch" as const,
      latestRevision: 2,
      latest: z.object({ name: z.string() }),
      previous: [
        {
          revision: 1,
          // @ts-expect-error an upcaster's output must equal `latest`'s output
          parse: z.object({ oldName: z.string() }),
        },
      ],
    }),
  () =>
    defineAggregate({
      type: "mismatched",
      events: [created],
      // @ts-expect-error initialState must match evolve's state type
      initialState: 123,
      evolve: (state: State, event): State => (event.eventType === "widget.created" ? { name: event.payload.name } : state),
    }),
  () => {
    const registry = defineEvents([created]);
    // @ts-expect-error parseInsertable rejects an unregistered event type at compile time too
    return registry.parseInsertable({ eventType: "widget.deleted", payload: {} });
  },
  // @ts-expect-error no table declared: createInMemoryStore() infers an empty table map
  () => createInMemoryStore().writer.table("t"),
];
