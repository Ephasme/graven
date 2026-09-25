// Compile-time regression test, checked by `bun run typecheck` and never executed. The augmentation
// mirrors `@asteasolutions/zod-to-openapi` (loaded by `@hono/zod-openapi`), whose `.openapi()` method
// takes `Input` and returns `this`. Bounding user schemas on the augmented classic `z.ZodType` made
// every `defineEvent`/`defineEvents` call fail with TS2589. The augmentation is program-wide, so it
// also covers `typing.ts` and `src/**/*.test.ts`.
import { z } from "zod";
import type { core } from "zod";
import { defineAggregate, defineEvent, defineEvents } from "../src/index.js";

declare module "zod" {
  interface ZodType<
    out Output = unknown,
    out Input = unknown,
    out Internals extends core.$ZodTypeInternals<Output, Input> = core.$ZodTypeInternals<Output, Input>,
  > extends core.$ZodType<Output, Input, Internals> {
    openapi(metadata: Partial<{ example: Input; examples: Input[] }>): this;
  }
}

const opened = defineEvent({
  eventType: "ticket.opened",
  latestRevision: 2,
  latest: z.object({ title: z.string(), priority: z.number() }),
  previous: [{ revision: 1, parse: z.object({ title: z.string() }).transform((p) => ({ ...p, priority: 0 })) }],
});
const closed = defineEvent({
  eventType: "ticket.closed",
  latestRevision: 1,
  latest: z.object({ reason: z.enum(["done", "wontfix"]) }),
});

export const registry = defineEvents([opened, closed]);

type State = { title: string; open: boolean } | null;

export const ticket = defineAggregate({
  type: "ticket",
  events: [opened, closed],
  initialState: null,
  evolve: (state: State, event): State =>
    event.eventType === "ticket.opened"
      ? { title: event.payload.title, open: event.payload.priority >= 0 }
      : state && { title: `${state.title} (${event.payload.reason})`, open: false },
});
