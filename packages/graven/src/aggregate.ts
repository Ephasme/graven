import { defineEvents } from "./events.js";
import type { AnyEventFamily, EventOf, EventRegistry } from "./events.js";
import type { TimedEvent } from "./types.js";

export interface Aggregate<S, C extends readonly AnyEventFamily[]> {
  readonly type: string;
  readonly events: EventRegistry<C>;
  readonly initialState: S;
  readonly evolve: (state: S, event: TimedEvent<EventOf<C>>) => S;
  /** Default false. When true, an event whose evolve returns the same reference is dropped. */
  readonly elideNoOps: boolean;
}

export function defineAggregate<S, const C extends readonly AnyEventFamily[]>(config: {
  readonly type: string;
  readonly events: C;
  readonly initialState: S;
  readonly evolve: (state: S, event: TimedEvent<EventOf<C>>) => S;
  readonly elideNoOps?: boolean;
}): Aggregate<S, C> {
  return {
    type: config.type,
    events: defineEvents(config.events),
    initialState: config.initialState,
    evolve: config.evolve,
    elideNoOps: config.elideNoOps ?? false,
  };
}

export function fold<S, C extends readonly AnyEventFamily[]>(
  aggregate: Aggregate<S, C>,
  events: readonly TimedEvent<EventOf<C>>[],
): S {
  return events.reduce(aggregate.evolve, aggregate.initialState);
}
