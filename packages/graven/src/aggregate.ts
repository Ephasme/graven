import { defineEvents } from "./events.js";
import type { EventFamilies, EventOf, EventRegistry } from "./events.js";
import type { TimedEvent } from "./types.js";

export type Fold<S, C extends EventFamilies> = {
  readonly state: S;
  readonly appended: readonly TimedEvent<EventOf<C>>[];
};

/** Opaque handle on one decision's fold; only the aggregate that sealed it can open it. */
export type SealedFold = { readonly aggregateType: string };

export interface Aggregate<S, C extends EventFamilies> {
  readonly type: string;
  readonly events: EventRegistry<C>;
  readonly initialState: S;
  readonly evolve: (state: S, event: TimedEvent<EventOf<C>>) => S;
  /** Default false. When true, an event whose evolve returns the same reference is dropped. */
  readonly elideNoOps: boolean;
  /** decide → projection handoff: seal a fold so a projection defined over this exact aggregate can read it typed. */
  seal(fold: Fold<S, C>): SealedFold;
  unseal(sealed: SealedFold): Fold<S, C> | undefined;
}

export function defineAggregate<S, const C extends EventFamilies>(config: {
  readonly type: string;
  readonly events: C;
  readonly initialState: NoInfer<S>;
  readonly evolve: (state: S, event: TimedEvent<EventOf<C>>) => S;
  readonly elideNoOps?: boolean;
}): Aggregate<S, C> {
  const folds = new WeakMap<SealedFold, Fold<S, C>>();

  return {
    type: config.type,
    events: defineEvents(config.events),
    initialState: config.initialState,
    evolve: config.evolve,
    elideNoOps: config.elideNoOps ?? false,

    seal(fold) {
      const token: SealedFold = Object.freeze({ aggregateType: config.type });
      folds.set(token, fold);
      return token;
    },

    unseal(sealed) {
      return folds.get(sealed);
    },
  };
}

export function fold<S, C extends EventFamilies>(
  aggregate: Aggregate<S, C>,
  events: readonly TimedEvent<EventOf<C>>[],
): S {
  return events.reduce(aggregate.evolve, aggregate.initialState);
}
