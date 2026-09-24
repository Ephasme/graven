import type { Aggregate } from "./aggregate.js";
import { ProjectionNotRebuildableError } from "./errors.js";
import type { AnyEventFamily, EventOf } from "./events.js";
import type { EventStore } from "./store.js";
import type { TimedEvent } from "./types.js";

export type ProjectionContext<S, C extends readonly AnyEventFamily[], W> = {
  readonly streamId: string;
  readonly state: S; // folded after the append
  readonly appended: readonly TimedEvent<EventOf<C>>[]; // what this decision added
  readonly version: number; // the stream's version after the append
  readonly writer: W;
};

export interface Projection<S, C extends readonly AnyEventFamily[], W> {
  readonly name: string;
  readonly aggregate: Aggregate<S, C>;
  apply(context: ProjectionContext<S, C, W>): Promise<void>;
  /** Runs inside the rebuild transaction, before any stream is re-projected. */
  wipe?(writer: W): Promise<void>;
}

export function defineProjection<S, C extends readonly AnyEventFamily[], W>(config: {
  readonly name: string;
  readonly aggregate: Aggregate<S, C>;
  readonly apply: (context: ProjectionContext<S, C, W>) => Promise<void>;
  readonly wipe?: (writer: W) => Promise<void>;
}): Projection<S, C, W> {
  return {
    name: config.name,
    aggregate: config.aggregate,
    apply: config.apply,
    wipe: config.wipe,
  };
}

export type RebuildResult = {
  readonly projections: readonly string[];
  readonly streams: number;
};

export async function rebuild<W>(
  store: EventStore<W>,
  options?: { readonly projection?: string },
): Promise<RebuildResult> {
  return store.transaction(async (tx) => {
    const selected = options?.projection
      ? tx.projections.filter((p) => p.name === options.projection)
      : tx.projections;

    for (const projection of selected) {
      if (!projection.wipe) {
        throw new ProjectionNotRebuildableError(projection.name);
      }
    }

    for (const projection of selected) {
      await tx.lockProjectionExclusive(projection.name);
    }

    for (const projection of selected) {
      await projection.wipe!(tx.writer);
    }

    let streams = 0;
    for (const projection of selected) {
      const streamIds = await tx.readStreamIds(projection.aggregate.type);
      for (const streamId of streamIds) {
        const rows = await tx.readStream(streamId);
        const events = rows.map((row) =>
          projection.aggregate.events.parseStored({
            eventType: row.event.eventType,
            eventRevision: row.eventRevision,
            payload: row.event.payload,
            occurredAt: row.event.occurredAt,
          }),
        );
        const state = events.reduce(projection.aggregate.evolve, projection.aggregate.initialState);
        const last = rows.at(-1);
        if (!last) continue;
        await projection.apply({
          streamId,
          state,
          appended: events as never,
          version: last.streamVersion,
          writer: tx.writer,
        });
        streams++;
      }
    }

    return { projections: selected.map((p) => p.name), streams };
  });
}
