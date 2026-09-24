import type { Aggregate, Fold, SealedFold } from "./aggregate.js";
import { ProjectionAggregateMismatchError, ProjectionNotRebuildableError } from "./errors.js";
import type { EventFamilies } from "./events.js";
import type { EventStore } from "./store.js";
import type { StoredEvent } from "./types.js";

export type ProjectionContext<S, C extends EventFamilies, W> = Fold<S, C> & {
  readonly streamId: string;
  readonly version: number; // the stream's version after the append
  readonly writer: W;
};

/** A projection as a store holds it: aggregate types erased, only the writer kept. */
export interface Projection<W> {
  readonly name: string;
  readonly aggregate: { readonly type: string };
  readonly wipe: ((writer: W) => Promise<void>) | undefined;
  /** decide: apply the fold decide sealed. */
  project(input: {
    readonly streamId: string;
    readonly version: number;
    readonly sealed: SealedFold;
    readonly writer: W;
  }): Promise<void>;
  /** rebuild: fold `rows` with the projection's own aggregate and apply; false when `rows` is empty. */
  replay(input: {
    readonly streamId: string;
    readonly rows: readonly StoredEvent[];
    readonly writer: W;
  }): Promise<boolean>;
}

export function defineProjection<S, C extends EventFamilies, W>(config: {
  readonly name: string;
  readonly aggregate: Aggregate<S, C>;
  readonly apply: (context: ProjectionContext<S, C, W>) => Promise<void>;
  readonly wipe?: (writer: W) => Promise<void>;
}): Projection<W> {
  const { aggregate, apply } = config;

  return {
    name: config.name,
    aggregate,
    wipe: config.wipe,

    async project({ streamId, version, sealed, writer }) {
      const fold = aggregate.unseal(sealed);
      if (!fold) {
        throw new ProjectionAggregateMismatchError(config.name, aggregate.type);
      }
      await apply({ ...fold, streamId, version, writer });
    },

    async replay({ streamId, rows, writer }) {
      const last = rows.at(-1);
      if (!last) return false;

      const events = rows.map((row) =>
        aggregate.events.parseStored({
          eventType: row.event.eventType,
          eventRevision: row.eventRevision,
          payload: row.event.payload,
          occurredAt: row.event.occurredAt,
        }),
      );
      const state = events.reduce(aggregate.evolve, aggregate.initialState);
      await apply({ streamId, version: last.streamVersion, writer, state, appended: events });
      return true;
    },
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

    const wipes: { readonly projection: Projection<W>; readonly wipe: (writer: W) => Promise<void> }[] = [];
    for (const projection of selected) {
      if (!projection.wipe) {
        throw new ProjectionNotRebuildableError(projection.name);
      }
      wipes.push({ projection, wipe: projection.wipe });
    }

    for (const projection of selected) {
      await tx.lockProjectionExclusive(projection.name);
    }

    for (const { wipe } of wipes) {
      await wipe(tx.writer);
    }

    let streams = 0;
    for (const projection of selected) {
      const streamIds = await tx.readStreamIds(projection.aggregate.type);
      for (const streamId of streamIds) {
        const rows = await tx.readStream(streamId);
        if (await projection.replay({ streamId, rows, writer: tx.writer })) {
          streams++;
        }
      }
    }

    return { projections: selected.map((p) => p.name), streams };
  });
}
