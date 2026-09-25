import { fold } from "./aggregate.js";
import type { Aggregate, StateWithEvents } from "./aggregate.js";
import { ProjectionAggregateMismatchError, ProjectionNotRebuildableError } from "./errors.js";
import type { EventFamilies } from "./events.js";
import type { EventStore, Projection } from "./store.js";

export type ProjectionContext<S, C extends EventFamilies, W> = StateWithEvents<S, C> & {
  readonly streamId: string;
  readonly version: number; // the stream's version after the append
  readonly writer: W;
};

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
      const decided = aggregate.unseal(sealed);
      // `decide` rejects a projection built over another Aggregate object before appending, so this
      // miss is unreachable from it; the branch exists because a WeakMap lookup is `| undefined`.
      if (!decided) {
        throw new ProjectionAggregateMismatchError(config.name, aggregate.type);
      }
      await apply({ ...decided, streamId, version, writer });
    },

    async replay({ streamId, rows, writer }) {
      const last = rows.at(-1);
      if (!last) return false;

      const events = rows.map((row) => aggregate.events.parseStored(row));
      const state = fold(aggregate, events);
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
