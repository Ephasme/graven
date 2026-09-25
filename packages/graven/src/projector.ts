import type { Aggregate } from "./aggregate.js";
import { DuplicateProjectorError } from "./errors.js";
import type { EventFamilies, EventOf } from "./events.js";
import type { EventStore } from "./store.js";
import type { BaseEvent, StoredEvent } from "./types.js";

export type ProjectorContext<E extends BaseEvent, W> = {
  readonly stored: StoredEvent<E>;
  readonly writer: W;
};

export interface Projector<W> {
  readonly name: string;
  readonly aggregateType: string | undefined;
  apply(context: ProjectorContext<BaseEvent, W>): Promise<void>;
}

export function defineProjector<S, C extends EventFamilies, W>(config: {
  readonly name: string;
  readonly aggregate: Aggregate<S, C>;
  readonly apply: (context: ProjectorContext<EventOf<C>, W>) => Promise<void>;
}): Projector<W>;
export function defineProjector<W>(config: {
  readonly name: string;
  readonly apply: (context: ProjectorContext<BaseEvent, W>) => Promise<void>;
}): Projector<W>;
export function defineProjector<S, C extends EventFamilies, W>(
  config:
    | {
        readonly name: string;
        readonly aggregate: Aggregate<S, C>;
        readonly apply: (context: ProjectorContext<EventOf<C>, W>) => Promise<void>;
      }
    | {
        readonly name: string;
        readonly apply: (context: ProjectorContext<BaseEvent, W>) => Promise<void>;
      },
): Projector<W> {
  if ("aggregate" in config) {
    const { name, aggregate, apply } = config;
    return {
      name,
      aggregateType: aggregate.type,
      // The declared payload is the latest, upcast shape; decode the row before it reaches `apply`.
      apply: async ({ stored, writer }) => {
        const event = aggregate.events.parseStored(stored);
        await apply({ writer, stored: { ...stored, event } });
      },
    };
  }
  return { name: config.name, aggregateType: undefined, apply: config.apply };
}

const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_INTERVAL_MS = 1000;

/** One pass. Returns how many events it consumed. */
export async function runProjector<W>(
  store: EventStore<W>,
  projector: Projector<W>,
  options?: { readonly batchSize?: number },
): Promise<number> {
  const batchSize = options?.batchSize ?? DEFAULT_BATCH_SIZE;
  let consumed = 0;

  for (;;) {
    let checkpoint = await store.readCheckpoint(projector.name);
    const batch = await store.readGlobal({ fromPosition: checkpoint, limit: batchSize, aggregateType: projector.aggregateType });
    let raced = false;
    for (const stored of batch) {
      const applied = await store.transaction(async (tx) => {
        // Another runner of this projector moved the checkpoint since this batch was read.
        if ((await tx.lockCheckpoint(projector.name)) !== checkpoint) return false;
        await projector.apply({ stored, writer: tx.writer });
        await tx.writeCheckpoint(projector.name, stored.globalPosition);
        return true;
      });
      if (!applied) {
        raced = true;
        break;
      }
      checkpoint = stored.globalPosition;
      consumed++;
    }
    if (!raced && batch.length < batchSize) break;
  }

  return consumed;
}

function createScheduler(runPass: () => Promise<void>, intervalMs: number): { readonly wake: () => void; readonly stop: () => Promise<void> } {
  let stopped = false;
  let wakeRequested = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let currentPass: Promise<void> | null = null;
  let stopPromise: Promise<void> | null = null;

  function scheduleNext(): void {
    if (stopped) return;
    timer = setTimeout(tick, intervalMs);
  }

  function tick(): void {
    if (stopped) return;
    currentPass = runPass().finally(() => {
      currentPass = null;
      if (wakeRequested && !stopped) {
        wakeRequested = false;
        tick();
      } else {
        scheduleNext();
      }
    });
  }

  tick();

  return {
    wake() {
      if (stopped) return;
      if (currentPass) {
        wakeRequested = true;
      } else {
        clearTimeout(timer);
        tick();
      }
    },

    stop() {
      if (stopPromise) return stopPromise;
      stopped = true;
      clearTimeout(timer);
      stopPromise = (currentPass ?? Promise.resolve()).then(() => undefined);
      return stopPromise;
    },
  };
}

export function startProjectors<W>(
  store: EventStore<W>,
  projectors: readonly Projector<W>[],
  options?: {
    readonly intervalMs?: number;
    readonly batchSize?: number;
    readonly onError?: (error: unknown, projector: string) => void;
  },
): {
  readonly wake: () => void;
  readonly stop: () => Promise<void>;
  readonly status: () => Promise<readonly { readonly name: string; readonly position: number; readonly lag: number }[]>;
} {
  const seen = new Set<string>();
  for (const projector of projectors) {
    if (seen.has(projector.name)) {
      throw new DuplicateProjectorError(projector.name);
    }
    seen.add(projector.name);
  }

  const batchSize = options?.batchSize ?? DEFAULT_BATCH_SIZE;
  const onError = options?.onError ?? ((error, name) => console.error(`graven: projector ${name} failed`, error));

  async function runPass(): Promise<void> {
    for (const projector of projectors) {
      try {
        await runProjector(store, projector, { batchSize });
      } catch (error) {
        onError(error, projector.name);
      }
    }
  }

  const scheduler = createScheduler(runPass, options?.intervalMs ?? DEFAULT_INTERVAL_MS);

  return {
    wake: scheduler.wake,
    stop: scheduler.stop,

    async status() {
      const rows = [];
      for (const projector of projectors) {
        rows.push({ name: projector.name, checkpoint: await store.readCheckpoint(projector.name) });
      }
      // Position is read last: it only grows, so lag cannot go negative.
      const position = await store.currentPosition();
      return rows.map(({ name, checkpoint }) => ({ name, position: checkpoint, lag: position - checkpoint }));
    },
  };
}
