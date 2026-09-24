import type { Aggregate } from "./aggregate.js";
import { DuplicateProjectorError } from "./errors.js";
import type { EventFamilies, EventOf } from "./events.js";
import type { EventStore } from "./store.js";
import type { BaseEvent, StoredEvent } from "./types.js";

export type ProjectorContext<E extends BaseEvent, W> = {
  readonly event: StoredEvent<E>;
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
      apply: async ({ event, writer }) => {
        const decoded = aggregate.events.parseStored({
          eventType: event.event.eventType,
          eventRevision: event.eventRevision,
          payload: event.event.payload,
          occurredAt: event.event.occurredAt,
        });
        await apply({ writer, event: { ...event, event: decoded } });
      },
    };
  }
  return { name: config.name, aggregateType: undefined, apply: config.apply };
}

/** One pass. Returns how many events it consumed. */
export async function runProjector<W>(
  store: EventStore<W>,
  projector: Projector<W>,
  options?: { readonly batchSize?: number },
): Promise<number> {
  const batchSize = options?.batchSize ?? 100;
  let consumed = 0;

  for (;;) {
    const checkpoint = await store.readCheckpoint(projector.name);
    const batch = await store.readGlobal({
      fromPosition: checkpoint,
      limit: batchSize,
      aggregateType: projector.aggregateType,
    });

    for (const event of batch) {
      await store.transaction(async (tx) => {
        await projector.apply({ event, writer: tx.writer });
        await tx.writeCheckpoint(projector.name, event.globalPosition);
      });
      consumed++;
    }

    if (batch.length < batchSize) break;
  }

  return consumed;
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

  const intervalMs = options?.intervalMs ?? 1000;
  const batchSize = options?.batchSize ?? 100;
  const onError = options?.onError ?? ((error, name) => console.error(`graven: projector ${name} failed`, error));

  let stopped = false;
  let wakeRequested = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let currentPass: Promise<void> | null = null;
  let stopPromise: Promise<void> | null = null;

  async function runPass(): Promise<void> {
    for (const projector of projectors) {
      try {
        await runProjector(store, projector, { batchSize });
      } catch (error) {
        onError(error, projector.name);
      }
    }
  }

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

    async status() {
      const position = await store.currentPosition();
      const rows = [];
      for (const projector of projectors) {
        const checkpoint = await store.readCheckpoint(projector.name);
        rows.push({ name: projector.name, position: checkpoint, lag: position - checkpoint });
      }
      return rows;
    },
  };
}
