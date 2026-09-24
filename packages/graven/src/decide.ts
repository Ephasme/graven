import type { Aggregate } from "./aggregate.js";
import {
  ConcurrentModificationError,
  ProjectionAggregateMismatchError,
  StaleStreamVersionError,
  StreamVersionConflictError,
} from "./errors.js";
import type { EventFamilies, EventOf, InsertableEventOf, ParsedInsertable } from "./events.js";
import type { EventStore } from "./store.js";
import type { EventMetadata, EventToAppend, StoredEvent, TimedEvent } from "./types.js";

export type Proposal<C extends EventFamilies, R> = {
  readonly emit: readonly InsertableEventOf<C>[];
  readonly result: R;
};

export type DecideResult<R> =
  | {
      readonly appended: true;
      readonly result: R;
      readonly version: number;
      readonly events: readonly StoredEvent[];
    }
  | { readonly appended: false; readonly reason: "refused" | "no-op" };

export type DecideOptions = {
  readonly metadata?: Partial<EventMetadata>;
  readonly occurredAt?: Date;
  readonly newId?: () => string; // default crypto.randomUUID
  readonly clock?: () => Date; // default () => new Date()
  readonly maxAttempts?: number; // default 3
  readonly initialDelayMs?: number; // default 25, doubled per attempt
  readonly projections?: boolean; // default true: run the store's registered inline projections
};

function sleep(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  return promise;
}

function isRetryable(error: unknown): boolean {
  return error instanceof StaleStreamVersionError || error instanceof StreamVersionConflictError;
}

async function attemptDecide<S, C extends EventFamilies, R, W>(
  store: EventStore<W>,
  aggregate: Aggregate<S, C>,
  streamId: string,
  decision: (state: S, events: readonly TimedEvent<EventOf<C>>[]) => Proposal<C, R> | null,
  newId: () => string,
  clock: () => Date,
  occurredAtOverride: Date | undefined,
  metadata: Partial<EventMetadata> | undefined,
  runProjections: boolean,
): Promise<DecideResult<R>> {
  return store.transaction(async (tx) => {
    const matchingProjections = runProjections
      ? tx.projections.filter((p) => p.aggregate.type === aggregate.type)
      : [];

    for (const projection of matchingProjections) {
      if (projection.aggregate !== aggregate) {
        throw new ProjectionAggregateMismatchError(projection.name, aggregate.type);
      }
    }

    for (const projection of matchingProjections) {
      await tx.lockProjectionShared(projection.name);
    }

    await tx.lockStream(streamId);

    const existing = await tx.readStream(streamId);
    const events = existing.map((row) =>
      aggregate.events.parseStored({
        eventType: row.event.eventType,
        eventRevision: row.eventRevision,
        payload: row.event.payload,
        occurredAt: row.event.occurredAt,
      }),
    );

    const initialState = events.reduce(aggregate.evolve, aggregate.initialState);

    const proposal = decision(initialState, events);
    if (proposal === null) {
      return { appended: false, reason: "refused" };
    }

    const parsed: ParsedInsertable<C>[] = proposal.emit.map((entry) => aggregate.events.parseInsertable(entry));
    const occurredAt = occurredAtOverride ?? clock();

    // Walk cumulatively, dropping any event whose evolve is a no-op when the aggregate opts in.
    let state = initialState;
    const survivors: ParsedInsertable<C>[] = [];
    const appended: TimedEvent<EventOf<C>>[] = [];
    for (const entry of parsed) {
      const timed: TimedEvent<EventOf<C>> = { ...entry.event, occurredAt };
      const nextState = aggregate.evolve(state, timed);
      if (aggregate.elideNoOps && nextState === state) {
        continue;
      }
      state = nextState;
      survivors.push(entry);
      appended.push(timed);
    }

    if (aggregate.elideNoOps && survivors.length === 0) {
      return { appended: false, reason: "no-op" };
    }

    const lastExisting = existing.at(-1);
    const expected = lastExisting === undefined
      ? ({ kind: "no-stream" } as const)
      : ({ kind: "exact" as const, version: lastExisting.streamVersion });

    const toAppend: EventToAppend[] = survivors.map((entry) => ({
      eventId: newId(),
      eventType: entry.event.eventType,
      eventRevision: entry.eventRevision,
      payload: entry.event.payload,
      occurredAt,
    }));

    const result = await tx.append({
      streamId,
      aggregateType: aggregate.type,
      expected,
      events: toAppend,
      metadata: { correlationId: newId(), ...metadata },
    });

    const sealed = aggregate.seal({ state, appended });
    for (const projection of matchingProjections) {
      try {
        await tx.savepoint(() =>
          projection.project({ streamId, version: result.newVersion, sealed, writer: tx.writer }),
        );
      } catch (error) {
        tx.onProjectionError(error, { projection: projection.name, streamId });
      }
    }

    return {
      appended: true,
      result: proposal.result,
      version: result.newVersion,
      events: result.stored,
    };
  });
}

/** Keys an emitted entry may carry; anything else, such as `eventRevision`, is the store's to set. */
type ExactEmit<P, C extends EventFamilies> = P extends { readonly emit: readonly (infer E)[] }
  ? readonly { readonly [K in keyof E]: K extends keyof InsertableEventOf<C> ? E[K] : never }[]
  : never;

export async function decide<S, C extends EventFamilies, R, W, const P extends Proposal<C, R> = Proposal<C, R>>(
  store: EventStore<W>,
  aggregate: Aggregate<S, C>,
  streamId: string,
  decision: (state: S, events: readonly TimedEvent<EventOf<C>>[]) => (P & { readonly emit: ExactEmit<P, C> }) | null,
  options?: DecideOptions,
): Promise<DecideResult<R>> {
  const newId = options?.newId ?? (() => crypto.randomUUID());
  const clock = options?.clock ?? (() => new Date());
  const maxAttempts = options?.maxAttempts ?? 3;
  const initialDelayMs = options?.initialDelayMs ?? 25;
  const runProjections = options?.projections ?? true;

  let attempt = 0;
  for (;;) {
    attempt++;
    try {
      return await attemptDecide(
        store,
        aggregate,
        streamId,
        decision,
        newId,
        clock,
        options?.occurredAt,
        options?.metadata,
        runProjections,
      );
    } catch (error) {
      if (!isRetryable(error) || attempt >= maxAttempts) {
        if (isRetryable(error)) {
          throw new ConcurrentModificationError(streamId, attempt);
        }
        throw error;
      }
      await sleep(initialDelayMs * 2 ** (attempt - 1));
    }
  }
}
