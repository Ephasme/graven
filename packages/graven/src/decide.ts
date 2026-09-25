import { fold } from "./aggregate.js";
import type { Aggregate, SealedStateWithEvents } from "./aggregate.js";
import {
  ConcurrentModificationError,
  ProjectionAggregateMismatchError,
  StaleStreamVersionError,
  StreamVersionConflictError,
} from "./errors.js";
import type { EventFamilies, EventOf, InsertableEventOf, ParsedInsertable } from "./events.js";
import type { EventStore, EventStoreTransaction, Projection } from "./store.js";
import type { EventMetadata, EventToAppend, ExpectedVersion, StoredEvent, TimedEvent } from "./types.js";

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
  readonly runInlineProjections?: boolean; // default true
};

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_INITIAL_DELAY_MS = 25;

function sleep(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  return promise;
}

function isRetryable(error: unknown): boolean {
  return error instanceof StaleStreamVersionError || error instanceof StreamVersionConflictError;
}

type Attempt = {
  readonly newId: () => string;
  readonly clock: () => Date;
  readonly occurredAt: Date | undefined;
  readonly metadata: Partial<EventMetadata> | undefined;
  readonly runInlineProjections: boolean;
};

/** When disabled, returns immediately without inspecting registered projections. Otherwise
 * returns projections over this aggregate type and throws when one was built over another
 * Aggregate object. */
function inlineProjectionsFor<S, C extends EventFamilies, W>(
  tx: EventStoreTransaction<W>,
  aggregate: Aggregate<S, C>,
  enabled: boolean,
): readonly Projection<W>[] {
  if (!enabled) return [];
  const matching = tx.projections.filter((p) => p.aggregate.type === aggregate.type);
  for (const projection of matching) {
    if (projection.aggregate !== aggregate) {
      throw new ProjectionAggregateMismatchError(projection.name, aggregate.type);
    }
  }
  return matching;
}

/** Folds the proposal onto `state`, dropping no-op events when the aggregate opts in. */
function evolveProposal<S, C extends EventFamilies>(
  aggregate: Aggregate<S, C>,
  state: S,
  parsed: readonly ParsedInsertable<C>[],
  occurredAt: Date,
): { readonly state: S; readonly survivors: readonly ParsedInsertable<C>[]; readonly appended: readonly TimedEvent<EventOf<C>>[] } {
  let nextState = state;
  const survivors: ParsedInsertable<C>[] = [];
  const appended: TimedEvent<EventOf<C>>[] = [];
  for (const entry of parsed) {
    const timed: TimedEvent<EventOf<C>> = { ...entry.event, occurredAt };
    const evolved = aggregate.evolve(nextState, timed);
    if (aggregate.elideNoOps && evolved === nextState) {
      continue;
    }
    nextState = evolved;
    survivors.push(entry);
    appended.push(timed);
  }
  return { state: nextState, survivors, appended };
}

function expectedVersionAfter(existing: readonly StoredEvent[]): ExpectedVersion {
  const last = existing.at(-1);
  return last === undefined ? { kind: "no-stream" } : { kind: "exact", version: last.streamVersion };
}

async function applyInlineProjections<W>(
  tx: EventStoreTransaction<W>,
  projections: readonly Projection<W>[],
  streamId: string,
  version: number,
  sealed: SealedStateWithEvents,
): Promise<void> {
  for (const projection of projections) {
    try {
      await tx.savepoint(() => projection.project({ streamId, version, sealed, writer: tx.writer }));
    } catch (error) {
      tx.onProjectionError(error, { projection: projection.name, streamId });
    }
  }
}

async function attemptDecide<S, C extends EventFamilies, R, W>(
  store: EventStore<W>,
  aggregate: Aggregate<S, C>,
  streamId: string,
  decision: (state: S, events: readonly TimedEvent<EventOf<C>>[]) => Proposal<C, R> | null,
  attempt: Attempt,
): Promise<DecideResult<R>> {
  return store.transaction(async (tx) => {
    const matchingProjections = inlineProjectionsFor(tx, aggregate, attempt.runInlineProjections);
    for (const projection of matchingProjections) {
      await tx.lockProjectionShared(projection.name);
    }
    await tx.lockStream(streamId);

    const existing = await tx.readStream(streamId);
    const events = existing.map((row) => aggregate.events.parseStored(row));
    const currentState = fold(aggregate, events);

    const proposal = decision(currentState, events);
    if (proposal === null) {
      return { appended: false, reason: "refused" };
    }

    const parsed: ParsedInsertable<C>[] = proposal.emit.map((entry) => aggregate.events.parseInsertable(entry));
    const occurredAt = attempt.occurredAt ?? attempt.clock();

    const evolved = evolveProposal(aggregate, currentState, parsed, occurredAt);
    if (aggregate.elideNoOps && evolved.survivors.length === 0) {
      return { appended: false, reason: "no-op" };
    }

    const toAppend: EventToAppend[] = evolved.survivors.map((entry) => ({
      eventId: attempt.newId(),
      eventType: entry.event.eventType,
      eventRevision: entry.eventRevision,
      payload: entry.event.payload,
      occurredAt,
    }));

    const result = await tx.append({
      streamId,
      aggregateType: aggregate.type,
      expected: expectedVersionAfter(existing),
      events: toAppend,
      metadata: { correlationId: attempt.newId(), ...attempt.metadata },
    });

    const sealed = aggregate.seal({ state: evolved.state, appended: evolved.appended });
    await applyInlineProjections(tx, matchingProjections, streamId, result.newVersion, sealed);

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
  const maxAttempts = options?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  const initialDelayMs = options?.initialDelayMs ?? DEFAULT_INITIAL_DELAY_MS;
  const attempt: Attempt = {
    newId: options?.newId ?? (() => crypto.randomUUID()),
    clock: options?.clock ?? (() => new Date()),
    occurredAt: options?.occurredAt,
    metadata: options?.metadata,
    runInlineProjections: options?.runInlineProjections ?? true,
  };

  let attemptNumber = 0;
  for (;;) {
    attemptNumber++;
    try {
      return await attemptDecide(store, aggregate, streamId, decision, attempt);
    } catch (error) {
      if (!isRetryable(error)) throw error;
      if (attemptNumber >= maxAttempts) throw new ConcurrentModificationError(streamId, attemptNumber);
      await sleep(initialDelayMs * 2 ** (attemptNumber - 1));
    }
  }
}
