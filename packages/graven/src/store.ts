import type { SealedStateWithEvents } from "./aggregate.js";
import type { AppendRequest, AppendResult, StoredEvent } from "./types.js";

export type GlobalReadOptions = {
  readonly fromPosition: number; // exclusive: rows with global_position > fromPosition
  readonly limit: number;
  readonly aggregateType?: string | undefined;
};

/**
 * Contract: `readGlobal` returns a gap-free prefix in commit order. Every position
 * `<= currentPosition()` at the time of the call is either returned (within `limit`)
 * or already below `fromPosition`. This is what lets a catch-up projector advance its
 * checkpoint to the last position it read with no gap detection.
 */

/** A projection as a store holds it: aggregate types erased, only the writer kept. */
export interface Projection<W> {
  readonly name: string;
  readonly aggregate: { readonly type: string };
  readonly wipe: ((writer: W) => Promise<void>) | undefined;
  /** decide: apply the fold decide sealed. */
  project(input: {
    readonly streamId: string;
    readonly version: number;
    readonly sealed: SealedStateWithEvents;
    readonly writer: W;
  }): Promise<void>;
  /** rebuild: fold `rows` with the projection's own aggregate and apply; false when `rows` is empty. */
  replay(input: {
    readonly streamId: string;
    readonly rows: readonly StoredEvent[];
    readonly writer: W;
  }): Promise<boolean>;
}

export type ProjectionErrorHandler = (
  error: unknown,
  context: { readonly projection: string; readonly streamId: string },
) => void;

export interface EventStore<W = void> {
  readonly writer: W;
  /** Outside a transaction, runs in one of its own. */
  append(request: AppendRequest): Promise<AppendResult>;
  readStream(streamId: string): Promise<readonly StoredEvent[]>;
  readGlobal(options: GlobalReadOptions): Promise<readonly StoredEvent[]>;
  readStreamIds(aggregateType: string): Promise<readonly string[]>;
  currentPosition(): Promise<number>;
  readCheckpoint(projectorName: string): Promise<number>; // 0 when the row is absent
  /** Outside a transaction, runs in one of its own. */
  writeCheckpoint(projectorName: string, position: number): Promise<void>;
  /** On a transaction, runs `fn(this)` in that same transaction (no nesting, no savepoint). */
  transaction<T>(fn: (tx: EventStoreTransaction<W>) => Promise<T>): Promise<T>;
  /** Registered inline projections, in registration order. */
  readonly projections: readonly Projection<W>[];
  readonly onProjectionError: ProjectionErrorHandler;
}

/** The handle `transaction` passes to its callback: the operations only a transaction can hold. */
export interface EventStoreTransaction<W = void> extends EventStore<W> {
  /** Serialise every other lockStream/append on this stream until this transaction ends. */
  lockStream(streamId: string): Promise<void>;
  /** Shared maintenance lock for a projection, so a rebuild (exclusive) cannot overlap it. */
  lockProjectionShared(projectionName: string): Promise<void>;
  /** Exclusive counterpart, taken by `rebuild`; upgrades a shared lock this transaction holds. */
  lockProjectionExclusive(projectionName: string): Promise<void>;
  /** Row-lock a projector's checkpoint until this transaction ends; returns its position (0 when absent). */
  lockCheckpoint(projectorName: string): Promise<number>;
  /** A throw rolls the savepoint back and rethrows; a return releases it. */
  savepoint<T>(fn: () => Promise<T>): Promise<T>;
}
