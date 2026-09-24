import type { Projection } from "./projection.js";
import type { AppendRequest, AppendResult, StoredEvent } from "./types.js";

export type ReadStreamOptions = {
  readonly fromVersion?: number; // inclusive
  readonly toVersion?: number; // inclusive
};

export type GlobalReadOptions = {
  readonly fromPosition: number; // exclusive: rows with global_position > fromPosition
  readonly limit: number;
  readonly aggregateType?: string;
};

/**
 * Contract: `readGlobal` returns a gap-free prefix in commit order. Every position
 * `<= currentPosition()` at the time of the call is either returned (within `limit`)
 * or already below `fromPosition`. This is what lets a catch-up projector advance its
 * checkpoint to the last position it read with no gap detection.
 */
export interface EventStore<W = void> {
  /** Transaction-scoped writer handed to projections and projectors. */
  readonly writer: W;

  /**
   * Serialise every other lockStream/append on this stream until this transaction ends.
   * MUST be called inside `transaction`; calling it outside throws a plain Error.
   */
  lockStream(streamId: string): Promise<void>;

  /** Shared maintenance lock for a projection, so a rebuild (exclusive) cannot overlap it. */
  lockProjectionShared(projectionName: string): Promise<void>;

  /** Exclusive counterpart, taken by `rebuild`; blocks until every inline holder commits. */
  lockProjectionExclusive(projectionName: string): Promise<void>;

  append(request: AppendRequest): Promise<AppendResult>;

  readStream(streamId: string, options?: ReadStreamOptions): Promise<readonly StoredEvent[]>;

  readStreams(streamIds: readonly string[]): Promise<ReadonlyMap<string, readonly StoredEvent[]>>;

  readGlobal(options: GlobalReadOptions): Promise<readonly StoredEvent[]>;

  readStreamIds(aggregateType: string): Promise<readonly string[]>;

  currentPosition(): Promise<number>;

  readCheckpoint(projectorName: string): Promise<number>; // 0 when the row is absent

  writeCheckpoint(projectorName: string, position: number): Promise<void>;

  /**
   * Run `fn` inside a transaction. Called on a store that is already transaction-scoped,
   * it runs `fn(this)` in the same transaction (no nesting, no savepoint).
   */
  transaction<T>(fn: (tx: EventStore<W>) => Promise<T>): Promise<T>;

  /**
   * Run `fn` inside a savepoint of the current transaction; a throw rolls the savepoint
   * back and rethrows, a return releases it. Outside a transaction it throws a plain Error.
   */
  savepoint<T>(fn: () => Promise<T>): Promise<T>;

  /** Registered inline projections, in registration order. */
  readonly projections: readonly Projection<any, any, W>[];

  readonly onProjectionError: (
    error: unknown,
    context: { readonly projection: string; readonly streamId: string },
  ) => void;
}
