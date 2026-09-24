import { AsyncReaderWriterLock } from "@esfx/async-readerwriterlock";
import { Mutex } from "async-mutex";
import { DuplicateEventIdError, StaleStreamVersionError } from "./errors.js";
import type { Projection } from "./projection.js";
import type { EventStore, GlobalReadOptions, ReadStreamOptions } from "./store.js";
import type { AppendRequest, AppendResult, StoredEvent } from "./types.js";

export type InMemoryWriter = { readonly tables: Map<string, Map<string, unknown>> };

type Tables = Map<string, Map<string, unknown>>;

function copyTables(tables: Tables): Tables {
  const copy: Tables = new Map();
  for (const [name, table] of tables) {
    copy.set(name, new Map(table));
  }
  return copy;
}

/** Replaces `target`'s contents in place, so a writer handle captured earlier stays live. */
function restoreTables(target: Tables, source: Tables): void {
  target.clear();
  for (const [name, table] of source) {
    target.set(name, new Map(table));
  }
}

const EMPTY_TABLE: ReadonlyMap<string, unknown> = new Map();

/**
 * Applies to `root` only the rows `working` changed relative to `baseline`. Writing the whole
 * working copy back would erase rows another transaction committed after this one began.
 */
function commitTables(root: Tables, baseline: Tables, working: Tables): void {
  for (const name of new Set([...baseline.keys(), ...working.keys()])) {
    const before = baseline.get(name) ?? EMPTY_TABLE;
    const after = working.get(name) ?? EMPTY_TABLE;
    let target = root.get(name);
    for (const [key, value] of after) {
      if (before.has(key) && before.get(key) === value) continue;
      if (!target) {
        target = new Map();
        root.set(name, target);
      }
      target.set(key, value);
    }
    if (!target) continue;
    for (const key of before.keys()) {
      if (!after.has(key)) target.delete(key);
    }
  }
}

type RootState = {
  readonly streams: Map<string, number>;
  readonly events: StoredEvent[];
  readonly eventIds: Set<string>;
  readonly checkpoints: Map<string, number>;
  position: number;
  readonly tables: Tables;
  readonly streamMutexes: Map<string, Mutex>;
  readonly projectionLocks: Map<string, AsyncReaderWriterLock>;
  /** Serialises position allocation across transactions, mirroring the SQL positions row lock. */
  readonly positionMutex: Mutex;
};

function lockFor<L>(map: Map<string, L>, key: string, create: () => L): L {
  const existing = map.get(key);
  if (existing) return existing;
  const lock = create();
  map.set(key, lock);
  return lock;
}

function inRange(version: number, options: ReadStreamOptions | undefined): boolean {
  if (options?.fromVersion !== undefined && version < options.fromVersion) return false;
  if (options?.toVersion !== undefined && version > options.toVersion) return false;
  return true;
}

class InMemoryView implements EventStore<InMemoryWriter> {
  private readonly unboundWriter: InMemoryWriter;
  readonly projections: readonly Projection<any, any, InMemoryWriter>[];
  readonly onProjectionError: (
    error: unknown,
    context: { readonly projection: string; readonly streamId: string },
  ) => void;

  private readonly root: RootState;
  private readonly clock: () => Date;
  private readonly bound: boolean;

  // Per-transaction staged state; only meaningful when `bound` is true.
  private pendingEvents: StoredEvent[] = [];
  private readonly pendingEventIds = new Set<string>();
  private readonly pendingStreamVersions = new Map<string, number>();
  private readonly pendingCheckpoints = new Map<string, number>();
  private readonly lockedStreams = new Set<string>();
  private readonly lockedProjections = new Set<string>();
  private acquiredLocks: (() => void)[] = [];
  private positionLocked = false;
  /** Private copy of the projection tables, taken on this transaction's first `writer` access. */
  private staged: { readonly baseline: Tables; readonly writer: InMemoryWriter } | undefined;

  constructor(
    root: RootState,
    clock: () => Date,
    projections: readonly Projection<any, any, InMemoryWriter>[],
    onProjectionError: (error: unknown, context: { projection: string; streamId: string }) => void,
    bound: boolean,
  ) {
    this.root = root;
    this.clock = clock;
    this.projections = projections;
    this.onProjectionError = onProjectionError;
    this.bound = bound;
    this.unboundWriter = { tables: root.tables };
  }

  get writer(): InMemoryWriter {
    if (!this.bound) return this.unboundWriter;
    if (!this.staged) {
      this.staged = { baseline: copyTables(this.root.tables), writer: { tables: copyTables(this.root.tables) } };
    }
    return this.staged.writer;
  }

  private requireBound(operation: string): void {
    if (!this.bound) {
      throw new Error(`${operation} must be called inside a transaction`);
    }
  }

  private effectiveStreamVersion(streamId: string): number {
    return this.pendingStreamVersions.get(streamId) ?? this.root.streams.get(streamId) ?? 0;
  }

  private effectiveEvents(streamId?: string): StoredEvent[] {
    const committed = streamId
      ? this.root.events.filter((e) => e.streamId === streamId)
      : this.root.events.slice();
    const pending = streamId
      ? this.pendingEvents.filter((e) => e.streamId === streamId)
      : this.pendingEvents.slice();
    return [...committed, ...pending];
  }

  async lockStream(streamId: string): Promise<void> {
    this.requireBound("lockStream");
    if (this.lockedStreams.has(streamId)) return;
    const release = await lockFor(this.root.streamMutexes, streamId, () => new Mutex()).acquire();
    this.lockedStreams.add(streamId);
    this.acquiredLocks.push(release);
  }

  async lockProjectionShared(projectionName: string): Promise<void> {
    this.requireBound("lockProjectionShared");
    await this.lockProjection(projectionName, false);
  }

  async lockProjectionExclusive(projectionName: string): Promise<void> {
    this.requireBound("lockProjectionExclusive");
    await this.lockProjection(projectionName, true);
  }

  // Re-entrant per transaction, like a row lock: a second request from the holder behind a
  // queued writer would otherwise wait on itself.
  private async lockProjection(projectionName: string, exclusive: boolean): Promise<void> {
    if (this.lockedProjections.has(projectionName)) return;
    const lock = lockFor(this.root.projectionLocks, projectionName, () => new AsyncReaderWriterLock());
    const handle = exclusive ? await lock.write() : await lock.read();
    this.lockedProjections.add(projectionName);
    this.acquiredLocks.push(() => handle.unlock());
  }

  async append(request: AppendRequest): Promise<AppendResult> {
    if (!this.bound) {
      return this.transaction((tx) => tx.append(request));
    }

    await this.lockStream(request.streamId);

    const currentVersion = this.effectiveStreamVersion(request.streamId);
    if (request.expected.kind === "no-stream" && currentVersion !== 0) {
      throw new StaleStreamVersionError(request.streamId, 0, currentVersion);
    }
    if (request.expected.kind === "exact" && currentVersion !== request.expected.version) {
      throw new StaleStreamVersionError(request.streamId, request.expected.version, currentVersion);
    }

    for (const event of request.events) {
      if (this.root.eventIds.has(event.eventId) || this.pendingEventIds.has(event.eventId)) {
        throw new DuplicateEventIdError(request.streamId, event.eventId);
      }
    }

    if (!this.positionLocked) {
      const release = await this.root.positionMutex.acquire();
      this.acquiredLocks.push(release);
      this.positionLocked = true;
    }
    const basePosition = this.root.position + this.pendingEvents.length;
    const stored: StoredEvent[] = request.events.map((event, index) => ({
      globalPosition: basePosition + index + 1,
      streamId: request.streamId,
      streamVersion: currentVersion + index + 1,
      aggregateType: request.aggregateType,
      eventId: event.eventId,
      eventRevision: event.eventRevision,
      event: { eventType: event.eventType, payload: event.payload, occurredAt: event.occurredAt } as StoredEvent["event"],
      metadata: request.metadata,
      recordedAt: this.clock(),
    }));

    for (const row of stored) {
      this.pendingEvents.push(row);
      this.pendingEventIds.add(row.eventId);
    }
    this.pendingStreamVersions.set(request.streamId, currentVersion + request.events.length);

    return { newVersion: currentVersion + request.events.length, stored };
  }

  async readStream(streamId: string, options?: ReadStreamOptions): Promise<readonly StoredEvent[]> {
    return this.effectiveEvents(streamId)
      .filter((e) => inRange(e.streamVersion, options))
      .sort((a, b) => a.streamVersion - b.streamVersion);
  }

  async readStreams(streamIds: readonly string[]): Promise<ReadonlyMap<string, readonly StoredEvent[]>> {
    const result = new Map<string, readonly StoredEvent[]>();
    for (const streamId of streamIds) {
      result.set(streamId, await this.readStream(streamId));
    }
    return result;
  }

  async readGlobal(options: GlobalReadOptions): Promise<readonly StoredEvent[]> {
    return this.effectiveEvents()
      .filter((e) => e.globalPosition > options.fromPosition)
      .filter((e) => options.aggregateType === undefined || e.aggregateType === options.aggregateType)
      .sort((a, b) => a.globalPosition - b.globalPosition)
      .slice(0, options.limit);
  }

  async readStreamIds(aggregateType: string): Promise<readonly string[]> {
    const ids = new Set<string>();
    for (const event of this.effectiveEvents()) {
      if (event.aggregateType === aggregateType) ids.add(event.streamId);
    }
    return [...ids];
  }

  async currentPosition(): Promise<number> {
    return this.root.position + this.pendingEvents.length;
  }

  async readCheckpoint(projectorName: string): Promise<number> {
    return this.pendingCheckpoints.get(projectorName) ?? this.root.checkpoints.get(projectorName) ?? 0;
  }

  async writeCheckpoint(projectorName: string, position: number): Promise<void> {
    if (!this.bound) {
      return this.transaction((tx) => tx.writeCheckpoint(projectorName, position));
    }
    this.pendingCheckpoints.set(projectorName, position);
  }

  async transaction<T>(fn: (tx: EventStore<InMemoryWriter>) => Promise<T>): Promise<T> {
    if (this.bound) {
      return fn(this);
    }

    const view = new InMemoryView(this.root, this.clock, this.projections, this.onProjectionError, true);
    try {
      const result = await fn(view);
      for (const event of view.pendingEvents) {
        this.root.events.push(event);
        this.root.eventIds.add(event.eventId);
      }
      this.root.position += view.pendingEvents.length;
      for (const [streamId, version] of view.pendingStreamVersions) {
        this.root.streams.set(streamId, version);
      }
      for (const [name, position] of view.pendingCheckpoints) {
        this.root.checkpoints.set(name, position);
      }
      if (view.staged) {
        commitTables(this.root.tables, view.staged.baseline, view.staged.writer.tables);
      }
      return result;
    } finally {
      for (const release of view.acquiredLocks.slice().reverse()) release();
    }
  }

  async savepoint<T>(fn: () => Promise<T>): Promise<T> {
    this.requireBound("savepoint");

    // Unstaged at entry means nothing was written before the savepoint, so rolling back
    // returns the working copy to its baseline.
    const tablesSnap = this.staged ? copyTables(this.staged.writer.tables) : undefined;
    const eventsCount = this.pendingEvents.length;
    const eventIdsSnap = new Set(this.pendingEventIds);
    const streamVersionsSnap = new Map(this.pendingStreamVersions);
    const checkpointsSnap = new Map(this.pendingCheckpoints);

    try {
      return await fn();
    } catch (error) {
      if (this.staged) {
        restoreTables(this.staged.writer.tables, tablesSnap ?? this.staged.baseline);
      }
      this.pendingEvents = this.pendingEvents.slice(0, eventsCount);
      this.pendingEventIds.clear();
      for (const id of eventIdsSnap) this.pendingEventIds.add(id);
      this.pendingStreamVersions.clear();
      for (const [k, v] of streamVersionsSnap) this.pendingStreamVersions.set(k, v);
      this.pendingCheckpoints.clear();
      for (const [k, v] of checkpointsSnap) this.pendingCheckpoints.set(k, v);
      throw error;
    }
  }
}

export function createInMemoryStore(options?: {
  readonly projections?: readonly Projection<any, any, InMemoryWriter>[];
  readonly onProjectionError?: (
    error: unknown,
    context: { readonly projection: string; readonly streamId: string },
  ) => void;
  readonly clock?: () => Date;
}): EventStore<InMemoryWriter> {
  const root: RootState = {
    streams: new Map(),
    events: [],
    eventIds: new Set(),
    checkpoints: new Map(),
    position: 0,
    tables: new Map(),
    streamMutexes: new Map(),
    projectionLocks: new Map(),
    positionMutex: new Mutex(),
  };

  return new InMemoryView(
    root,
    options?.clock ?? (() => new Date()),
    options?.projections ?? [],
    options?.onProjectionError ??
      ((error, context) => {
        console.error(`graven: projection ${context.projection} failed for stream ${context.streamId}`, error);
      }),
    false,
  );
}
