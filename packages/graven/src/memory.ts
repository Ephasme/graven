import { AsyncReaderWriterLock } from "@esfx/async-readerwriterlock";
import { Mutex } from "async-mutex";
import { DuplicateEventIdError, StaleStreamVersionError } from "./errors.js";
import type { Projection } from "./projection.js";
import type { EventStore, GlobalReadOptions, ReadStreamOptions } from "./store.js";
import type { AppendRequest, AppendResult, StoredEvent } from "./types.js";

type TableMap<T extends object> = { [K in keyof T]?: Map<string, T[K]> };

/** A transaction's typed read-model tables. The table name is the key; the row type is fixed
 * per table by `T`. */
export class InMemoryWriter<T extends object> {
  readonly #tables: TableMap<T>;

  constructor(tables: TableMap<T>) {
    this.#tables = tables;
  }

  /** The table's rows by key; created empty on first access. */
  table<K extends keyof T & string>(name: K): Map<string, T[K]> {
    // `hasOwn` first: a plain-object `#tables` otherwise returns `Object.prototype[name]` for
    // names like `constructor`, typed as a Map but not one.
    let table = Object.hasOwn(this.#tables, name) ? this.#tables[name] : undefined;
    if (!table) {
      table = new Map();
      this.#tables[name] = table;
    }
    return table;
  }
}

function copyTable<T extends object, K extends keyof T>(target: TableMap<T>, source: TableMap<T>, key: K): void {
  const table = source[key];
  if (table) target[key] = new Map(table);
}

function copyTables<T extends object>(tables: TableMap<T>): TableMap<T> {
  const copy: TableMap<T> = {};
  for (const key in tables) {
    copyTable(copy, tables, key);
  }
  return copy;
}

/** Replaces `target`'s contents in place, so a writer handle captured earlier stays live. */
function restoreTables<T extends object>(target: TableMap<T>, source: TableMap<T>): void {
  for (const key in target) delete target[key];
  for (const key in source) copyTable(target, source, key);
}

const EMPTY_TABLE: ReadonlyMap<string, never> = new Map<string, never>();

/**
 * Applies to `root` only the rows `working` changed relative to `baseline`. Writing the whole
 * working copy back would erase rows another transaction committed after this one began.
 */
function commitTables<T extends object>(root: TableMap<T>, baseline: TableMap<T>, working: TableMap<T>): void {
  const keys = new Set<keyof T & string>();
  for (const key in baseline) keys.add(key);
  for (const key in working) keys.add(key);
  for (const key of keys) {
    const before: ReadonlyMap<string, T[typeof key]> = baseline[key] ?? EMPTY_TABLE;
    const after: ReadonlyMap<string, T[typeof key]> = working[key] ?? EMPTY_TABLE;
    let target = root[key];
    for (const [rowKey, value] of after) {
      if (before.has(rowKey) && before.get(rowKey) === value) continue;
      if (!target) {
        target = new Map();
        root[key] = target;
      }
      target.set(rowKey, value);
    }
    if (!target) continue;
    for (const rowKey of before.keys()) {
      if (!after.has(rowKey)) target.delete(rowKey);
    }
  }
}

type RootState<T extends object> = {
  readonly streams: Map<string, number>;
  readonly events: StoredEvent[];
  readonly eventIds: Set<string>;
  readonly checkpoints: Map<string, number>;
  position: number;
  readonly tables: TableMap<T>;
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

class InMemoryView<T extends object> implements EventStore<InMemoryWriter<T>> {
  private readonly unboundWriter: InMemoryWriter<T>;
  readonly projections: readonly Projection<InMemoryWriter<T>>[];
  readonly onProjectionError: (
    error: unknown,
    context: { readonly projection: string; readonly streamId: string },
  ) => void;

  private readonly root: RootState<T>;
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
  private staged:
    | { readonly baseline: TableMap<T>; readonly working: TableMap<T>; readonly writer: InMemoryWriter<T> }
    | undefined;

  constructor(
    root: RootState<T>,
    clock: () => Date,
    projections: readonly Projection<InMemoryWriter<T>>[],
    onProjectionError: (error: unknown, context: { projection: string; streamId: string }) => void,
    bound: boolean,
  ) {
    this.root = root;
    this.clock = clock;
    this.projections = projections;
    this.onProjectionError = onProjectionError;
    this.bound = bound;
    this.unboundWriter = new InMemoryWriter(root.tables);
  }

  get writer(): InMemoryWriter<T> {
    if (!this.bound) return this.unboundWriter;
    if (!this.staged) {
      const baseline = copyTables(this.root.tables);
      const working = copyTables(this.root.tables);
      this.staged = { baseline, working, writer: new InMemoryWriter(working) };
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
      event: { eventType: event.eventType, payload: event.payload, occurredAt: event.occurredAt },
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

  async transaction<T2>(fn: (tx: EventStore<InMemoryWriter<T>>) => Promise<T2>): Promise<T2> {
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
        commitTables(this.root.tables, view.staged.baseline, view.staged.working);
      }
      return result;
    } finally {
      for (const release of view.acquiredLocks.slice().reverse()) release();
    }
  }

  async savepoint<T2>(fn: () => Promise<T2>): Promise<T2> {
    this.requireBound("savepoint");

    // Unstaged at entry means nothing was written before the savepoint, so rolling back
    // returns the working copy to its baseline.
    const tablesSnap = this.staged ? copyTables(this.staged.working) : undefined;
    const eventsCount = this.pendingEvents.length;
    const eventIdsSnap = new Set(this.pendingEventIds);
    const streamVersionsSnap = new Map(this.pendingStreamVersions);
    const checkpointsSnap = new Map(this.pendingCheckpoints);

    try {
      return await fn();
    } catch (error) {
      if (this.staged) {
        restoreTables(this.staged.working, tablesSnap ?? this.staged.baseline);
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

export function createInMemoryStore<T extends object = Record<never, never>>(options?: {
  readonly projections?: readonly Projection<InMemoryWriter<T>>[];
  readonly onProjectionError?: (
    error: unknown,
    context: { readonly projection: string; readonly streamId: string },
  ) => void;
  readonly clock?: () => Date;
}): EventStore<InMemoryWriter<T>> {
  const root: RootState<T> = {
    streams: new Map(),
    events: [],
    eventIds: new Set(),
    checkpoints: new Map(),
    position: 0,
    tables: {},
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
