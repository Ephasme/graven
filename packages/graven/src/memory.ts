import { AsyncReaderWriterLock } from "@esfx/async-readerwriterlock";
import { Mutex } from "async-mutex";
import { assertExpectedVersion, buildStoredEvents, defaultProjectionErrorHandler } from "./adapter.js";
import { DuplicateEventIdError } from "./errors.js";
import type { TableMap } from "./memory-tables.js";
import { InMemoryWriter, commitTables, copyTables, restoreTables } from "./memory-tables.js";
import type { EventStore, EventStoreTransaction, GlobalReadOptions, Projection, ProjectionErrorHandler } from "./store.js";
import type { AppendRequest, AppendResult, StoredEvent } from "./types.js";

type RootState<T extends object> = {
  readonly streams: Map<string, number>;
  readonly events: StoredEvent[];
  readonly eventIds: Set<string>;
  readonly checkpoints: Map<string, number>;
  position: number;
  readonly tables: TableMap<T>;
  readonly streamMutexes: Map<string, Mutex>;
  readonly projectionLocks: Map<string, AsyncReaderWriterLock>;
  readonly checkpointMutexes: Map<string, Mutex>;
  /** Serialises position allocation across transactions, mirroring the SQL positions row lock. */
  readonly positionMutex: Mutex;
};

type Shared<T extends object> = {
  readonly root: RootState<T>;
  readonly clock: () => Date;
  readonly projections: readonly Projection<InMemoryWriter<T>>[];
  readonly onProjectionError: ProjectionErrorHandler;
};

type LockMode = "shared" | "exclusive";

function lockFor<L>(map: Map<string, L>, key: string, create: () => L): L {
  const existing = map.get(key);
  if (existing) return existing;
  const lock = create();
  map.set(key, lock);
  return lock;
}

abstract class InMemoryReader<T extends object> {
  readonly projections: readonly Projection<InMemoryWriter<T>>[];
  readonly onProjectionError: ProjectionErrorHandler;
  protected readonly shared: Shared<T>;

  constructor(shared: Shared<T>) {
    this.shared = shared;
    this.projections = shared.projections;
    this.onProjectionError = shared.onProjectionError;
  }

  protected abstract stagedEvents(): readonly StoredEvent[];
  protected abstract pendingCheckpoint(name: string): number | undefined;

  private effectiveEvents(): StoredEvent[] {
    return [...this.shared.root.events, ...this.stagedEvents()];
  }

  async readStream(streamId: string): Promise<readonly StoredEvent[]> {
    return this.effectiveEvents()
      .filter((e) => e.streamId === streamId)
      .sort((a, b) => a.streamVersion - b.streamVersion);
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
    return this.shared.root.position + this.stagedEvents().length;
  }

  async readCheckpoint(projectorName: string): Promise<number> {
    return this.pendingCheckpoint(projectorName) ?? this.shared.root.checkpoints.get(projectorName) ?? 0;
  }
}

class InMemoryStore<T extends object> extends InMemoryReader<T> implements EventStore<InMemoryWriter<T>> {
  readonly writer = new InMemoryWriter<T>(() => this.shared.root.tables);

  protected stagedEvents(): readonly StoredEvent[] {
    return [];
  }

  protected pendingCheckpoint(): number | undefined {
    return undefined;
  }

  async append(request: AppendRequest): Promise<AppendResult> {
    return this.transaction((tx) => tx.append(request));
  }

  async writeCheckpoint(projectorName: string, position: number): Promise<void> {
    return this.transaction((tx) => tx.writeCheckpoint(projectorName, position));
  }

  async transaction<T2>(fn: (tx: EventStoreTransaction<InMemoryWriter<T>>) => Promise<T2>): Promise<T2> {
    const tx = new InMemoryTransaction(this.shared);
    try {
      const result = await fn(tx);
      tx.commit();
      return result;
    } finally {
      tx.releaseLocks();
    }
  }
}

class InMemoryTransaction<T extends object> extends InMemoryReader<T> implements EventStoreTransaction<InMemoryWriter<T>> {
  readonly writer = new InMemoryWriter<T>(() => this.stageTables());

  private pendingEvents: StoredEvent[] = [];
  private readonly pendingEventIds = new Set<string>();
  private readonly pendingStreamVersions = new Map<string, number>();
  private readonly pendingCheckpoints = new Map<string, number>();
  private readonly lockedStreams = new Set<string>();
  private readonly lockedCheckpoints = new Set<string>();
  private readonly heldProjections = new Map<string, { readonly mode: LockMode; readonly unlock: () => void }>();
  private readonly releases: (() => void)[] = [];
  private positionLocked = false;
  private staged: { readonly baseline: TableMap<T>; readonly working: TableMap<T> } | undefined;

  protected stagedEvents(): readonly StoredEvent[] {
    return this.pendingEvents;
  }

  protected pendingCheckpoint(name: string): number | undefined {
    return this.pendingCheckpoints.get(name);
  }

  private stageTables(): TableMap<T> {
    if (!this.staged) {
      const baseline = copyTables(this.shared.root.tables);
      const working = copyTables(this.shared.root.tables);
      this.staged = { baseline, working };
    }
    return this.staged.working;
  }

  private effectiveStreamVersion(streamId: string): number {
    return this.pendingStreamVersions.get(streamId) ?? this.shared.root.streams.get(streamId) ?? 0;
  }

  async lockStream(streamId: string): Promise<void> {
    if (this.lockedStreams.has(streamId)) return;
    const release = await lockFor(this.shared.root.streamMutexes, streamId, () => new Mutex()).acquire();
    this.lockedStreams.add(streamId);
    this.releases.push(release);
  }

  async lockProjectionShared(projectionName: string): Promise<void> {
    await this.lockProjection(projectionName, "shared");
  }

  async lockProjectionExclusive(projectionName: string): Promise<void> {
    await this.lockProjection(projectionName, "exclusive");
  }

  private async lockProjection(projectionName: string, mode: LockMode): Promise<void> {
    const held = this.heldProjections.get(projectionName);
    if (held && (held.mode === "exclusive" || mode === "shared")) return;
    // Upgrade by release-then-acquire: holding the read lock while queueing for the write
    // lock would wait on ourselves. Another writer may run in between, where SQL would
    // instead deadlock one of two upgrading holders.
    if (held) {
      this.heldProjections.delete(projectionName);
      held.unlock();
    }
    const lock = lockFor(this.shared.root.projectionLocks, projectionName, () => new AsyncReaderWriterLock());
    const handle = mode === "exclusive" ? await lock.write() : await lock.read();
    this.heldProjections.set(projectionName, { mode, unlock: () => handle.unlock() });
  }

  async lockCheckpoint(projectorName: string): Promise<number> {
    if (!this.lockedCheckpoints.has(projectorName)) {
      const release = await lockFor(this.shared.root.checkpointMutexes, projectorName, () => new Mutex()).acquire();
      this.releases.push(release);
      this.lockedCheckpoints.add(projectorName);
    }
    return this.readCheckpoint(projectorName);
  }

  async append(request: AppendRequest): Promise<AppendResult> {
    await this.lockStream(request.streamId);

    const currentVersion = this.effectiveStreamVersion(request.streamId);
    assertExpectedVersion(request, currentVersion);

    for (const event of request.events) {
      if (this.shared.root.eventIds.has(event.eventId) || this.pendingEventIds.has(event.eventId)) {
        throw new DuplicateEventIdError(request.streamId, event.eventId);
      }
    }

    if (!this.positionLocked) {
      const release = await this.shared.root.positionMutex.acquire();
      this.releases.push(release);
      this.positionLocked = true;
    }
    const basePosition = this.shared.root.position + this.pendingEvents.length;
    const stored = buildStoredEvents(request, basePosition, currentVersion, this.shared.clock());

    for (const row of stored) {
      this.pendingEvents.push(row);
      this.pendingEventIds.add(row.eventId);
    }
    this.pendingStreamVersions.set(request.streamId, currentVersion + request.events.length);

    return { newVersion: currentVersion + request.events.length, stored };
  }

  async writeCheckpoint(projectorName: string, position: number): Promise<void> {
    this.pendingCheckpoints.set(projectorName, position);
  }

  async transaction<T2>(fn: (tx: EventStoreTransaction<InMemoryWriter<T>>) => Promise<T2>): Promise<T2> {
    return fn(this);
  }

  async savepoint<T2>(fn: () => Promise<T2>): Promise<T2> {
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

  releaseLocks(): void {
    for (const release of this.releases.slice().reverse()) release();
    for (const held of this.heldProjections.values()) held.unlock();
  }

  commit(): void {
    for (const event of this.pendingEvents) {
      this.shared.root.events.push(event);
      this.shared.root.eventIds.add(event.eventId);
    }
    this.shared.root.position += this.pendingEvents.length;
    for (const [streamId, version] of this.pendingStreamVersions) {
      this.shared.root.streams.set(streamId, version);
    }
    for (const [name, position] of this.pendingCheckpoints) {
      this.shared.root.checkpoints.set(name, position);
    }
    if (this.staged) {
      commitTables(this.shared.root.tables, this.staged.baseline, this.staged.working);
    }
  }
}

export function createInMemoryStore<T extends object = Record<never, never>>(options?: {
  readonly projections?: readonly Projection<InMemoryWriter<T>>[];
  readonly onProjectionError?: ProjectionErrorHandler;
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
    checkpointMutexes: new Map(),
    positionMutex: new Mutex(),
  };

  const shared: Shared<T> = {
    root,
    clock: options?.clock ?? (() => new Date()),
    projections: options?.projections ?? [],
    onProjectionError: options?.onProjectionError ?? defaultProjectionErrorHandler,
  };

  return new InMemoryStore(shared);
}
