import {
  DuplicateEventIdError,
  StreamVersionConflictError,
} from "@les-sherpas/graven";
import type {
  AppendRequest,
  AppendResult,
  EventStore,
  EventStoreTransaction,
  GlobalReadOptions,
  Projection,
  ProjectionErrorHandler,
  StoredEvent,
} from "@les-sherpas/graven";
import { assertExpectedVersion, buildStoredEvents, defaultProjectionErrorHandler } from "@les-sherpas/graven";
import dbErrors from "db-errors";
import { sql, type Kysely, type Transaction } from "kysely";
import { z } from "zod";
import { dbInteger, flavorOf, type Flavor } from "./dialect.js";
import { execute, selectRows } from "./execute.js";
import { migrateSchema } from "./migrate.js";
import { createQueryBuilder, namesFor, POSITIONS_ROW_ID, streamVersionConstraint, type GravenDatabase } from "./schema.js";
import { resolveTableNames, type TableNames } from "./tables.js";

export type SqlEventStore<DB> = EventStore<Kysely<DB>> & {
  migrate(): Promise<void>;
};

export type CreateSqlEventStoreOptions<DB> = {
  /** A `Kysely` over `PostgresDialect` or `MysqlDialect`, or a caller's `Transaction` to join. */
  readonly db: Kysely<DB> | Transaction<DB>;
  readonly tables?: Partial<TableNames>;
  readonly clock?: () => Date;
  readonly projections?: readonly Projection<Kysely<DB>>[];
  readonly onProjectionError?: ProjectionErrorHandler;
};

type LockMode = "shared" | "exclusive";

type Shared<DB> = {
  readonly tables: TableNames;
  readonly flavor: Flavor;
  readonly clock: () => Date;
  readonly builder: Kysely<GravenDatabase>;
  readonly projections: readonly Projection<Kysely<DB>>[];
  readonly onProjectionError: ProjectionErrorHandler;
};

abstract class SqlReader<DB> {
  readonly writer: Kysely<DB>;
  readonly projections: readonly Projection<Kysely<DB>>[];
  readonly onProjectionError: ProjectionErrorHandler;

  protected readonly db: Kysely<DB>;
  protected readonly shared: Shared<DB>;

  constructor(db: Kysely<DB>, shared: Shared<DB>) {
    this.db = db;
    this.shared = shared;
    this.writer = db;
    this.projections = shared.projections;
    this.onProjectionError = shared.onProjectionError;
  }

  protected selectEvents() {
    const { jsonTextType, timestampSelectType } = this.shared.flavor;
    return this.shared.builder
      .selectFrom("graven_events")
      .select((eb) => [
        "global_position",
        "stream_id",
        "stream_version",
        "aggregate_type",
        "event_id",
        "event_type",
        "event_revision",
        eb.cast<string>(eb.ref("payload"), jsonTextType).as("payload"),
        eb.cast<string>(eb.ref("metadata"), jsonTextType).as("metadata"),
        eb.cast<Date | string>(eb.ref("occurred_at"), timestampSelectType).as("occurred_at"),
        eb.cast<Date | string>(eb.ref("recorded_at"), timestampSelectType).as("recorded_at"),
      ]);
  }

  async readStream(streamId: string): Promise<readonly StoredEvent[]> {
    const query = this.selectEvents().where("stream_id", "=", streamId);
    return selectRows(this.db, "graven_events", query.orderBy("stream_version"), this.shared.flavor.storedEventRow);
  }

  async readGlobal(options: GlobalReadOptions): Promise<readonly StoredEvent[]> {
    let query = this.selectEvents().where("global_position", ">", options.fromPosition);
    if (options.aggregateType !== undefined) query = query.where("aggregate_type", "=", options.aggregateType);
    return selectRows(this.db, "graven_events", query.orderBy("global_position").limit(options.limit), this.shared.flavor.storedEventRow);
  }

  async readStreamIds(aggregateType: string): Promise<readonly string[]> {
    const rows = await selectRows(
      this.db,
      "graven_events",
      this.shared.builder.selectFrom("graven_events").select("stream_id").distinct().where("aggregate_type", "=", aggregateType),
      z.object({ stream_id: z.string() }),
    );
    return rows.map((row) => row.stream_id);
  }

  async currentPosition(): Promise<number> {
    const rows = await selectRows(
      this.db,
      "graven_positions",
      this.shared.builder.selectFrom("graven_positions").select("position").where("id", "=", POSITIONS_ROW_ID),
      z.object({ position: dbInteger }),
    );
    return rows[0]?.position ?? 0;
  }

  async readCheckpoint(projectorName: string): Promise<number> {
    const rows = await selectRows(
      this.db,
      "graven_checkpoints",
      this.shared.builder.selectFrom("graven_checkpoints").select("position").where("projector", "=", projectorName),
      z.object({ position: dbInteger }),
    );
    return rows[0]?.position ?? 0;
  }
}

class SqlStore<DB> extends SqlReader<DB> implements SqlEventStore<DB> {
  async append(request: AppendRequest): Promise<AppendResult> {
    return this.transaction((tx) => tx.append(request));
  }

  async writeCheckpoint(projectorName: string, position: number): Promise<void> {
    return this.transaction((tx) => tx.writeCheckpoint(projectorName, position));
  }

  async transaction<T>(fn: (tx: EventStoreTransaction<Kysely<DB>>) => Promise<T>): Promise<T> {
    if (this.db.isTransaction) return fn(new SqlTransaction(this.db, this.shared));
    return this.db.transaction().execute((trx) => fn(new SqlTransaction(trx, this.shared)));
  }

  async migrate(): Promise<void> {
    await migrateSchema({ db: this.db, builder: this.shared.builder, flavor: this.shared.flavor, tables: this.shared.tables });
  }
}

class SqlTransaction<DB> extends SqlReader<DB> implements EventStoreTransaction<Kysely<DB>> {
  private readonly lockedStreamVersions = new Map<string, number>();
  private savepointCounter = 0;

  /**
   * Row-locks first, inserts only on a miss. On InnoDB an `insert ignore` against an existing
   * row takes a shared lock, and a `for update` after it then asks for an upgrade — under
   * concurrent writers to one stream that deadlocks (18/20 failed at 20-way contention).
   */
  private async lockOrCreate<R>(select: () => Promise<readonly R[]>, create: () => Promise<unknown>): Promise<readonly R[]> {
    const rows = await select();
    if (rows.length > 0) return rows;
    await create();
    return select();
  }

  async lockStream(streamId: string): Promise<void> {
    if (this.lockedStreamVersions.has(streamId)) return;

    const select = () =>
      selectRows(
        this.db,
        "graven_streams",
        this.shared.builder.selectFrom("graven_streams").select("version").where("stream_id", "=", streamId).forUpdate(),
        z.object({ version: dbInteger }),
      );
    const create = () =>
      execute(
        this.db,
        this.shared.flavor.insertIfAbsent(
          this.shared.builder.insertInto("graven_streams").values({
            stream_id: streamId,
            version: 0,
            updated_at: this.shared.flavor.writeTimestamp(this.shared.clock()),
          }),
          "stream_id",
        ),
      );

    const rows = await this.lockOrCreate(select, create);
    this.lockedStreamVersions.set(streamId, rows[0]?.version ?? 0);
  }

  private async lockProjection(projectionName: string, mode: LockMode): Promise<void> {
    const key = `graven:projection:${projectionName}`;
    const select = () => {
      const query = this.shared.builder.selectFrom("graven_locks").select("lock_key").where("lock_key", "=", key);
      return selectRows(
        this.db,
        "graven_locks",
        mode === "exclusive" ? query.forUpdate() : query.modifyEnd(this.shared.flavor.shareLock),
        z.object({ lock_key: z.string() }),
      );
    };
    const create = () =>
      execute(this.db, this.shared.flavor.insertIfAbsent(this.shared.builder.insertInto("graven_locks").values({ lock_key: key }), "lock_key"));

    await this.lockOrCreate(select, create);
  }

  async lockProjectionShared(projectionName: string): Promise<void> {
    await this.lockProjection(projectionName, "shared");
  }

  async lockProjectionExclusive(projectionName: string): Promise<void> {
    await this.lockProjection(projectionName, "exclusive");
  }

  async lockCheckpoint(projectorName: string): Promise<number> {
    const select = () =>
      selectRows(
        this.db,
        "graven_checkpoints",
        this.shared.builder.selectFrom("graven_checkpoints").select("position").where("projector", "=", projectorName).forUpdate(),
        z.object({ position: dbInteger }),
      );
    const create = () =>
      execute(
        this.db,
        this.shared.flavor.insertIfAbsent(
          this.shared.builder
            .insertInto("graven_checkpoints")
            .values({ projector: projectorName, position: 0, updated_at: this.shared.flavor.writeTimestamp(this.shared.clock()) }),
          "projector",
        ),
      );

    const rows = await this.lockOrCreate(select, create);
    return rows[0]?.position ?? 0;
  }

  private async allocatePositions(count: number): Promise<number> {
    const positionRows = await selectRows(
      this.db,
      "graven_positions",
      this.shared.builder.selectFrom("graven_positions").select("position").where("id", "=", POSITIONS_ROW_ID).forUpdate(),
      z.object({ position: dbInteger }),
    );
    const basePosition = positionRows[0]?.position ?? 0;
    await execute(
      this.db,
      this.shared.builder.updateTable("graven_positions").set({ position: basePosition + count }).where("id", "=", POSITIONS_ROW_ID),
    );
    return basePosition;
  }

  private async insertEvents(request: AppendRequest, stored: readonly StoredEvent[], currentVersion: number): Promise<void> {
    const { writeTimestamp } = this.shared.flavor;
    try {
      await execute(
        this.db,
        this.shared.builder.insertInto("graven_events").values(
          stored.map((row) => ({
            global_position: row.globalPosition,
            stream_id: row.streamId,
            stream_version: row.streamVersion,
            aggregate_type: row.aggregateType,
            event_id: row.eventId,
            event_type: row.event.eventType,
            event_revision: row.eventRevision,
            payload: JSON.stringify(row.event.payload),
            metadata: JSON.stringify(row.metadata),
            occurred_at: writeTimestamp(row.event.occurredAt),
            recorded_at: writeTimestamp(row.recordedAt),
          })),
        ),
      );
    } catch (error) {
      this.translateInsertError(error, request, currentVersion);
    }
  }

  private translateInsertError(error: unknown, request: AppendRequest, currentVersion: number): never {
    const wrapped = error instanceof Error ? dbErrors.wrapError(error) : error;
    if (wrapped instanceof dbErrors.UniqueViolationError) {
      // MySQL 8 prefixes the key with its table (`graven_events.graven_events_…`); MariaDB does not.
      if (wrapped.constraint.endsWith(streamVersionConstraint(this.shared.tables))) {
        throw new StreamVersionConflictError(request.streamId, currentVersion);
      }
      throw new DuplicateEventIdError(request.streamId, request.events[0]?.eventId ?? "unknown");
    }
    throw error;
  }

  async append(request: AppendRequest): Promise<AppendResult> {
    await this.lockStream(request.streamId);
    const currentVersion = this.lockedStreamVersions.get(request.streamId) ?? 0;

    assertExpectedVersion(request, currentVersion);

    if (request.events.length === 0) {
      return { newVersion: currentVersion, stored: [] };
    }

    const basePosition = await this.allocatePositions(request.events.length);
    const stored = buildStoredEvents(request, basePosition, currentVersion, this.shared.clock());

    await this.insertEvents(request, stored, currentVersion);

    const newVersion = currentVersion + request.events.length;
    await execute(
      this.db,
      this.shared.builder
        .updateTable("graven_streams")
        .set({ version: newVersion, updated_at: this.shared.flavor.writeTimestamp(this.shared.clock()) })
        .where("stream_id", "=", request.streamId),
    );
    this.lockedStreamVersions.set(request.streamId, newVersion);

    return { newVersion, stored };
  }

  async writeCheckpoint(projectorName: string, position: number): Promise<void> {
    const updatedAt = this.shared.flavor.writeTimestamp(this.shared.clock());
    const insert = this.shared.builder.insertInto("graven_checkpoints").values({ projector: projectorName, position, updated_at: updatedAt });
    await execute(this.db, this.shared.flavor.upsertCheckpoint(insert, { position, updated_at: updatedAt }));
  }

  async transaction<T>(fn: (tx: EventStoreTransaction<Kysely<DB>>) => Promise<T>): Promise<T> {
    return fn(this);
  }

  async savepoint<T>(fn: () => Promise<T>): Promise<T> {
    const name = sql.id(`graven_sp_${++this.savepointCounter}`);
    // The version cache must roll back with the rows, or the next append trusts a version
    // the database no longer has.
    const versions = new Map(this.lockedStreamVersions);
    await sql`savepoint ${name}`.execute(this.db);
    try {
      const result = await fn();
      await sql`release savepoint ${name}`.execute(this.db);
      return result;
    } catch (error) {
      await sql`rollback to savepoint ${name}`.execute(this.db);
      this.lockedStreamVersions.clear();
      for (const [streamId, version] of versions) this.lockedStreamVersions.set(streamId, version);
      throw error;
    }
  }
}

export function createSqlEventStore<DB>(options: CreateSqlEventStoreOptions<DB>): SqlEventStore<DB> {
  const db = options.db;
  const tables = resolveTableNames(options.tables);
  const flavor = flavorOf(db);
  const shared: Shared<DB> = {
    tables,
    flavor,
    clock: options.clock ?? (() => new Date()),
    builder: createQueryBuilder(flavor.compiler, namesFor(tables)),
    projections: options.projections ?? [],
    onProjectionError: options.onProjectionError ?? defaultProjectionErrorHandler,
  };

  return new SqlStore(db, shared);
}
