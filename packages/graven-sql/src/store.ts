import {
  DuplicateEventIdError,
  StaleStreamVersionError,
  StreamVersionConflictError,
} from "@les-sherpas/graven";
import type {
  AppendRequest,
  AppendResult,
  EventMetadata,
  EventStore,
  GlobalReadOptions,
  Projection,
  ReadStreamOptions,
  StoredEvent,
} from "@les-sherpas/graven";
import { utc } from "@date-fns/utc";
import { format, parseISO } from "date-fns";
import dbErrors from "db-errors";
import { type Kysely, MysqlAdapter, PostgresAdapter, sql } from "kysely";
import { resolveTableNames, type TableNames } from "./tables.js";

export type SqlEventStore<DB> = EventStore<Kysely<DB>> & {
  migrate(): Promise<void>;
};

export type CreateSqlEventStoreOptions<DB> = {
  /** A `Kysely` over `PostgresDialect` or `MysqlDialect`, or a caller's `Transaction` to join. */
  readonly db: Kysely<DB>;
  readonly tables?: Partial<TableNames>;
  readonly clock?: () => Date;
  readonly projections?: readonly Projection<any, any, Kysely<DB>>[];
  readonly onProjectionError?: (
    error: unknown,
    context: { readonly projection: string; readonly streamId: string },
  ) => void;
};

/** What differs between the two databases once Kysely has compiled the query. */
type Flavor = {
  readonly keyType: "text" | "varchar(255)";
  readonly jsonType: "jsonb" | "json";
  readonly timestampType: "timestamptz" | "datetime(3)";
  /** JSON columns are read back as text: each driver otherwise decodes a bare JSON string differently. */
  readonly jsonTextType: "text" | "char";
  /** How `occurred_at`/`recorded_at` are selected, so the driver never applies a zone. */
  readonly timestampSelectType: "timestamptz" | "char";
  /** Kysely compiles `forShare()` to `for share`, which MariaDB rejects. */
  readonly shareLock: ReturnType<typeof sql>;
  readonly writeTimestamp: (value: Date) => Date | string;
  readonly readTimestamp: (value: unknown) => Date;
  readonly insertIfAbsent: <Q extends { onConflict: any; ignore: any }>(query: Q, column: string) => Q;
};

const postgres: Flavor = {
  keyType: "text",
  jsonType: "jsonb",
  timestampType: "timestamptz",
  jsonTextType: "text",
  timestampSelectType: "timestamptz",
  shareLock: sql`for share`,
  writeTimestamp: (value) => value,
  readTimestamp: (value) => value as Date,
  insertIfAbsent: (query, column) => query.onConflict((oc: any) => oc.column(column).doNothing()),
};

// `datetime(3)` carries no zone, and mysql2 converts through the pool's `timezone` option; binding
// and reading UTC text keeps `occurred_at` right whatever the caller's pool says.
const UTC_DATETIME = "yyyy-MM-dd HH:mm:ss.SSS";

const mysql: Flavor = {
  keyType: "varchar(255)",
  jsonType: "json",
  timestampType: "datetime(3)",
  jsonTextType: "char",
  timestampSelectType: "char",
  shareLock: sql`lock in share mode`,
  writeTimestamp: (value) => format(value, UTC_DATETIME, { in: utc }),
  readTimestamp: (value) => parseISO(value as string, { in: utc }),
  insertIfAbsent: (query) => query.ignore(),
};

function flavorOf(db: Kysely<any>): Flavor {
  const adapter = db.getExecutor().adapter;
  if (adapter instanceof PostgresAdapter) return postgres;
  if (adapter instanceof MysqlAdapter) return mysql;
  throw new Error("graven-sql supports Kysely's PostgresDialect and MysqlDialect only");
}

type Shared = {
  readonly tables: TableNames;
  readonly flavor: Flavor;
  readonly clock: () => Date;
  readonly projections: readonly Projection<any, any, Kysely<any>>[];
  readonly onProjectionError: (error: unknown, context: { projection: string; streamId: string }) => void;
};

class SqlStoreView implements EventStore<Kysely<any>> {
  readonly writer: Kysely<any>;
  readonly projections: readonly Projection<any, any, Kysely<any>>[];
  readonly onProjectionError: Shared["onProjectionError"];

  private readonly db: Kysely<any>;
  private readonly shared: Shared;

  // Per-transaction state; only meaningful when `db.isTransaction`.
  private readonly lockedStreamVersions = new Map<string, number>();
  private savepointCounter = 0;

  constructor(db: Kysely<any>, shared: Shared) {
    this.db = db;
    this.shared = shared;
    this.writer = db;
    this.projections = shared.projections;
    this.onProjectionError = shared.onProjectionError;
  }

  private get t(): TableNames {
    return this.shared.tables;
  }

  private requireBound(operation: string): void {
    if (!this.db.isTransaction) {
      throw new Error(`${operation} must be called inside a transaction`);
    }
  }

  async lockStream(streamId: string): Promise<void> {
    this.requireBound("lockStream");
    if (this.lockedStreamVersions.has(streamId)) return;

    // Row-lock first, insert only on a miss: on InnoDB an `insert ignore` against an existing
    // row takes a shared lock, and a `for update` after it then asks for an upgrade — under
    // concurrent writers to one stream that deadlocks (18/20 failed at 20-way contention).
    const select = () =>
      this.db.selectFrom(this.t.streams).select("version").where("stream_id", "=", streamId).forUpdate().executeTakeFirst();

    let row = await select();
    if (!row) {
      await this.shared.flavor
        .insertIfAbsent(
          this.db.insertInto(this.t.streams).values({
            stream_id: streamId,
            version: 0,
            updated_at: this.shared.flavor.writeTimestamp(this.shared.clock()),
          }),
          "stream_id",
        )
        .execute();
      row = await select();
    }
    this.lockedStreamVersions.set(streamId, Number(row?.version ?? 0));
  }

  private async takeLockRow(exclusive: boolean, projectionName: string): Promise<void> {
    const key = `graven:projection:${projectionName}`;
    // Same row-lock-first ordering as `lockStream`, for the same reason.
    const select = () => {
      const query = this.db.selectFrom(this.t.locks).select("lock_key").where("lock_key", "=", key);
      return (exclusive ? query.forUpdate() : query.modifyEnd(this.shared.flavor.shareLock)).executeTakeFirst();
    };
    if (await select()) return;
    await this.shared.flavor.insertIfAbsent(this.db.insertInto(this.t.locks).values({ lock_key: key }), "lock_key").execute();
    await select();
  }

  async lockProjectionShared(projectionName: string): Promise<void> {
    this.requireBound("lockProjectionShared");
    await this.takeLockRow(false, projectionName);
  }

  async lockProjectionExclusive(projectionName: string): Promise<void> {
    this.requireBound("lockProjectionExclusive");
    await this.takeLockRow(true, projectionName);
  }

  async append(request: AppendRequest): Promise<AppendResult> {
    if (!this.db.isTransaction) {
      return this.transaction((tx) => tx.append(request));
    }

    await this.lockStream(request.streamId);
    const currentVersion = this.lockedStreamVersions.get(request.streamId) ?? 0;

    if (request.expected.kind === "no-stream" && currentVersion !== 0) {
      throw new StaleStreamVersionError(request.streamId, 0, currentVersion);
    }
    if (request.expected.kind === "exact" && currentVersion !== request.expected.version) {
      throw new StaleStreamVersionError(request.streamId, request.expected.version, currentVersion);
    }
    if (request.events.length === 0) {
      return { newVersion: currentVersion, stored: [] };
    }

    const positionRow = await this.db
      .selectFrom(this.t.positions)
      .select("position")
      .where("id", "=", 1)
      .forUpdate()
      .executeTakeFirst();
    const basePosition = Number(positionRow?.position ?? 0);
    await this.db
      .updateTable(this.t.positions)
      .set({ position: basePosition + request.events.length })
      .where("id", "=", 1)
      .execute();

    const now = this.shared.clock();
    const stored: StoredEvent[] = request.events.map((event, index) => ({
      globalPosition: basePosition + index + 1,
      streamId: request.streamId,
      streamVersion: currentVersion + index + 1,
      aggregateType: request.aggregateType,
      eventId: event.eventId,
      eventRevision: event.eventRevision,
      event: { eventType: event.eventType, payload: event.payload, occurredAt: event.occurredAt } as StoredEvent["event"],
      metadata: request.metadata,
      recordedAt: now,
    }));

    const { writeTimestamp } = this.shared.flavor;
    try {
      await this.db
        .insertInto(this.t.events)
        .values(
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
        )
        .execute();
    } catch (error) {
      const wrapped = error instanceof Error ? dbErrors.wrapError(error) : error;
      if (wrapped instanceof dbErrors.UniqueViolationError) {
        // MySQL 8 prefixes the key with its table (`graven_events.graven_events_…`); MariaDB does not.
        if (wrapped.constraint.endsWith(`${this.t.events}_stream_version_key`)) {
          throw new StreamVersionConflictError(request.streamId, currentVersion);
        }
        throw new DuplicateEventIdError(request.streamId, request.events[0]?.eventId ?? "unknown");
      }
      throw error;
    }

    const newVersion = currentVersion + request.events.length;
    await this.db
      .updateTable(this.t.streams)
      .set({ version: newVersion, updated_at: writeTimestamp(now) })
      .where("stream_id", "=", request.streamId)
      .execute();
    this.lockedStreamVersions.set(request.streamId, newVersion);

    return { newVersion, stored };
  }

  private selectEvents() {
    const { jsonTextType, timestampSelectType } = this.shared.flavor;
    return this.db
      .selectFrom(this.t.events)
      .select((eb) => [
        "global_position",
        "stream_id",
        "stream_version",
        "aggregate_type",
        "event_id",
        "event_type",
        "event_revision",
        eb.cast(eb.ref("payload"), jsonTextType).as("payload"),
        eb.cast(eb.ref("metadata"), jsonTextType).as("metadata"),
        eb.cast(eb.ref("occurred_at"), timestampSelectType).as("occurred_at"),
        eb.cast(eb.ref("recorded_at"), timestampSelectType).as("recorded_at"),
      ]);
  }

  private toStoredEvent = (row: Record<string, unknown>): StoredEvent => {
    const { readTimestamp } = this.shared.flavor;
    return {
      globalPosition: Number(row.global_position),
      streamId: row.stream_id as string,
      streamVersion: Number(row.stream_version),
      aggregateType: row.aggregate_type as string,
      eventId: row.event_id as string,
      eventRevision: Number(row.event_revision),
      event: {
        eventType: row.event_type as string,
        payload: JSON.parse(row.payload as string),
        occurredAt: readTimestamp(row.occurred_at),
      } as StoredEvent["event"],
      metadata: JSON.parse(row.metadata as string) as EventMetadata,
      recordedAt: readTimestamp(row.recorded_at),
    };
  };

  async readStream(streamId: string, options?: ReadStreamOptions): Promise<readonly StoredEvent[]> {
    let query = this.selectEvents().where("stream_id", "=", streamId);
    if (options?.fromVersion !== undefined) query = query.where("stream_version", ">=", options.fromVersion);
    if (options?.toVersion !== undefined) query = query.where("stream_version", "<=", options.toVersion);
    const rows = await query.orderBy("stream_version").execute();
    return rows.map(this.toStoredEvent);
  }

  async readStreams(streamIds: readonly string[]): Promise<ReadonlyMap<string, readonly StoredEvent[]>> {
    const map = new Map<string, StoredEvent[]>(streamIds.map((id) => [id, []]));
    if (streamIds.length === 0) return map;
    const rows = await this.selectEvents()
      .where("stream_id", "in", [...streamIds])
      .orderBy("stream_id")
      .orderBy("stream_version")
      .execute();
    for (const row of rows) {
      const event = this.toStoredEvent(row);
      map.get(event.streamId)?.push(event);
    }
    return map;
  }

  async readGlobal(options: GlobalReadOptions): Promise<readonly StoredEvent[]> {
    let query = this.selectEvents().where("global_position", ">", options.fromPosition);
    if (options.aggregateType !== undefined) query = query.where("aggregate_type", "=", options.aggregateType);
    const rows = await query.orderBy("global_position").limit(options.limit).execute();
    return rows.map(this.toStoredEvent);
  }

  async readStreamIds(aggregateType: string): Promise<readonly string[]> {
    const rows = await this.db
      .selectFrom(this.t.events)
      .select("stream_id")
      .distinct()
      .where("aggregate_type", "=", aggregateType)
      .execute();
    return rows.map((row) => row.stream_id as string);
  }

  async currentPosition(): Promise<number> {
    const row = await this.db.selectFrom(this.t.positions).select("position").where("id", "=", 1).executeTakeFirst();
    return Number(row?.position ?? 0);
  }

  async readCheckpoint(projectorName: string): Promise<number> {
    const row = await this.db
      .selectFrom(this.t.checkpoints)
      .select("position")
      .where("projector", "=", projectorName)
      .executeTakeFirst();
    return Number(row?.position ?? 0);
  }

  async writeCheckpoint(projectorName: string, position: number): Promise<void> {
    if (!this.db.isTransaction) {
      await this.transaction((tx) => tx.writeCheckpoint(projectorName, position));
      return;
    }
    const updatedAt = this.shared.flavor.writeTimestamp(this.shared.clock());
    const insert = this.db.insertInto(this.t.checkpoints).values({ projector: projectorName, position, updated_at: updatedAt });
    await (this.shared.flavor === postgres
      ? insert.onConflict((oc) => oc.column("projector").doUpdateSet({ position, updated_at: updatedAt }))
      : insert.onDuplicateKeyUpdate({ position, updated_at: updatedAt })
    ).execute();
  }

  async transaction<T>(fn: (tx: EventStore<Kysely<any>>) => Promise<T>): Promise<T> {
    if (this.db.isTransaction) return fn(this);
    return this.db.transaction().execute((trx) => fn(new SqlStoreView(trx, this.shared)));
  }

  async savepoint<T>(fn: () => Promise<T>): Promise<T> {
    this.requireBound("savepoint");
    const name = sql.id(`graven_sp_${++this.savepointCounter}`);
    await sql`savepoint ${name}`.execute(this.db);
    try {
      const result = await fn();
      await sql`release savepoint ${name}`.execute(this.db);
      return result;
    } catch (error) {
      await sql`rollback to savepoint ${name}`.execute(this.db);
      throw error;
    }
  }
}

async function migrate(db: Kysely<any>, tables: TableNames, flavor: Flavor): Promise<void> {
  const { keyType, jsonType, timestampType } = flavor;
  // MySQL has no `create index if not exists`, so indexes are created with their table.
  const existing = new Set((await db.introspection.getTables()).map((table) => table.name));

  await db.schema
    .createTable(tables.positions)
    .ifNotExists()
    .addColumn("id", "integer", (c) => c.primaryKey())
    .addColumn("position", "bigint", (c) => c.notNull())
    .execute();
  await flavor.insertIfAbsent(db.insertInto(tables.positions).values({ id: 1, position: 0 }), "id").execute();

  await db.schema
    .createTable(tables.streams)
    .ifNotExists()
    .addColumn("stream_id", keyType, (c) => c.primaryKey())
    .addColumn("version", "integer", (c) => c.notNull())
    .addColumn("updated_at", timestampType, (c) => c.notNull())
    .execute();

  if (!existing.has(tables.events)) {
    await db.schema
      .createTable(tables.events)
      .ifNotExists()
      .addColumn("global_position", "bigint", (c) => c.primaryKey())
      .addColumn("stream_id", keyType, (c) => c.notNull())
      .addColumn("stream_version", "integer", (c) => c.notNull())
      .addColumn("aggregate_type", keyType, (c) => c.notNull())
      .addColumn("event_id", keyType, (c) => c.notNull())
      .addColumn("event_type", keyType, (c) => c.notNull())
      .addColumn("event_revision", "integer", (c) => c.notNull())
      .addColumn("payload", jsonType, (c) => c.notNull())
      .addColumn("metadata", jsonType, (c) => c.notNull())
      .addColumn("occurred_at", timestampType, (c) => c.notNull())
      .addColumn("recorded_at", timestampType, (c) => c.notNull())
      .addUniqueConstraint(`${tables.events}_stream_version_key`, ["stream_id", "stream_version"])
      .addUniqueConstraint(`${tables.events}_event_id_key`, ["event_id"])
      .execute();
    await db.schema
      .createIndex(`${tables.events}_aggregate_position_idx`)
      .on(tables.events)
      .columns(["aggregate_type", "global_position"])
      .execute();
    await db.schema
      .createIndex(`${tables.events}_type_position_idx`)
      .on(tables.events)
      .columns(["event_type", "global_position"])
      .execute();
  }

  await db.schema
    .createTable(tables.checkpoints)
    .ifNotExists()
    .addColumn("projector", keyType, (c) => c.primaryKey())
    .addColumn("position", "bigint", (c) => c.notNull())
    .addColumn("updated_at", timestampType, (c) => c.notNull())
    .execute();

  await db.schema.createTable(tables.locks).ifNotExists().addColumn("lock_key", keyType, (c) => c.primaryKey()).execute();
}

export function createSqlEventStore<DB>(options: CreateSqlEventStoreOptions<DB>): SqlEventStore<DB> {
  const db = options.db as Kysely<any>;
  const shared: Shared = {
    tables: resolveTableNames(options.tables),
    flavor: flavorOf(db),
    clock: options.clock ?? (() => new Date()),
    projections: options.projections ?? [],
    onProjectionError:
      options.onProjectionError ??
      ((error, context) => {
        console.error(`graven: projection ${context.projection} failed for stream ${context.streamId}`, error);
      }),
  };

  return Object.assign(new SqlStoreView(db, shared), {
    migrate: () => migrate(db, shared.tables, shared.flavor),
  }) as unknown as SqlEventStore<DB>;
}
