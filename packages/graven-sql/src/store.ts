import {
  DuplicateEventIdError,
  eventMetadataSchema,
  InvalidStoredRowError,
  jsonValueSchema,
  StaleStreamVersionError,
  StreamVersionConflictError,
} from "@les-sherpas/graven";
import type {
  AppendRequest,
  AppendResult,
  EventMetadata,
  EventStore,
  GlobalReadOptions,
  JsonValue,
  Projection,
  ReadStreamOptions,
  StoredEvent,
} from "@les-sherpas/graven";
import { utc } from "@date-fns/utc";
import { format, parseISO } from "date-fns";
import dbErrors from "db-errors";
import {
  MysqlAdapter,
  PostgresAdapter,
  createQueryId,
  sql,
  type Compilable,
  type InsertQueryBuilder,
  type Kysely,
  type QueryResult,
  type RootOperationNode,
  type Transaction,
} from "kysely";
import { z } from "zod";
import { createQueryBuilder, namesFor, type GravenDatabase, type GravenTable } from "./schema.js";
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
  readonly onProjectionError?: (
    error: unknown,
    context: { readonly projection: string; readonly streamId: string },
  ) => void;
};

/** Compiles and runs a graven query through the caller's executor: its plugins, schema and
 * transaction apply. */
async function execute<DB>(
  db: Kysely<DB>,
  query: { toOperationNode(): RootOperationNode },
): Promise<QueryResult<unknown>> {
  const executor = db.getExecutor();
  const queryId = createQueryId();
  return executor.executeQuery(executor.compileQuery(executor.transformQuery(query.toOperationNode(), queryId), queryId));
}

/** `schema` must accept every row Kysely types the query as returning, so the decoder cannot
 * drift from the select list. */
async function selectRows<DB, O, S extends z.ZodType>(
  db: Kysely<DB>,
  table: GravenTable,
  query: { toOperationNode(): RootOperationNode } & Compilable<O>,
  schema: S & ([O] extends [z.input<S>] ? unknown : { readonly rowMismatch: O }),
): Promise<z.output<S>[]> {
  const result = z.array(schema).safeParse((await execute(db, query)).rows);
  if (!result.success) throw new InvalidStoredRowError(table, result.error.issues);
  return result.data;
}

const dbInteger = z.union([z.int(), z.string().regex(/^-?\d+$/).transform(Number).pipe(z.int())]);

const jsonText = z.string().transform((text, ctx): unknown => {
  try {
    return JSON.parse(text);
  } catch {
    ctx.issues.push({ code: "custom", message: "column holds malformed JSON", input: text });
    return z.NEVER;
  }
});

const payloadColumn: z.ZodType<JsonValue, string> = jsonText.pipe(jsonValueSchema);
const metadataColumn: z.ZodType<EventMetadata, string> = jsonText.pipe(eventMetadataSchema);

function rejectTimestamp(message: string) {
  return (value: Date | string, ctx: z.core.$RefinementCtx): never => {
    ctx.issues.push({ code: "custom", message, input: value });
    return z.NEVER;
  };
}

const postgresTimestamp: z.ZodType<Date, Date | string> = z.union([
  z.date(),
  z.string().transform(rejectTimestamp("timestamptz must arrive as a Date")),
]);

const mysqlTimestamp: z.ZodType<Date, Date | string> = z.union([
  z.string().transform((value, ctx) => {
    const date = parseISO(value, { in: utc });
    return Number.isNaN(date.getTime()) ? rejectTimestamp("invalid datetime")(value, ctx) : date;
  }),
  z.date().transform(rejectTimestamp("datetime must be selected as char")),
]);

const storedEventRowShape = {
  global_position: dbInteger,
  stream_id: z.string(),
  stream_version: dbInteger,
  aggregate_type: z.string(),
  event_id: z.string(),
  event_type: z.string(),
  event_revision: dbInteger,
  payload: payloadColumn,
  metadata: metadataColumn,
};

function storedEventRowSchema(timestamp: z.ZodType<Date, Date | string>): z.ZodType<StoredEvent, unknown> {
  return z
    .object({ ...storedEventRowShape, occurred_at: timestamp, recorded_at: timestamp })
    .transform(
      (row): StoredEvent => ({
        globalPosition: row.global_position,
        streamId: row.stream_id,
        streamVersion: row.stream_version,
        aggregateType: row.aggregate_type,
        eventId: row.event_id,
        eventRevision: row.event_revision,
        event: { eventType: row.event_type, payload: row.payload, occurredAt: row.occurred_at },
        metadata: row.metadata,
        recordedAt: row.recorded_at,
      }),
    );
}

/** What differs between the two databases once Kysely has compiled the query. */
type Flavor = {
  readonly kind: "postgres" | "mysql";
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
  readonly timestamp: z.ZodType<Date, Date | string>;
  readonly storedEventRow: z.ZodType<StoredEvent, unknown>;
};

const postgres: Flavor = {
  kind: "postgres",
  keyType: "text",
  jsonType: "jsonb",
  timestampType: "timestamptz",
  jsonTextType: "text",
  timestampSelectType: "timestamptz",
  shareLock: sql`for share`,
  writeTimestamp: (value) => value,
  timestamp: postgresTimestamp,
  storedEventRow: storedEventRowSchema(postgresTimestamp),
};

// `datetime(3)` carries no zone, and mysql2 converts through the pool's `timezone` option; binding
// and reading UTC text keeps `occurred_at` right whatever the caller's pool says.
const UTC_DATETIME = "yyyy-MM-dd HH:mm:ss.SSS";

const mysql: Flavor = {
  kind: "mysql",
  keyType: "varchar(255)",
  jsonType: "json",
  timestampType: "datetime(3)",
  jsonTextType: "char",
  timestampSelectType: "char",
  shareLock: sql`lock in share mode`,
  writeTimestamp: (value) => format(value, UTC_DATETIME, { in: utc }),
  timestamp: mysqlTimestamp,
  storedEventRow: storedEventRowSchema(mysqlTimestamp),
};

function insertIfAbsent<TB extends GravenTable, O>(
  kind: Flavor["kind"],
  query: InsertQueryBuilder<GravenDatabase, TB, O>,
  column: keyof GravenDatabase[TB] & string,
): InsertQueryBuilder<GravenDatabase, TB, O> {
  return kind === "postgres" ? query.onConflict((oc) => oc.column(column).doNothing()) : query.ignore();
}

function flavorOf<DB>(db: Kysely<DB>): Flavor {
  const adapter = db.getExecutor().adapter;
  if (adapter instanceof PostgresAdapter) return postgres;
  if (adapter instanceof MysqlAdapter) return mysql;
  throw new Error("graven-sql supports Kysely's PostgresDialect and MysqlDialect only");
}

type Shared<DB> = {
  readonly tables: TableNames;
  readonly flavor: Flavor;
  readonly clock: () => Date;
  readonly builder: Kysely<GravenDatabase>;
  readonly projections: readonly Projection<Kysely<DB>>[];
  readonly onProjectionError: (error: unknown, context: { projection: string; streamId: string }) => void;
};

class SqlStoreView<DB> implements EventStore<Kysely<DB>> {
  readonly writer: Kysely<DB>;
  readonly projections: readonly Projection<Kysely<DB>>[];
  readonly onProjectionError: Shared<DB>["onProjectionError"];

  private readonly db: Kysely<DB>;
  private readonly shared: Shared<DB>;

  // Per-transaction state; only meaningful when `db.isTransaction`.
  private readonly lockedStreamVersions = new Map<string, number>();
  private savepointCounter = 0;

  constructor(db: Kysely<DB>, shared: Shared<DB>) {
    this.db = db;
    this.shared = shared;
    this.writer = db;
    this.projections = shared.projections;
    this.onProjectionError = shared.onProjectionError;
  }

  private get t(): TableNames {
    return this.shared.tables;
  }

  private get b(): Kysely<GravenDatabase> {
    return this.shared.builder;
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
      selectRows(
        this.db,
        "graven_streams",
        this.b.selectFrom("graven_streams").select("version").where("stream_id", "=", streamId).forUpdate(),
        z.object({ version: dbInteger }),
      );

    let rows = await select();
    if (rows.length === 0) {
      await execute(
        this.db,
        insertIfAbsent(
          this.shared.flavor.kind,
          this.b.insertInto("graven_streams").values({
            stream_id: streamId,
            version: 0,
            updated_at: this.shared.flavor.writeTimestamp(this.shared.clock()),
          }),
          "stream_id",
        ),
      );
      rows = await select();
    }
    this.lockedStreamVersions.set(streamId, rows[0]?.version ?? 0);
  }

  private async takeLockRow(exclusive: boolean, projectionName: string): Promise<void> {
    const key = `graven:projection:${projectionName}`;
    // Same row-lock-first ordering as `lockStream`, for the same reason.
    const select = () => {
      const query = this.b.selectFrom("graven_locks").select("lock_key").where("lock_key", "=", key);
      return selectRows(
        this.db,
        "graven_locks",
        exclusive ? query.forUpdate() : query.modifyEnd(this.shared.flavor.shareLock),
        z.object({ lock_key: z.string() }),
      );
    };
    if ((await select()).length > 0) return;
    await execute(this.db, insertIfAbsent(this.shared.flavor.kind, this.b.insertInto("graven_locks").values({ lock_key: key }), "lock_key"));
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

    const positionRows = await selectRows(
      this.db,
      "graven_positions",
      this.b.selectFrom("graven_positions").select("position").where("id", "=", 1).forUpdate(),
      z.object({ position: dbInteger }),
    );
    const basePosition = positionRows[0]?.position ?? 0;
    await execute(
      this.db,
      this.b.updateTable("graven_positions").set({ position: basePosition + request.events.length }).where("id", "=", 1),
    );

    const now = this.shared.clock();
    const stored: StoredEvent[] = request.events.map((event, index) => ({
      globalPosition: basePosition + index + 1,
      streamId: request.streamId,
      streamVersion: currentVersion + index + 1,
      aggregateType: request.aggregateType,
      eventId: event.eventId,
      eventRevision: event.eventRevision,
      event: { eventType: event.eventType, payload: event.payload, occurredAt: event.occurredAt },
      metadata: request.metadata,
      recordedAt: now,
    }));

    const { writeTimestamp } = this.shared.flavor;
    try {
      await execute(
        this.db,
        this.b.insertInto("graven_events").values(
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
    await execute(
      this.db,
      this.b
        .updateTable("graven_streams")
        .set({ version: newVersion, updated_at: writeTimestamp(now) })
        .where("stream_id", "=", request.streamId),
    );
    this.lockedStreamVersions.set(request.streamId, newVersion);

    return { newVersion, stored };
  }

  private selectEvents() {
    const { jsonTextType, timestampSelectType } = this.shared.flavor;
    return this.b
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

  async readStream(streamId: string, options?: ReadStreamOptions): Promise<readonly StoredEvent[]> {
    let query = this.selectEvents().where("stream_id", "=", streamId);
    if (options?.fromVersion !== undefined) query = query.where("stream_version", ">=", options.fromVersion);
    if (options?.toVersion !== undefined) query = query.where("stream_version", "<=", options.toVersion);
    return selectRows(this.db, "graven_events", query.orderBy("stream_version"), this.shared.flavor.storedEventRow);
  }

  async readStreams(streamIds: readonly string[]): Promise<ReadonlyMap<string, readonly StoredEvent[]>> {
    const map = new Map<string, StoredEvent[]>(streamIds.map((id) => [id, []]));
    if (streamIds.length === 0) return map;
    const rows = await selectRows(
      this.db,
      "graven_events",
      this.selectEvents().where("stream_id", "in", [...streamIds]).orderBy("stream_id").orderBy("stream_version"),
      this.shared.flavor.storedEventRow,
    );
    for (const event of rows) {
      map.get(event.streamId)?.push(event);
    }
    return map;
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
      this.b.selectFrom("graven_events").select("stream_id").distinct().where("aggregate_type", "=", aggregateType),
      z.object({ stream_id: z.string() }),
    );
    return rows.map((row) => row.stream_id);
  }

  async currentPosition(): Promise<number> {
    const rows = await selectRows(
      this.db,
      "graven_positions",
      this.b.selectFrom("graven_positions").select("position").where("id", "=", 1),
      z.object({ position: dbInteger }),
    );
    return rows[0]?.position ?? 0;
  }

  async readCheckpoint(projectorName: string): Promise<number> {
    const rows = await selectRows(
      this.db,
      "graven_checkpoints",
      this.b.selectFrom("graven_checkpoints").select("position").where("projector", "=", projectorName),
      z.object({ position: dbInteger }),
    );
    return rows[0]?.position ?? 0;
  }

  async writeCheckpoint(projectorName: string, position: number): Promise<void> {
    if (!this.db.isTransaction) {
      await this.transaction((tx) => tx.writeCheckpoint(projectorName, position));
      return;
    }
    const updatedAt = this.shared.flavor.writeTimestamp(this.shared.clock());
    const insert = this.b.insertInto("graven_checkpoints").values({ projector: projectorName, position, updated_at: updatedAt });
    await execute(
      this.db,
      this.shared.flavor.kind === "postgres"
        ? insert.onConflict((oc) => oc.column("projector").doUpdateSet({ position, updated_at: updatedAt }))
        : insert.onDuplicateKeyUpdate({ position, updated_at: updatedAt }),
    );
  }

  async transaction<T>(fn: (tx: EventStore<Kysely<DB>>) => Promise<T>): Promise<T> {
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

  async migrate(): Promise<void> {
    const { keyType, jsonType, timestampType } = this.shared.flavor;
    // MySQL has no `create index if not exists`, so indexes are created with their table.
    const existing = new Set((await this.db.introspection.getTables()).map((table) => table.name));

    await execute(
      this.db,
      this.b.schema
        .createTable("graven_positions")
        .ifNotExists()
        .addColumn("id", "integer", (c) => c.primaryKey())
        .addColumn("position", "bigint", (c) => c.notNull()),
    );
    await execute(this.db, insertIfAbsent(this.shared.flavor.kind, this.b.insertInto("graven_positions").values({ id: 1, position: 0 }), "id"));

    await execute(
      this.db,
      this.b.schema
        .createTable("graven_streams")
        .ifNotExists()
        .addColumn("stream_id", keyType, (c) => c.primaryKey())
        .addColumn("version", "integer", (c) => c.notNull())
        .addColumn("updated_at", timestampType, (c) => c.notNull()),
    );

    if (!existing.has(this.t.events)) {
      await execute(
        this.db,
        this.b.schema
          .createTable("graven_events")
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
          .addUniqueConstraint(`${this.t.events}_stream_version_key`, ["stream_id", "stream_version"])
          .addUniqueConstraint(`${this.t.events}_event_id_key`, ["event_id"]),
      );
      await execute(
        this.db,
        this.b.schema.createIndex(`${this.t.events}_aggregate_position_idx`).on("graven_events").columns(["aggregate_type", "global_position"]),
      );
      await execute(
        this.db,
        this.b.schema.createIndex(`${this.t.events}_type_position_idx`).on("graven_events").columns(["event_type", "global_position"]),
      );
    }

    await execute(
      this.db,
      this.b.schema
        .createTable("graven_checkpoints")
        .ifNotExists()
        .addColumn("projector", keyType, (c) => c.primaryKey())
        .addColumn("position", "bigint", (c) => c.notNull())
        .addColumn("updated_at", timestampType, (c) => c.notNull()),
    );

    await execute(
      this.db,
      this.b.schema.createTable("graven_locks").ifNotExists().addColumn("lock_key", keyType, (c) => c.primaryKey()),
    );
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
    builder: createQueryBuilder(flavor.kind, namesFor(tables)),
    projections: options.projections ?? [],
    onProjectionError:
      options.onProjectionError ??
      ((error, context) => {
        console.error(`graven: projection ${context.projection} failed for stream ${context.streamId}`, error);
      }),
  };

  return new SqlStoreView(db, shared);
}
