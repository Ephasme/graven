import { eventMetadataSchema, jsonValueSchema } from "@les-sherpas/graven";
import type { EventMetadata, JsonValue, StoredEvent } from "@les-sherpas/graven";
import { utc } from "@date-fns/utc";
import { format, parseISO } from "date-fns";
import {
  DummyDriver,
  MysqlAdapter,
  MysqlIntrospector,
  MysqlQueryCompiler,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
  sql,
  type Dialect,
  type InsertQueryBuilder,
  type InsertResult,
  type Kysely,
  type RawBuilder,
} from "kysely";
import { z } from "zod";
import type { GravenDatabase, GravenTable } from "./schema.js";

export const dbInteger: z.ZodType<number, number | bigint | string> = z.union([
  z.int(),
  z.bigint().transform(Number).pipe(z.int()),
  z.string().regex(/^-?\d+$/).transform(Number).pipe(z.int()),
]);

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

/** The row `selectRows` must be handed for a `StoredEvent` to decode: typing the schema by its
 * input keeps the drift guard live, so a select list missing a column fails to compile. */
export type StoredEventRow = {
  global_position: number | bigint | string;
  stream_id: string;
  stream_version: number | bigint | string;
  aggregate_type: string;
  event_id: string;
  event_type: string;
  event_revision: number | bigint | string;
  payload: string;
  metadata: string;
  occurred_at: Date | string;
  recorded_at: Date | string;
};

function storedEventRowSchema(timestamp: z.ZodType<Date, Date | string>): z.ZodType<StoredEvent, StoredEventRow> {
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

function postgresInsertIfAbsent<TB extends GravenTable, O>(
  query: InsertQueryBuilder<GravenDatabase, TB, O>,
  column: keyof GravenDatabase[TB] & string,
): InsertQueryBuilder<GravenDatabase, TB, O> {
  return query.onConflict((oc) => oc.column(column).doNothing());
}

function mysqlInsertIfAbsent<TB extends GravenTable, O>(
  query: InsertQueryBuilder<GravenDatabase, TB, O>,
  _column: keyof GravenDatabase[TB] & string,
): InsertQueryBuilder<GravenDatabase, TB, O> {
  return query.ignore();
}

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
  readonly shareLock: RawBuilder<unknown>;
  readonly writeTimestamp: (value: Date) => Date | string;
  readonly storedEventRow: z.ZodType<StoredEvent, StoredEventRow>;
  /** Compile-only dialect for graven's query builder; its driver never executes. */
  readonly compiler: Dialect;
  readonly insertIfAbsent: <TB extends GravenTable, O>(
    query: InsertQueryBuilder<GravenDatabase, TB, O>,
    column: keyof GravenDatabase[TB] & string,
  ) => InsertQueryBuilder<GravenDatabase, TB, O>;
  readonly upsertCheckpoint: (
    query: InsertQueryBuilder<GravenDatabase, "graven_checkpoints", InsertResult>,
    set: { readonly position: number; readonly updated_at: Date | string },
  ) => InsertQueryBuilder<GravenDatabase, "graven_checkpoints", InsertResult>;
};

const postgres: Flavor = {
  keyType: "text",
  jsonType: "jsonb",
  timestampType: "timestamptz",
  jsonTextType: "text",
  timestampSelectType: "timestamptz",
  shareLock: sql`for share`,
  writeTimestamp: (value) => value,
  storedEventRow: storedEventRowSchema(postgresTimestamp),
  compiler: {
    createAdapter: () => new PostgresAdapter(),
    createDriver: () => new DummyDriver(),
    createIntrospector: (db) => new PostgresIntrospector(db),
    createQueryCompiler: () => new PostgresQueryCompiler(),
  },
  insertIfAbsent: postgresInsertIfAbsent,
  upsertCheckpoint: (query, set) => query.onConflict((oc) => oc.column("projector").doUpdateSet(set)),
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
  storedEventRow: storedEventRowSchema(mysqlTimestamp),
  compiler: {
    createAdapter: () => new MysqlAdapter(),
    createDriver: () => new DummyDriver(),
    createIntrospector: (db) => new MysqlIntrospector(db),
    createQueryCompiler: () => new MysqlQueryCompiler(),
  },
  insertIfAbsent: mysqlInsertIfAbsent,
  upsertCheckpoint: (query, set) => query.onDuplicateKeyUpdate(set),
};

export function flavorOf<DB>(db: Kysely<DB>): Flavor {
  const adapter = db.getExecutor().adapter;
  if (adapter instanceof PostgresAdapter) return postgres;
  if (adapter instanceof MysqlAdapter) return mysql;
  throw new Error("graven-sql supports Kysely's PostgresDialect and MysqlDialect only");
}

export type { Flavor };
