import {
  DummyDriver,
  Kysely,
  MysqlAdapter,
  MysqlIntrospector,
  MysqlQueryCompiler,
  OperationNodeTransformer,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
  type ColumnType,
  type TableNode,
} from "kysely";
import type { TableNames } from "./tables.js";

type Integer = ColumnType<number | string, number, number>; // bigint comes back as a string from pg
type Timestamp = ColumnType<Date | string, Date | string, Date | string>;

/** graven's own compile-only schema: queries are built against this, then compiled and run
 * through the caller's `Kysely<DB>` executor, with table names renamed to the caller's config. */
export type GravenDatabase = {
  graven_events: {
    global_position: ColumnType<number | string, number, never>;
    stream_id: string;
    stream_version: number;
    aggregate_type: string;
    event_id: string;
    event_type: string;
    event_revision: number;
    payload: ColumnType<unknown, string, never>;
    metadata: ColumnType<unknown, string, never>;
    occurred_at: ColumnType<Date | string, Date | string, never>;
    recorded_at: ColumnType<Date | string, Date | string, never>;
  };
  graven_streams: { stream_id: string; version: number; updated_at: Timestamp };
  graven_positions: { id: number; position: Integer };
  graven_checkpoints: { projector: string; position: Integer; updated_at: Timestamp };
  graven_locks: { lock_key: string };
};

export type GravenTable = keyof GravenDatabase;

const GRAVEN_TABLES = [
  "graven_events",
  "graven_streams",
  "graven_positions",
  "graven_checkpoints",
  "graven_locks",
] as const satisfies readonly GravenTable[];

const GRAVEN_TABLE_SET: ReadonlySet<string> = new Set(GRAVEN_TABLES);

function isGravenTable(name: string): name is GravenTable {
  return GRAVEN_TABLE_SET.has(name);
}

/** Rewrites every logical `graven_*` table reference to the caller's configured name. */
class RenameTables extends OperationNodeTransformer {
  readonly #names: Readonly<Record<GravenTable, string>>;

  constructor(names: Readonly<Record<GravenTable, string>>) {
    super();
    this.#names = names;
  }

  protected override transformTable(node: TableNode): TableNode {
    const transformed = super.transformTable(node);
    const name = transformed.table.identifier.name;
    if (!isGravenTable(name)) return transformed;
    const renamed = this.#names[name];
    return {
      ...transformed,
      table: { ...transformed.table, identifier: { ...transformed.table.identifier, name: renamed } },
    };
  }
}

/** A compile-only `Kysely<GravenDatabase>` over `kind`, renaming logical table names to `names`
 * on every compiled query. It never executes; execution runs through the caller's own executor
 * via `execute`/`selectRows` in `store.ts`, so the caller's plugins and transaction still apply. */
export function createQueryBuilder(
  kind: "postgres" | "mysql",
  names: Readonly<Record<GravenTable, string>>,
): Kysely<GravenDatabase> {
  const transformer = new RenameTables(names);

  return new Kysely<GravenDatabase>({
    dialect: {
      createAdapter: () => (kind === "postgres" ? new PostgresAdapter() : new MysqlAdapter()),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => (kind === "postgres" ? new PostgresIntrospector(db) : new MysqlIntrospector(db)),
      createQueryCompiler: () => (kind === "postgres" ? new PostgresQueryCompiler() : new MysqlQueryCompiler()),
    },
    plugins: [
      {
        transformQuery: (args) => transformer.transformNode(args.node, args.queryId),
        transformResult: async (args) => args.result,
      },
    ],
  });
}

export function namesFor(tables: TableNames): Readonly<Record<GravenTable, string>> {
  return {
    graven_events: tables.events,
    graven_streams: tables.streams,
    graven_positions: tables.positions,
    graven_checkpoints: tables.checkpoints,
    graven_locks: tables.locks,
  };
}
