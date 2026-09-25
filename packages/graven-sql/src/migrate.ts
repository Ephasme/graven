import type { Kysely } from "kysely";
import { execute } from "./execute.js";
import { POSITIONS_ROW_ID, streamVersionConstraint, type GravenDatabase } from "./schema.js";
import type { TableNames } from "./tables.js";
import type { Flavor } from "./dialect.js";

export type MigrationTarget<DB> = {
  readonly db: Kysely<DB>;
  readonly builder: Kysely<GravenDatabase>;
  readonly flavor: Flavor;
  readonly tables: TableNames;
};

async function createPositionsTable<DB>(target: MigrationTarget<DB>): Promise<void> {
  await execute(
    target.db,
    target.builder.schema
      .createTable("graven_positions")
      .ifNotExists()
      .addColumn("id", "integer", (c) => c.primaryKey())
      .addColumn("position", "bigint", (c) => c.notNull()),
  );
  await execute(
    target.db,
    target.flavor.insertIfAbsent(
      target.builder.insertInto("graven_positions").values({ id: POSITIONS_ROW_ID, position: 0 }),
      "id",
    ),
  );
}

async function createStreamsTable<DB>(target: MigrationTarget<DB>): Promise<void> {
  const { keyType, timestampType } = target.flavor;
  await execute(
    target.db,
    target.builder.schema
      .createTable("graven_streams")
      .ifNotExists()
      .addColumn("stream_id", keyType, (c) => c.primaryKey())
      .addColumn("version", "integer", (c) => c.notNull())
      .addColumn("updated_at", timestampType, (c) => c.notNull()),
  );
}

async function createEventsTable<DB>(target: MigrationTarget<DB>, existing: ReadonlySet<string>): Promise<void> {
  if (existing.has(target.tables.events)) return;

  const { keyType, jsonType, timestampType } = target.flavor;
  await execute(
    target.db,
    target.builder.schema
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
      .addUniqueConstraint(streamVersionConstraint(target.tables), ["stream_id", "stream_version"])
      .addUniqueConstraint(`${target.tables.events}_event_id_key`, ["event_id"]),
  );
  // MySQL has no `create index if not exists`, so indexes are created with their table.
  await execute(
    target.db,
    target.builder.schema
      .createIndex(`${target.tables.events}_aggregate_position_idx`)
      .on("graven_events")
      .columns(["aggregate_type", "global_position"]),
  );
}

async function createCheckpointsTable<DB>(target: MigrationTarget<DB>): Promise<void> {
  const { keyType, timestampType } = target.flavor;
  await execute(
    target.db,
    target.builder.schema
      .createTable("graven_checkpoints")
      .ifNotExists()
      .addColumn("projector", keyType, (c) => c.primaryKey())
      .addColumn("position", "bigint", (c) => c.notNull())
      .addColumn("updated_at", timestampType, (c) => c.notNull()),
  );
}

async function createLocksTable<DB>(target: MigrationTarget<DB>): Promise<void> {
  await execute(
    target.db,
    target.builder.schema.createTable("graven_locks").ifNotExists().addColumn("lock_key", target.flavor.keyType, (c) => c.primaryKey()),
  );
}

export async function migrateSchema<DB>(target: MigrationTarget<DB>): Promise<void> {
  const existing = new Set((await target.db.introspection.getTables()).map((table) => table.name));
  await createPositionsTable(target);
  await createStreamsTable(target);
  await createEventsTable(target, existing);
  await createCheckpointsTable(target);
  await createLocksTable(target);
}
