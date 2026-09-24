export type TableNames = {
  readonly events: string;
  readonly streams: string;
  readonly positions: string;
  readonly checkpoints: string;
  readonly locks: string;
};

const DEFAULT_TABLE_NAMES: TableNames = {
  events: "graven_events",
  streams: "graven_streams",
  positions: "graven_positions",
  checkpoints: "graven_checkpoints",
  locks: "graven_locks",
};

const VALID_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Constraint and index names are derived from these, so they must be plain identifiers. */
export function resolveTableNames(overrides?: Partial<TableNames>): TableNames {
  const resolved: TableNames = { ...DEFAULT_TABLE_NAMES, ...overrides };
  for (const [key, value] of Object.entries(resolved)) {
    if (!VALID_IDENTIFIER.test(value)) {
      throw new RangeError(`invalid table name for ${key}: ${value}`);
    }
  }
  return resolved;
}
