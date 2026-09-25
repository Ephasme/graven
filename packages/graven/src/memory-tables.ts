export type TableMap<T extends object> = { [K in keyof T]?: Map<string, T[K]> };

/** A transaction's typed read-model tables. The table name is the key; the row type is fixed
 * per table by `T`.
 *
 * `tables` is resolved on each `table()` call; a transaction stages its private copy on the
 * first one. */
export class InMemoryWriter<T extends object> {
  readonly #tables: () => TableMap<T>;

  constructor(tables: () => TableMap<T>) {
    this.#tables = tables;
  }

  /** The table's rows by key; created empty on first access. */
  table<K extends keyof T & string>(name: K): Map<string, T[K]> {
    const tables = this.#tables();
    // `hasOwn` first: a plain-object `tables` otherwise returns `Object.prototype[name]` for
    // names like `constructor`, typed as a Map but not one.
    let table = Object.hasOwn(tables, name) ? tables[name] : undefined;
    if (!table) {
      table = new Map();
      tables[name] = table;
    }
    return table;
  }
}

function copyTable<T extends object, K extends keyof T>(target: TableMap<T>, source: TableMap<T>, key: K): void {
  const table = source[key];
  if (table) target[key] = new Map(table);
}

export function copyTables<T extends object>(tables: TableMap<T>): TableMap<T> {
  const copy: TableMap<T> = {};
  for (const key in tables) {
    copyTable(copy, tables, key);
  }
  return copy;
}

/** Replaces `target`'s contents in place, so a writer handle captured earlier stays live. */
export function restoreTables<T extends object>(target: TableMap<T>, source: TableMap<T>): void {
  for (const key in target) delete target[key];
  for (const key in source) copyTable(target, source, key);
}

const EMPTY_TABLE: ReadonlyMap<string, never> = new Map<string, never>();

/**
 * Applies to `root` only the rows `working` changed relative to `baseline`. Writing the whole
 * working copy back would erase rows another transaction committed after this one began.
 */
export function commitTables<T extends object>(root: TableMap<T>, baseline: TableMap<T>, working: TableMap<T>): void {
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
