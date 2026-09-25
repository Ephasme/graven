import type { AppendRequest, AppendResult, EventStore, EventStoreTransaction } from "../src/index.js";

type Forwarded<W> = Pick<
  EventStore<W>,
  "readStream" | "readGlobal" | "readStreamIds" | "currentPosition" | "readCheckpoint" | "writeCheckpoint"
>;

function forward<W>(target: EventStore<W>): Forwarded<W> {
  return {
    readStream: (streamId) => target.readStream(streamId),
    readGlobal: (options) => target.readGlobal(options),
    readStreamIds: (aggregateType) => target.readStreamIds(aggregateType),
    currentPosition: () => target.currentPosition(),
    readCheckpoint: (projectorName) => target.readCheckpoint(projectorName),
    writeCheckpoint: (projectorName, position) => target.writeCheckpoint(projectorName, position),
  };
}

/** Wraps `store` so every `append` call runs through `intercept` instead of the real store,
 * with `tx` the real (possibly transaction-scoped) store to fall through to. Every other
 * `EventStore` member forwards unchanged, including across `transaction`/`savepoint`. */
export function interceptAppend<W>(
  store: EventStore<W>,
  intercept: (request: AppendRequest, tx: EventStore<W>) => Promise<AppendResult>,
): EventStore<W> {
  function wrapTransaction(tx: EventStoreTransaction<W>): EventStoreTransaction<W> {
    return {
      ...forward(tx),
      get writer(): W {
        return tx.writer;
      },
      projections: tx.projections,
      onProjectionError: tx.onProjectionError,
      lockStream: (streamId) => tx.lockStream(streamId),
      lockProjectionShared: (projectionName) => tx.lockProjectionShared(projectionName),
      lockProjectionExclusive: (projectionName) => tx.lockProjectionExclusive(projectionName),
      lockCheckpoint: (projectorName) => tx.lockCheckpoint(projectorName),
      savepoint: (fn) => tx.savepoint(fn),
      append: (request) => intercept(request, tx),
      transaction: (fn) => tx.transaction((inner) => fn(wrapTransaction(inner))),
    };
  }

  return {
    ...forward(store),
    get writer(): W {
      return store.writer;
    },
    projections: store.projections,
    onProjectionError: store.onProjectionError,
    append: (request) => intercept(request, store),
    transaction: (fn) => store.transaction((tx) => fn(wrapTransaction(tx))),
  };
}
