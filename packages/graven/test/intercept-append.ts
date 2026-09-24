import type { AppendRequest, AppendResult, EventStore } from "../src/index.js";

/** Wraps `store` so every `append` call runs through `intercept` instead of the real store,
 * with `tx` the real (possibly transaction-scoped) store to fall through to. Every other
 * `EventStore` member forwards unchanged, including across `transaction`/`savepoint`. */
export function interceptAppend<W>(
  store: EventStore<W>,
  intercept: (request: AppendRequest, tx: EventStore<W>) => Promise<AppendResult>,
): EventStore<W> {
  function wrap(target: EventStore<W>): EventStore<W> {
    return {
      get writer(): W {
        return target.writer;
      },
      projections: target.projections,
      onProjectionError: target.onProjectionError,
      lockStream: (streamId) => target.lockStream(streamId),
      lockProjectionShared: (projectionName) => target.lockProjectionShared(projectionName),
      lockProjectionExclusive: (projectionName) => target.lockProjectionExclusive(projectionName),
      append: (request) => intercept(request, target),
      readStream: (streamId, options) => target.readStream(streamId, options),
      readStreams: (streamIds) => target.readStreams(streamIds),
      readGlobal: (options) => target.readGlobal(options),
      readStreamIds: (aggregateType) => target.readStreamIds(aggregateType),
      currentPosition: () => target.currentPosition(),
      readCheckpoint: (projectorName) => target.readCheckpoint(projectorName),
      writeCheckpoint: (projectorName, position) => target.writeCheckpoint(projectorName, position),
      transaction: (fn) => target.transaction((inner) => fn(wrap(inner))),
      savepoint: (fn) => target.savepoint(fn),
    };
  }

  return wrap(store);
}
