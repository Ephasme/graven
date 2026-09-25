import { StaleStreamVersionError } from "./errors.js";
import type { ProjectionErrorHandler } from "./store.js";
import type { AppendRequest, StoredEvent } from "./types.js";

export function assertExpectedVersion(request: AppendRequest, currentVersion: number): void {
  if (request.expected.kind === "no-stream" && currentVersion !== 0) {
    throw new StaleStreamVersionError(request.streamId, 0, currentVersion);
  }
  if (request.expected.kind === "exact" && currentVersion !== request.expected.version) {
    throw new StaleStreamVersionError(request.streamId, request.expected.version, currentVersion);
  }
}

export function buildStoredEvents(
  request: AppendRequest,
  basePosition: number,
  currentVersion: number,
  recordedAt: Date,
): StoredEvent[] {
  return request.events.map((event, index) => ({
    globalPosition: basePosition + index + 1,
    streamId: request.streamId,
    streamVersion: currentVersion + index + 1,
    aggregateType: request.aggregateType,
    eventId: event.eventId,
    eventRevision: event.eventRevision,
    event: { eventType: event.eventType, payload: event.payload, occurredAt: event.occurredAt },
    metadata: request.metadata,
    recordedAt,
  }));
}

export const defaultProjectionErrorHandler: ProjectionErrorHandler = (error, context) => {
  console.error(`graven: projection ${context.projection} failed for stream ${context.streamId}`, error);
};
