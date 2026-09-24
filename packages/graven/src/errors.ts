export abstract class GravenError extends Error {
  abstract readonly code: string;
  readonly context: Readonly<Record<string, unknown>>;

  constructor(message: string, context: Readonly<Record<string, unknown>> = {}) {
    super(message);
    this.name = new.target.name;
    this.context = context;
  }
}

export class UnknownEventTypeError extends GravenError {
  readonly code = "GRAVEN_UNKNOWN_EVENT_TYPE";

  constructor(eventType: string) {
    super(`unknown event type: ${eventType}`, { eventType });
  }
}

export class UnknownEventRevisionError extends GravenError {
  readonly code = "GRAVEN_UNKNOWN_EVENT_REVISION";

  constructor(eventType: string, eventRevision: number) {
    super(`unknown revision ${eventRevision} for event type: ${eventType}`, {
      eventType,
      eventRevision,
    });
  }
}

export class InvalidEventPayloadError extends GravenError {
  readonly code = "GRAVEN_INVALID_EVENT_PAYLOAD";

  constructor(eventType: string, eventRevision: number, issues: readonly unknown[]) {
    super(`invalid payload for event type: ${eventType} (revision ${eventRevision})`, {
      eventType,
      eventRevision,
      issues,
    });
  }
}

export class DuplicateEventFamilyError extends GravenError {
  readonly code = "GRAVEN_DUPLICATE_EVENT_FAMILY";

  constructor(eventType: string, revision: number) {
    super(`duplicate event family: ${eventType}`, { eventType, revision });
  }
}

export class DuplicateProjectorError extends GravenError {
  readonly code = "GRAVEN_DUPLICATE_PROJECTOR";

  constructor(name: string) {
    super(`duplicate projector: ${name}`, { name });
  }
}

export class StaleStreamVersionError extends GravenError {
  readonly code = "GRAVEN_STALE_STREAM_VERSION";

  constructor(streamId: string, expected: number, actual: number) {
    super(`stale stream version for ${streamId}: expected ${expected}, actual ${actual}`, {
      streamId,
      expected,
      actual,
    });
  }
}

export class StreamVersionConflictError extends GravenError {
  readonly code = "GRAVEN_STREAM_VERSION_CONFLICT";

  constructor(streamId: string, expected: number) {
    super(`stream version conflict for ${streamId}: expected ${expected}`, {
      streamId,
      expected,
    });
  }
}

export class DuplicateEventIdError extends GravenError {
  readonly code = "GRAVEN_DUPLICATE_EVENT_ID";

  constructor(streamId: string, eventId: string) {
    super(`duplicate event id for stream ${streamId}: ${eventId}`, { streamId, eventId });
  }
}

export class ConcurrentModificationError extends GravenError {
  readonly code = "GRAVEN_CONCURRENT_MODIFICATION";

  constructor(streamId: string, attempts: number) {
    super(`concurrent modification of stream ${streamId} after ${attempts} attempts`, {
      streamId,
      attempts,
    });
  }
}

export class ProjectionNotRebuildableError extends GravenError {
  readonly code = "GRAVEN_PROJECTION_NOT_REBUILDABLE";

  constructor(projection: string) {
    super(`projection is not rebuildable (no wipe): ${projection}`, { projection });
  }
}
