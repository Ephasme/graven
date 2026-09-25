import type { z } from "zod";

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
  declare readonly context: { readonly eventType: string };

  constructor(eventType: string) {
    super(`unknown event type: ${eventType}`, { eventType });
  }
}

export class UnknownEventRevisionError extends GravenError {
  readonly code = "GRAVEN_UNKNOWN_EVENT_REVISION";
  declare readonly context: { readonly eventType: string; readonly eventRevision: number };

  constructor(eventType: string, eventRevision: number) {
    super(`unknown revision ${eventRevision} for event type: ${eventType}`, {
      eventType,
      eventRevision,
    });
  }
}

export class InvalidEventPayloadError extends GravenError {
  readonly code = "GRAVEN_INVALID_EVENT_PAYLOAD";
  declare readonly context: {
    readonly eventType: string;
    readonly eventRevision: number;
    readonly issues: readonly z.core.$ZodIssue[];
  };

  constructor(eventType: string, eventRevision: number, issues: readonly z.core.$ZodIssue[]) {
    super(`invalid payload for event type: ${eventType} (revision ${eventRevision})`, {
      eventType,
      eventRevision,
      issues,
    });
  }
}

export type EventFamilyDefect = "duplicate-type" | "duplicate-revision" | "revision-out-of-range" | "missing-upcaster";

export class InvalidEventFamilyError extends GravenError {
  readonly code = "GRAVEN_INVALID_EVENT_FAMILY";
  declare readonly context: { readonly eventType: string; readonly revision: number; readonly defect: EventFamilyDefect };

  constructor(eventType: string, revision: number, defect: EventFamilyDefect) {
    super(`invalid event family ${eventType}: ${defect} (revision ${revision})`, { eventType, revision, defect });
  }
}

export class DuplicateProjectorError extends GravenError {
  readonly code = "GRAVEN_DUPLICATE_PROJECTOR";
  declare readonly context: { readonly name: string };

  constructor(name: string) {
    super(`duplicate projector: ${name}`, { name });
  }
}

export class StaleStreamVersionError extends GravenError {
  readonly code = "GRAVEN_STALE_STREAM_VERSION";
  declare readonly context: { readonly streamId: string; readonly expected: number; readonly actual: number };

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
  declare readonly context: { readonly streamId: string; readonly expected: number };

  constructor(streamId: string, expected: number) {
    super(`stream version conflict for ${streamId}: expected ${expected}`, {
      streamId,
      expected,
    });
  }
}

export class DuplicateEventIdError extends GravenError {
  readonly code = "GRAVEN_DUPLICATE_EVENT_ID";
  declare readonly context: { readonly streamId: string; readonly eventId: string };

  constructor(streamId: string, eventId: string) {
    super(`duplicate event id for stream ${streamId}: ${eventId}`, { streamId, eventId });
  }
}

export class ConcurrentModificationError extends GravenError {
  readonly code = "GRAVEN_CONCURRENT_MODIFICATION";
  declare readonly context: { readonly streamId: string; readonly attempts: number };

  constructor(streamId: string, attempts: number) {
    super(`concurrent modification of stream ${streamId} after ${attempts} attempts`, {
      streamId,
      attempts,
    });
  }
}

export class ProjectionNotRebuildableError extends GravenError {
  readonly code = "GRAVEN_PROJECTION_NOT_REBUILDABLE";
  declare readonly context: { readonly projection: string };

  constructor(projection: string) {
    super(`projection is not rebuildable (no wipe): ${projection}`, { projection });
  }
}

export class InvalidStoredRowError extends GravenError {
  readonly code = "GRAVEN_INVALID_STORED_ROW";
  declare readonly context: { readonly table: string; readonly issues: readonly z.core.$ZodIssue[] };

  constructor(table: string, issues: readonly z.core.$ZodIssue[]) {
    super(`invalid row in ${table}`, { table, issues });
  }
}

export class ProjectionAggregateMismatchError extends GravenError {
  readonly code = "GRAVEN_PROJECTION_AGGREGATE_MISMATCH";
  declare readonly context: { readonly projection: string; readonly aggregateType: string };

  constructor(projection: string, aggregateType: string) {
    super(`projection ${projection} is bound to another ${aggregateType} aggregate object`, {
      projection,
      aggregateType,
    });
  }
}
