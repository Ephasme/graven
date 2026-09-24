export type EventMetadata = {
  readonly correlationId: string;
  readonly causationId?: string;
  readonly actor?: string;
};

export type BaseEvent = { readonly eventType: string; readonly payload: unknown };

export type TimedEvent<TEvent> = TEvent & { readonly occurredAt: Date };

export type StoredEvent<TEvent extends BaseEvent = BaseEvent> = {
  readonly globalPosition: number;
  readonly streamId: string;
  readonly streamVersion: number;
  readonly aggregateType: string;
  readonly eventId: string;
  readonly eventRevision: number;
  readonly event: TimedEvent<TEvent>;
  readonly metadata: EventMetadata;
  readonly recordedAt: Date;
};

export type InsertableEvent = { readonly eventType: string; readonly payload: unknown };

export type ExpectedVersion =
  | { readonly kind: "no-stream" }
  | { readonly kind: "exact"; readonly version: number };

/** One row as the store writes it: the runner has already validated and revision-stamped it. */
export type EventToAppend = {
  readonly eventId: string;
  readonly eventType: string;
  readonly eventRevision: number;
  readonly payload: unknown;
  readonly occurredAt: Date;
};

export type AppendRequest = {
  readonly streamId: string;
  readonly aggregateType: string;
  readonly expected: ExpectedVersion;
  readonly events: readonly EventToAppend[];
  readonly metadata: EventMetadata;
};

export type AppendResult = {
  readonly newVersion: number;
  readonly stored: readonly StoredEvent[];
};
