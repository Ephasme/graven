import type { z } from "zod";
import { DuplicateEventFamilyError, InvalidEventPayloadError, UnknownEventRevisionError, UnknownEventTypeError } from "./errors.js";
import type { JsonValue } from "./json.js";
import type { TimedEvent } from "./types.js";

export type Upcaster<P> = { readonly revision: number; readonly parse: z.ZodType<P, unknown> };

export type EventFamily<T extends string, S extends z.ZodType<JsonValue, unknown>> = {
  readonly eventType: T;
  readonly latestRevision: number;
  /** Payload schema: validates an insert, and parses a row stored at latestRevision. */
  readonly latest: S;
  /** One entry per older revision: parses that revision's stored payload into the latest shape. */
  readonly previous?: readonly Upcaster<z.output<S>>[];
};

/** Any family: the constraint every event-set parameter takes. */
export type AnyEventFamily = EventFamily<string, z.ZodType<JsonValue, unknown>>;

export type EventFamilies = readonly AnyEventFamily[];

/** Identity; exists so `eventType` infers as a literal and the schema's input type is kept. */
export function defineEvent<const T extends string, S extends z.ZodType<JsonValue, unknown>>(
  family: EventFamily<T, S>,
): EventFamily<T, S> {
  return family;
}

export type EventTypeOf<C extends EventFamilies> = C[number]["eventType"];

export type FamilyOf<C extends EventFamilies, K extends EventTypeOf<C>> = Extract<C[number], { readonly eventType: K }>;

type UpcasterSchema<F extends AnyEventFamily> = NonNullable<F["previous"]>[number]["parse"];

/** The upcaster half equals the latest half for any concrete C; it is spelled out so the generic
 * decoder typechecks. Gated on `previous` being a declared key: an inline family literal without
 * it would otherwise widen the union to `unknown`. */
export type PayloadOf<C extends EventFamilies, K extends EventTypeOf<C>> =
  | z.output<FamilyOf<C, K>["latest"]>
  | ("previous" extends keyof FamilyOf<C, K> ? z.output<UpcasterSchema<FamilyOf<C, K>>> : never);

export type InputOf<C extends EventFamilies, K extends EventTypeOf<C>> = z.input<FamilyOf<C, K>["latest"]>;

export type EventOf<C extends EventFamilies, K extends EventTypeOf<C> = EventTypeOf<C>> = {
  [P in K]: { readonly eventType: P; readonly payload: PayloadOf<C, P> };
}[K];

export type InsertableEventOf<C extends EventFamilies, K extends EventTypeOf<C> = EventTypeOf<C>> = {
  [P in K]: { readonly eventType: P; readonly payload: InputOf<C, P> };
}[K];

export type ParsedInsertable<C extends EventFamilies> = {
  readonly eventRevision: number;
  readonly event: EventOf<C>;
};

export type ParsedStoredRow = {
  readonly eventType: string;
  readonly eventRevision: number;
  readonly payload: JsonValue;
  readonly occurredAt: Date;
};

export interface EventRegistry<C extends EventFamilies> {
  readonly types: readonly EventTypeOf<C>[];
  latestRevision(eventType: EventTypeOf<C>): number;
  parseInsertable(input: InsertableEventOf<C>): ParsedInsertable<C>;
  parseStored(row: ParsedStoredRow): TimedEvent<EventOf<C>>;
}

function validateFamilies(families: EventFamilies): void {
  const seen = new Set<string>();
  for (const family of families) {
    if (seen.has(family.eventType)) {
      throw new DuplicateEventFamilyError(family.eventType, family.latestRevision);
    }
    seen.add(family.eventType);

    const previous = family.previous ?? [];
    const revisionsSeen = new Set<number>();
    for (const entry of previous) {
      if (revisionsSeen.has(entry.revision)) {
        throw new DuplicateEventFamilyError(family.eventType, entry.revision);
      }
      revisionsSeen.add(entry.revision);

      if (entry.revision < 1 || entry.revision >= family.latestRevision) {
        throw new DuplicateEventFamilyError(family.eventType, entry.revision);
      }
    }

    if (family.latestRevision > 1) {
      for (let revision = 1; revision < family.latestRevision; revision++) {
        if (!revisionsSeen.has(revision)) {
          throw new DuplicateEventFamilyError(family.eventType, revision);
        }
      }
    }
  }
}

function isFamilyOf<C extends EventFamilies, K extends EventTypeOf<C>>(
  family: C[number],
  eventType: K,
): family is FamilyOf<C, K> {
  return family.eventType === eventType;
}

function parsePayload<S extends z.ZodType>(
  schema: S,
  value: unknown,
  eventType: string,
  revision: number,
): z.output<S> {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new InvalidEventPayloadError(eventType, revision, result.error.issues);
  }
  return result.data;
}

export function defineEvents<const C extends EventFamilies>(families: C): EventRegistry<C> {
  validateFamilies(families);

  const byType = new Map<string, C[number]>(families.map((family) => [family.eventType, family]));

  function familyFor<K extends EventTypeOf<C>>(eventType: K): FamilyOf<C, K> {
    const family = byType.get(eventType);
    if (family === undefined || !isFamilyOf<C, K>(family, eventType)) {
      throw new UnknownEventTypeError(eventType);
    }
    return family;
  }

  const isEventType = (value: string): value is EventTypeOf<C> => byType.has(value);

  function decodeLatest<K extends EventTypeOf<C>>(input: InsertableEventOf<C, K>): ParsedInsertable<C> {
    const family = familyFor(input.eventType);
    const event: EventOf<C, K> = {
      eventType: input.eventType,
      payload: parsePayload<FamilyOf<C, K>["latest"]>(
        family.latest,
        input.payload,
        family.eventType,
        family.latestRevision,
      ),
    };
    return { eventRevision: family.latestRevision, event };
  }

  function decodeStored<K extends EventTypeOf<C>>(eventType: K, row: ParsedStoredRow): TimedEvent<EventOf<C, K>> {
    const family = familyFor(eventType);

    if (row.eventRevision === family.latestRevision) {
      const payload = parsePayload<FamilyOf<C, K>["latest"]>(
        family.latest,
        row.payload,
        family.eventType,
        family.latestRevision,
      );
      return { eventType, payload, occurredAt: row.occurredAt };
    }

    const previous: NonNullable<FamilyOf<C, K>["previous"]> = family.previous ?? [];
    const entry = previous.find((candidate) => candidate.revision === row.eventRevision);
    if (!entry) {
      throw new UnknownEventRevisionError(family.eventType, row.eventRevision);
    }
    const payload = parsePayload<UpcasterSchema<FamilyOf<C, K>>>(
      entry.parse,
      row.payload,
      family.eventType,
      row.eventRevision,
    );
    return { eventType, payload, occurredAt: row.occurredAt };
  }

  return {
    types: families.map((family) => family.eventType),

    latestRevision(eventType) {
      return familyFor(eventType).latestRevision;
    },

    parseInsertable(input) {
      return decodeLatest(input);
    },

    parseStored(row) {
      if (!isEventType(row.eventType)) {
        throw new UnknownEventTypeError(row.eventType);
      }
      return decodeStored(row.eventType, row);
    },
  };
}
