import { z } from "zod";
import { InvalidEventFamilyError, InvalidEventPayloadError, UnknownEventRevisionError, UnknownEventTypeError } from "./errors.js";
import type { JsonValue } from "./json.js";
import type { StoredEvent, TimedEvent } from "./types.js";

/** A schema bound on the core `$ZodType`, not classic `z.ZodType`: libraries augment the classic
 * interface (zod-to-openapi adds `openapi(metadata: …Input…): this`), and checking a concrete
 * schema against the augmented interface fails with TS2589. */
export type SchemaOf<O> = z.core.$ZodType<O, unknown>;

export type Upcaster<P> = { readonly revision: number; readonly parse: SchemaOf<P> };

export type EventFamily<T extends string, S extends SchemaOf<JsonValue>> = {
  readonly eventType: T;
  readonly latestRevision: number;
  /** Payload schema: validates an insert, and parses a row stored at latestRevision. */
  readonly latest: S;
  /** One entry per older revision: parses that revision's stored payload into the latest shape. */
  readonly previous?: readonly Upcaster<z.output<S>>[];
};

/** Any family: the constraint every event-set parameter takes. */
export type AnyEventFamily = EventFamily<string, SchemaOf<JsonValue>>;

export type EventFamilies = readonly AnyEventFamily[];

/** Identity; exists so `eventType` infers as a literal and the schema's input type is kept. */
export function defineEvent<const T extends string, S extends SchemaOf<JsonValue>>(
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

export interface EventRegistry<C extends EventFamilies> {
  parseInsertable(input: InsertableEventOf<C>): ParsedInsertable<C>;
  /** Decodes a stored row; an older revision is upcast to the latest payload shape. */
  parseStored(stored: StoredEvent): TimedEvent<EventOf<C>>;
}

function validateFamilies(families: EventFamilies): void {
  const seen = new Set<string>();
  for (const family of families) {
    if (seen.has(family.eventType)) {
      throw new InvalidEventFamilyError(family.eventType, family.latestRevision, "duplicate-type");
    }
    seen.add(family.eventType);

    const previous = family.previous ?? [];
    const revisionsSeen = new Set<number>();
    for (const entry of previous) {
      if (revisionsSeen.has(entry.revision)) {
        throw new InvalidEventFamilyError(family.eventType, entry.revision, "duplicate-revision");
      }
      revisionsSeen.add(entry.revision);

      if (entry.revision < 1 || entry.revision >= family.latestRevision) {
        throw new InvalidEventFamilyError(family.eventType, entry.revision, "revision-out-of-range");
      }
    }

    for (let revision = 1; revision < family.latestRevision; revision++) {
      if (!revisionsSeen.has(revision)) {
        throw new InvalidEventFamilyError(family.eventType, revision, "missing-upcaster");
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

function parsePayload<S extends z.core.$ZodType>(
  schema: S,
  value: unknown,
  eventType: string,
  revision: number,
): z.output<S> {
  const result = z.safeParse(schema, value);
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

  function decodeStored<K extends EventTypeOf<C>>(eventType: K, stored: StoredEvent): TimedEvent<EventOf<C, K>> {
    const family = familyFor(eventType);

    if (stored.eventRevision === family.latestRevision) {
      const payload = parsePayload<FamilyOf<C, K>["latest"]>(
        family.latest,
        stored.event.payload,
        family.eventType,
        family.latestRevision,
      );
      return { eventType, payload, occurredAt: stored.event.occurredAt };
    }

    const previous: NonNullable<FamilyOf<C, K>["previous"]> = family.previous ?? [];
    const entry = previous.find((candidate) => candidate.revision === stored.eventRevision);
    if (!entry) {
      throw new UnknownEventRevisionError(family.eventType, stored.eventRevision);
    }
    const payload = parsePayload<UpcasterSchema<FamilyOf<C, K>>>(
      entry.parse,
      stored.event.payload,
      family.eventType,
      stored.eventRevision,
    );
    return { eventType, payload, occurredAt: stored.event.occurredAt };
  }

  return {
    parseInsertable(input) {
      return decodeLatest(input);
    },

    parseStored(stored) {
      if (!isEventType(stored.event.eventType)) {
        throw new UnknownEventTypeError(stored.event.eventType);
      }
      return decodeStored(stored.event.eventType, stored);
    },
  };
}
