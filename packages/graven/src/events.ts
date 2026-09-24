import type { z } from "zod";
import { DuplicateEventFamilyError, InvalidEventPayloadError, UnknownEventRevisionError, UnknownEventTypeError } from "./errors.js";
import type { InsertableEvent, TimedEvent } from "./types.js";

export type EventFamily<TType extends string, TPayload, TInput = TPayload> = {
  readonly eventType: TType;
  readonly latestRevision: number;
  /** Payload schema: validates an insert, and parses a row stored at latestRevision. */
  readonly latest: z.ZodType<TPayload, TInput>;
  /** One entry per older revision: parses that revision's stored payload and transforms it to the latest shape. */
  readonly previous?: readonly {
    readonly revision: number;
    readonly parse: z.ZodType<TPayload>;
  }[];
};

/** Identity; exists so `eventType` infers as a literal and the schema's input type is kept. */
export function defineEvent<const TType extends string, TPayload, TInput>(
  family: EventFamily<TType, TPayload, TInput>,
): EventFamily<TType, TPayload, TInput> {
  return family;
}

/** Any family: the constraint every event-set parameter takes. */
export type AnyEventFamily = EventFamily<string, unknown, unknown>;

export type EventOf<C extends readonly AnyEventFamily[]> = {
  [F in C[number] as F["eventType"]]: {
    readonly eventType: F["eventType"];
    readonly payload: F extends EventFamily<string, infer P, unknown> ? P : never;
  };
}[C[number]["eventType"]];

export type InsertableEventOf<C extends readonly AnyEventFamily[]> = {
  [F in C[number] as F["eventType"]]: {
    readonly eventType: F["eventType"];
    readonly payload: F extends EventFamily<string, unknown, infer I> ? I : never;
  };
}[C[number]["eventType"]];

export type ParsedInsertable = {
  readonly eventType: string;
  readonly eventRevision: number;
  readonly payload: unknown;
};

export type ParsedStoredRow = {
  readonly eventType: string;
  readonly eventRevision: number;
  readonly payload: unknown;
  readonly occurredAt: Date;
};

export interface EventRegistry<C extends readonly AnyEventFamily[]> {
  readonly types: readonly string[];
  latestRevision(eventType: string): number;
  parseInsertable(input: InsertableEvent): ParsedInsertable;
  parseStored(row: ParsedStoredRow): TimedEvent<EventOf<C>>;
}

function validateFamilies(families: readonly AnyEventFamily[]): void {
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

export function defineEvents<const C extends readonly AnyEventFamily[]>(
  families: C,
): EventRegistry<C> {
  validateFamilies(families);

  const byType = new Map<string, AnyEventFamily>();
  for (const family of families) {
    byType.set(family.eventType, family);
  }

  const types = families.map((f) => f.eventType);

  function getFamily(eventType: string): AnyEventFamily {
    const family = byType.get(eventType);
    if (!family) {
      throw new UnknownEventTypeError(eventType);
    }
    return family;
  }

  return {
    types,

    latestRevision(eventType) {
      return getFamily(eventType).latestRevision;
    },

    parseInsertable(input) {
      const family = getFamily(input.eventType);
      const result = family.latest.safeParse(input.payload);
      if (!result.success) {
        throw new InvalidEventPayloadError(family.eventType, family.latestRevision, result.error.issues);
      }
      return {
        eventType: family.eventType,
        eventRevision: family.latestRevision,
        payload: result.data,
      };
    },

    parseStored(row) {
      const family = getFamily(row.eventType);

      let schema: z.ZodType<unknown>;
      if (row.eventRevision === family.latestRevision) {
        schema = family.latest;
      } else {
        const entry = (family.previous ?? []).find((p) => p.revision === row.eventRevision);
        if (!entry) {
          throw new UnknownEventRevisionError(family.eventType, row.eventRevision);
        }
        schema = entry.parse;
      }

      const result = schema.safeParse(row.payload);
      if (!result.success) {
        throw new InvalidEventPayloadError(family.eventType, row.eventRevision, result.error.issues);
      }

      return {
        eventType: family.eventType,
        payload: result.data,
        occurredAt: row.occurredAt,
      } as unknown as TimedEvent<EventOf<C>>;
    },
  };
}
