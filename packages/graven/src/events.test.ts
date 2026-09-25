import { describe, expect, it, assert } from "vitest";
import type { EventFamilyDefect } from "./errors.js";
import { InvalidEventFamilyError, InvalidEventPayloadError, UnknownEventRevisionError, UnknownEventTypeError } from "./errors.js";
import { defineEvent, defineEvents } from "./events.js";
import type { JsonValue } from "./json.js";
import type { StoredEvent } from "./types.js";
import { z } from "zod";

function stored(eventType: string, eventRevision: number, payload: JsonValue): StoredEvent {
  const at = new Date("2026-01-01T00:00:00.000Z");
  return {
    globalPosition: 1,
    streamId: "s1",
    streamVersion: 1,
    aggregateType: "widget",
    eventId: "e1",
    eventRevision,
    event: { eventType, payload, occurredAt: at },
    metadata: { correlationId: "c1" },
    recordedAt: at,
  };
}

function familyDefect(build: () => unknown): EventFamilyDefect | undefined {
  try {
    build();
  } catch (error) {
    if (error instanceof InvalidEventFamilyError) return error.context.defect;
    throw error;
  }
  return undefined;
}

describe("defineEvents", () => {
  it("parses an insertable at the latest revision", () => {
    const registry = defineEvents([
      defineEvent({
        eventType: "widget.created",
        latestRevision: 1,
        latest: z.object({ name: z.string() }),
      }),
    ]);

    const parsed = registry.parseInsertable({ eventType: "widget.created", payload: { name: "a" } });
    expect(parsed).toEqual({ eventRevision: 1, event: { eventType: "widget.created", payload: { name: "a" } } });
  });

  it("throws UnknownEventTypeError for an unregistered type", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
    ]);

    expect(() =>
      // @ts-expect-error unregistered event type is rejected at compile time too
      registry.parseInsertable({ eventType: "widget.deleted", payload: {} }),
    ).toThrow(UnknownEventTypeError);
  });

  it("throws InvalidEventPayloadError with issues on a bad payload", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({ name: z.string() }) }),
    ]);

    try {
      registry.parseStored(stored("widget.created", 1, { name: 1 }));
      expect.unreachable();
    } catch (error) {
      assert(error instanceof InvalidEventPayloadError);
      expect(error.context.issues).toBeInstanceOf(Array);
      expect(error.context.issues.length).toBeGreaterThan(0);
    }
  });

  it("upcasts an older revision through parseStored", () => {
    const v1 = z.object({ name: z.string() });
    const registry = defineEvents([
      defineEvent({
        eventType: "widget.created",
        latestRevision: 2,
        latest: z.object({ name: z.string(), sku: z.string() }),
        previous: [
          {
            revision: 1,
            parse: v1.transform((value) => ({ name: value.name, sku: "unknown" })),
          },
        ],
      }),
    ]);

    const event = registry.parseStored(stored("widget.created", 1, { name: "a" }));

    expect(event.payload).toEqual({ name: "a", sku: "unknown" });
  });

  it("throws UnknownEventRevisionError for a revision with no parser", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
    ]);

    expect(() => registry.parseStored(stored("widget.created", 5, {}))).toThrow(UnknownEventRevisionError);
  });

  it("throws InvalidEventFamilyError('duplicate-type') for a repeated event type", () => {
    expect(
      familyDefect(() =>
        defineEvents([
          defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
          defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
        ]),
      ),
    ).toBe("duplicate-type");
  });

  it("throws InvalidEventFamilyError('missing-upcaster') when a previous revision is missing from the chain", () => {
    expect(
      familyDefect(() =>
        defineEvents([
          defineEvent({
            eventType: "widget.created",
            latestRevision: 3,
            latest: z.object({}),
            previous: [{ revision: 1, parse: z.object({}) }],
          }),
        ]),
      ),
    ).toBe("missing-upcaster");
  });

  it("throws InvalidEventFamilyError('duplicate-revision') for a repeated revision within one family's previous chain", () => {
    expect(
      familyDefect(() =>
        defineEvents([
          defineEvent({
            eventType: "widget.created",
            latestRevision: 3,
            latest: z.object({}),
            previous: [
              { revision: 1, parse: z.object({}) },
              { revision: 1, parse: z.object({}) },
              { revision: 2, parse: z.object({}) },
            ],
          }),
        ]),
      ),
    ).toBe("duplicate-revision");
  });

  it("throws InvalidEventFamilyError('revision-out-of-range') for a previous revision at or above latestRevision", () => {
    // Every revision 1..latestRevision-1 is present and valid, isolating the range check
    // (entry.revision >= family.latestRevision) from the chain-completeness check.
    expect(
      familyDefect(() =>
        defineEvents([
          defineEvent({
            eventType: "widget.created",
            latestRevision: 3,
            latest: z.object({}),
            previous: [
              { revision: 1, parse: z.object({}) },
              { revision: 2, parse: z.object({}) },
              { revision: 3, parse: z.object({}) },
            ],
          }),
        ]),
      ),
    ).toBe("revision-out-of-range");
  });

  it("throws InvalidEventFamilyError('revision-out-of-range') for a previous revision below 1", () => {
    // Revision 1 is also present and valid, isolating the range check (entry.revision < 1)
    // from the chain-completeness check.
    expect(
      familyDefect(() =>
        defineEvents([
          defineEvent({
            eventType: "widget.created",
            latestRevision: 2,
            latest: z.object({}),
            previous: [
              { revision: 0, parse: z.object({}) },
              { revision: 1, parse: z.object({}) },
            ],
          }),
        ]),
      ),
    ).toBe("revision-out-of-range");
  });

  it("parseStored throws InvalidEventPayloadError when the stored payload fails the latest schema", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({ name: z.string() }) }),
    ]);

    expect(() => registry.parseStored(stored("widget.created", 1, { name: 123 }))).toThrow(InvalidEventPayloadError);
  });

  it("parseStored throws UnknownEventTypeError for a stored row of an unregistered type", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
    ]);
    expect(() => registry.parseStored(stored("widget.deleted", 1, {}))).toThrow(UnknownEventTypeError);
  });
});
