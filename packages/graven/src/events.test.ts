import { describe, expect, it, assert } from "vitest";
import { DuplicateEventFamilyError, InvalidEventPayloadError, UnknownEventRevisionError, UnknownEventTypeError } from "./errors.js";
import { defineEvent, defineEvents } from "./events.js";
import { z } from "zod";

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
      registry.parseStored({ eventType: "widget.created", eventRevision: 1, payload: { name: 1 }, occurredAt: new Date() });
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

    const event = registry.parseStored({
      eventType: "widget.created",
      eventRevision: 1,
      payload: { name: "a" },
      occurredAt: new Date("2026-01-01T00:00:00.000Z"),
    });

    expect(event.payload).toEqual({ name: "a", sku: "unknown" });
  });

  it("throws UnknownEventRevisionError for a revision with no parser", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
    ]);

    expect(() =>
      registry.parseStored({
        eventType: "widget.created",
        eventRevision: 5,
        payload: {},
        occurredAt: new Date(),
      }),
    ).toThrow(UnknownEventRevisionError);
  });

  it("throws DuplicateEventFamilyError for a repeated event type", () => {
    expect(() =>
      defineEvents([
        defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
        defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
      ]),
    ).toThrow(DuplicateEventFamilyError);
  });

  it("throws DuplicateEventFamilyError when a previous revision is missing from the chain", () => {
    expect(() =>
      defineEvents([
        defineEvent({
          eventType: "widget.created",
          latestRevision: 3,
          latest: z.object({}),
          previous: [{ revision: 1, parse: z.object({}) }],
        }),
      ]),
    ).toThrow(DuplicateEventFamilyError);
  });

  it("throws DuplicateEventFamilyError for a repeated revision within one family's previous chain", () => {
    expect(() =>
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
    ).toThrow(DuplicateEventFamilyError);
  });

  it("throws DuplicateEventFamilyError for a previous revision at or above latestRevision", () => {
    // Every revision 1..latestRevision-1 is present and valid, isolating the range check
    // (entry.revision >= family.latestRevision) from the chain-completeness check.
    expect(() =>
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
    ).toThrow(DuplicateEventFamilyError);
  });

  it("throws DuplicateEventFamilyError for a previous revision below 1", () => {
    // Revision 1 is also present and valid, isolating the range check (entry.revision < 1)
    // from the chain-completeness check.
    expect(() =>
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
    ).toThrow(DuplicateEventFamilyError);
  });

  it("registry.latestRevision returns the family's latest revision", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
      defineEvent({
        eventType: "widget.renamed",
        latestRevision: 2,
        latest: z.object({ name: z.string() }),
        previous: [{ revision: 1, parse: z.object({ oldName: z.string() }).transform((v) => ({ name: v.oldName })) }],
      }),
    ]);

    expect(registry.latestRevision("widget.created")).toBe(1);
    expect(registry.latestRevision("widget.renamed")).toBe(2);
  });

  it("registry.latestRevision throws UnknownEventTypeError for an unregistered type", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
    ]);
    // @ts-expect-error unregistered event type is rejected at compile time too
    expect(() => registry.latestRevision("widget.deleted")).toThrow(UnknownEventTypeError);
  });

  it("parseStored throws InvalidEventPayloadError when the stored payload fails the latest schema", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({ name: z.string() }) }),
    ]);

    expect(() =>
      registry.parseStored({
        eventType: "widget.created",
        eventRevision: 1,
        payload: { name: 123 },
        occurredAt: new Date(),
      }),
    ).toThrow(InvalidEventPayloadError);
  });

  it("parseStored throws UnknownEventTypeError for a stored row of an unregistered type", () => {
    const registry = defineEvents([
      defineEvent({ eventType: "widget.created", latestRevision: 1, latest: z.object({}) }),
    ]);
    expect(() =>
      registry.parseStored({ eventType: "widget.deleted", eventRevision: 1, payload: {}, occurredAt: new Date() }),
    ).toThrow(UnknownEventTypeError);
  });
});
