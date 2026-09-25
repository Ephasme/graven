import { assert, describe, expect, it } from "vitest";
import { z } from "zod";
import {
  ConcurrentModificationError,
  DuplicateEventIdError,
  DuplicateProjectorError,
  GravenError,
  InvalidEventFamilyError,
  InvalidEventPayloadError,
  ProjectionNotRebuildableError,
  StaleStreamVersionError,
  StreamVersionConflictError,
  UnknownEventRevisionError,
  UnknownEventTypeError,
} from "./errors.js";

describe("GravenError subclasses", () => {
  it("UnknownEventTypeError carries code and eventType context", () => {
    const error = new UnknownEventTypeError("widget.created");
    expect(error).toBeInstanceOf(GravenError);
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("UnknownEventTypeError");
    expect(error.code).toBe("GRAVEN_UNKNOWN_EVENT_TYPE");
    expect(error.context).toEqual({ eventType: "widget.created" });
    expect(error.message).toContain("widget.created");
  });

  it("UnknownEventRevisionError carries eventType and eventRevision context", () => {
    const error = new UnknownEventRevisionError("widget.created", 3);
    expect(error.code).toBe("GRAVEN_UNKNOWN_EVENT_REVISION");
    expect(error.context).toEqual({ eventType: "widget.created", eventRevision: 3 });
  });

  it("InvalidEventPayloadError carries eventType, revision, and the raw issues array", () => {
    const result = z.object({ name: z.string() }).safeParse({});
    assert(!result.success);
    const issues = result.error.issues;
    const error = new InvalidEventPayloadError("widget.created", 1, issues);
    expect(error.code).toBe("GRAVEN_INVALID_EVENT_PAYLOAD");
    expect(error.context.issues).toBe(issues);
  });

  it("InvalidEventFamilyError carries eventType, revision, and defect context", () => {
    const error = new InvalidEventFamilyError("widget.created", 2, "missing-upcaster");
    expect(error.code).toBe("GRAVEN_INVALID_EVENT_FAMILY");
    expect(error.context).toEqual({ eventType: "widget.created", revision: 2, defect: "missing-upcaster" });
  });

  it("DuplicateProjectorError carries the projector name", () => {
    const error = new DuplicateProjectorError("widgets");
    expect(error.code).toBe("GRAVEN_DUPLICATE_PROJECTOR");
    expect(error.context).toEqual({ name: "widgets" });
  });

  it("StaleStreamVersionError carries streamId, expected, and actual", () => {
    const error = new StaleStreamVersionError("s1", 2, 5);
    expect(error.code).toBe("GRAVEN_STALE_STREAM_VERSION");
    expect(error.context).toEqual({ streamId: "s1", expected: 2, actual: 5 });
    expect(error.message).toContain("s1");
  });

  it("StreamVersionConflictError carries streamId and expected", () => {
    const error = new StreamVersionConflictError("s1", 3);
    expect(error.code).toBe("GRAVEN_STREAM_VERSION_CONFLICT");
    expect(error.context).toEqual({ streamId: "s1", expected: 3 });
    expect(error.message).toContain("s1");
  });

  it("DuplicateEventIdError carries streamId and eventId", () => {
    const error = new DuplicateEventIdError("s1", "e1");
    expect(error.code).toBe("GRAVEN_DUPLICATE_EVENT_ID");
    expect(error.context).toEqual({ streamId: "s1", eventId: "e1" });
  });

  it("ConcurrentModificationError carries streamId and attempts", () => {
    const error = new ConcurrentModificationError("s1", 3);
    expect(error.code).toBe("GRAVEN_CONCURRENT_MODIFICATION");
    expect(error.context).toEqual({ streamId: "s1", attempts: 3 });
    expect(error.message).toContain("3 attempts");
  });

  it("ProjectionNotRebuildableError carries the projection name", () => {
    const error = new ProjectionNotRebuildableError("widgets");
    expect(error.code).toBe("GRAVEN_PROJECTION_NOT_REBUILDABLE");
    expect(error.context).toEqual({ projection: "widgets" });
  });

  it("defaults context to an empty object when the subclass omits it", () => {
    class NoContextError extends GravenError {
      readonly code = "TEST_NO_CONTEXT";
      constructor() {
        super("no context here");
      }
    }
    const error = new NoContextError();
    expect(error.context).toEqual({});
  });
});
