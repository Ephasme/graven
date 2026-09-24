import { describe, expect, it } from "vitest";
import { resolveTableNames } from "./tables.js";

describe("resolveTableNames", () => {
  it("returns the defaults with no overrides", () => {
    expect(resolveTableNames()).toEqual({
      events: "graven_events",
      streams: "graven_streams",
      positions: "graven_positions",
      checkpoints: "graven_checkpoints",
      locks: "graven_locks",
    });
  });

  it("applies a partial override", () => {
    expect(resolveTableNames({ events: "custom_events" }).events).toBe("custom_events");
  });

  it("throws RangeError for an invalid identifier", () => {
    expect(() => resolveTableNames({ events: "bad; drop table" })).toThrow(RangeError);
    expect(() => resolveTableNames({ events: "1leading_digit" })).toThrow(RangeError);
  });
});
