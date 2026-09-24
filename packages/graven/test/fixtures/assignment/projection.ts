import type { InMemoryWriter } from "../../../src/memory.js";
import { defineProjection } from "../../../src/projection.js";
import { assignment, type AssignmentState } from "./aggregate.js";
import type { assignmentEvents } from "./events.js";

export const assignmentsProjection = defineProjection<AssignmentState | null, typeof assignmentEvents, InMemoryWriter>({
  name: "assignments",
  aggregate: assignment,
  apply: async (ctx) => {
    const table = ctx.writer.tables.get("assignments") ?? new Map<string, unknown>();
    if (ctx.state === null || ctx.state.revoked) {
      table.delete(ctx.streamId);
    } else {
      table.set(ctx.streamId, ctx.state);
    }
    ctx.writer.tables.set("assignments", table);
  },
  wipe: async (writer) => {
    writer.tables.set("assignments", new Map());
  },
});
