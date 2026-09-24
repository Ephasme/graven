import type { InMemoryWriter } from "../../../src/memory.js";
import { defineProjection } from "../../../src/projection.js";
import { assignment, type AssignmentState } from "./aggregate.js";
import type { assignmentEvents } from "./events.js";

type Tables = { readonly assignments: AssignmentState };

export const assignmentsProjection = defineProjection<
  AssignmentState | null,
  typeof assignmentEvents,
  InMemoryWriter<Tables>
>({
  name: "assignments",
  aggregate: assignment,
  apply: async (ctx) => {
    const table = ctx.writer.table("assignments");
    if (ctx.state === null || ctx.state.revoked) {
      table.delete(ctx.streamId);
    } else {
      table.set(ctx.streamId, ctx.state);
    }
  },
  wipe: async (writer) => {
    writer.table("assignments").clear();
  },
});
