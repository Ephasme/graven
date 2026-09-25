/**
 * Ported from sherpas-flock `apps/api/src/aggregates/assignment.ts`. `evolve` is unchanged,
 * including the group-vs-direct precedence and the `event.occurredAt < state.requestedAt`
 * guards. Decision functions are ported verbatim, adapted only to graven's `Proposal` shape.
 */
import { defineAggregate } from "../../../src/aggregate.js";
import type { InsertableEventOf } from "../../../src/events.js";
import type { Proposal } from "../../../src/decide.js";
import { assignmentEvents } from "./events.js";

type AssignmentStatus = "pending" | "installed" | "removed" | "failed";

export interface AssignmentState {
  assignmentId: string;
  profileId: string;
  udid: string;
  sourceGroupId: string | null;
  commandUuid: string | null;
  error: string | null;
  status: AssignmentStatus;
  /** Latest `assignment.requested` event time. The binding's "assigned no earlier than". */
  requestedAt: Date;
  revoked: boolean;
}

type AssignmentEmit = InsertableEventOf<typeof assignmentEvents>;

export const assignment = defineAggregate({
  type: "assignment",
  events: assignmentEvents,
  initialState: null,
  elideNoOps: true,
  evolve: (state: AssignmentState | null, event): AssignmentState | null => {
    const fresh = state === null || state.revoked;

    if (event.eventType === "assignment.requested") {
      return fresh
        ? {
            assignmentId: event.payload.assignmentId,
            profileId: event.payload.profileId,
            udid: event.payload.udid,
            sourceGroupId: event.payload.sourceGroupId,
            commandUuid: event.payload.commandUuid,
            error: event.payload.error,
            status: event.payload.error !== null ? "failed" : "pending",
            requestedAt: event.occurredAt,
            revoked: false,
          }
        : {
            assignmentId: state.assignmentId,
            profileId: event.payload.profileId,
            udid: event.payload.udid,
            // NULL is "direct" and outranks every group: a direct assignment always wins,
            // and a group write never downgrades an already-null row.
            sourceGroupId: state.sourceGroupId === null ? null : event.payload.sourceGroupId,
            commandUuid: event.payload.commandUuid,
            error: event.payload.error,
            status: event.payload.error !== null ? "failed" : "pending",
            requestedAt: event.occurredAt,
            revoked: false,
          };
    }

    // Every other fact needs a live, non-revoked binding to apply to.
    if (fresh) return state;
    const current = state;

    switch (event.eventType) {
      case "assignment.install_acknowledged": {
        if (current.status !== "pending" || event.payload.commandUuid !== current.commandUuid) return state;
        return { ...current, status: "installed", error: null };
      }
      case "assignment.install_rejected": {
        if (event.payload.commandUuid !== current.commandUuid) return state;
        // `installed` is admitted so the command Flock sent can correct a `ProfileList`
        // that promoted the binding ahead of the command's own outcome.
        if (current.status !== "pending" && current.status !== "installed") return state;
        return { ...current, status: "failed", error: event.payload.status };
      }
      case "assignment.observed_present": {
        // `ProfileList` names identifiers, never versions: presence proves nothing about a
        // re-push that failed, so only a `pending` binding heals.
        if (current.status !== "pending") return state;
        if (event.occurredAt < current.requestedAt) return state;
        return { ...current, status: "installed", error: null };
      }
      case "assignment.observed_absent": {
        if (current.status !== "installed") return state;
        if (event.occurredAt < current.requestedAt) return state;
        return { ...current, status: "removed" };
      }
      case "assignment.removed": {
        if (current.status === "removed") return state;
        // Re-checks `withdrawAssignment`'s own precondition: that decision reads state
        // outside this event's append, so a direct `requested` can land between the two.
        if (event.payload.groupId !== null && event.payload.groupId !== current.sourceGroupId) return state;
        return { ...current, status: "removed" };
      }
      case "assignment.revoked": {
        return { ...current, revoked: true };
      }
      default: {
        const exhaustive: never = event;
        throw new Error(`unhandled domain event: ${JSON.stringify(exhaustive)}`);
      }
    }
  },
});

type RequestAssignmentInput = {
  readonly profileId: string;
  readonly udid: string;
  readonly sourceGroupId: string | null;
  readonly commandUuid: string | null;
  readonly error: string | null;
};

export function requestAssignment(
  state: AssignmentState | null,
  input: RequestAssignmentInput,
  newId: () => string,
): Proposal<typeof assignmentEvents, null> {
  const fresh = state === null || state.revoked;
  const assignmentId = fresh ? newId() : state.assignmentId;
  return {
    emit: [
      {
        eventType: "assignment.requested",
        payload: {
          assignmentId,
          profileId: input.profileId.toLowerCase(),
          udid: input.udid,
          sourceGroupId: input.sourceGroupId?.toLowerCase() ?? null,
          commandUuid: input.commandUuid,
          error: input.error,
        },
      } satisfies AssignmentEmit,
    ],
    result: null,
  };
}

export function resolveInstall(
  commandUuid: string,
  status: string,
  acknowledgedStatus: string,
): Proposal<typeof assignmentEvents, null> {
  return status === acknowledgedStatus
    ? { emit: [{ eventType: "assignment.install_acknowledged", payload: { commandUuid } } satisfies AssignmentEmit], result: null }
    : { emit: [{ eventType: "assignment.install_rejected", payload: { commandUuid, status } } satisfies AssignmentEmit], result: null };
}

export function observeProfile(present: boolean): Proposal<typeof assignmentEvents, null> {
  return present
    ? { emit: [{ eventType: "assignment.observed_present", payload: {} } satisfies AssignmentEmit], result: null }
    : { emit: [{ eventType: "assignment.observed_absent", payload: {} } satisfies AssignmentEmit], result: null };
}

/** A directly assigned row survives a group's withdrawal: a human asked for it independently. */
export function withdrawAssignment(
  state: AssignmentState | null,
  groupId: string,
): Proposal<typeof assignmentEvents, null> | null {
  const group = groupId.toLowerCase();
  if (state === null || state.sourceGroupId !== group) return null;
  return { emit: [{ eventType: "assignment.removed", payload: { groupId: group } } satisfies AssignmentEmit], result: null };
}

export function revokeAssignment(state: AssignmentState | null): Proposal<typeof assignmentEvents, null> | null {
  if (state === null || state.revoked) return null;
  return { emit: [{ eventType: "assignment.revoked", payload: {} } satisfies AssignmentEmit], result: null };
}
