/**
 * Ported from sherpas-flock `packages/contract/src/events.ts` (assignment payload schemas)
 * and `apps/api/src/aggregates/assignment.ts` (the aggregate itself). Proves graven's core can
 * express Flock's assignment aggregate with no loss, including the no-op elision path.
 */
import { defineEvent } from "../../../src/events.js";
import { z } from "zod";

export const assignmentRequestedPayload = z.object({
  assignmentId: z.uuid(),
  profileId: z.uuid(),
  udid: z.string().min(1),
  sourceGroupId: z.uuid().nullable(),
  commandUuid: z.string().nullable(),
  error: z.string().nullable(),
});

export const assignmentRequested = defineEvent({
  eventType: "assignment.requested" as const,
  latestRevision: 1,
  latest: assignmentRequestedPayload,
});

export const assignmentInstallAcknowledged = defineEvent({
  eventType: "assignment.install_acknowledged" as const,
  latestRevision: 1,
  latest: z.object({ commandUuid: z.string().min(1) }),
});

export const assignmentInstallRejected = defineEvent({
  eventType: "assignment.install_rejected" as const,
  latestRevision: 1,
  latest: z.object({ commandUuid: z.string().min(1), status: z.string() }),
});

export const assignmentObservedPresent = defineEvent({
  eventType: "assignment.observed_present" as const,
  latestRevision: 1,
  latest: z.object({}),
});

export const assignmentObservedAbsent = defineEvent({
  eventType: "assignment.observed_absent" as const,
  latestRevision: 1,
  latest: z.object({}),
});

export const assignmentRemoved = defineEvent({
  eventType: "assignment.removed" as const,
  latestRevision: 1,
  latest: z.object({ groupId: z.uuid().nullable() }),
});

export const assignmentRevoked = defineEvent({
  eventType: "assignment.revoked" as const,
  latestRevision: 1,
  latest: z.object({}),
});

export const assignmentEvents = [
  assignmentRequested,
  assignmentInstallAcknowledged,
  assignmentInstallRejected,
  assignmentObservedPresent,
  assignmentObservedAbsent,
  assignmentRemoved,
  assignmentRevoked,
] as const;
