import { describe, expect, it } from "vitest";
import { decide } from "../../../src/decide.js";
import { createInMemoryStore } from "../../../src/memory.js";
import {
  assignment,
  observeProfile,
  requestAssignment,
  resolveInstall,
  revokeAssignment,
  withdrawAssignment,
  type AssignmentState,
} from "./aggregate.js";
import { assignmentsProjection } from "./projection.js";

const PROFILE_ID = "11111111-1111-4111-8111-111111111111";
const GROUP_ID = "22222222-2222-4222-8222-222222222222";
const UDID = "device-1";
const ACKNOWLEDGED = "Acknowledged";

type Tables = { readonly assignments: AssignmentState };

function makeStore() {
  return createInMemoryStore<Tables>({ projections: [assignmentsProjection] });
}

const streamId = (profileId: string, udid: string) => `${profileId.toLowerCase()}:${udid}`;

describe("assignment aggregate (ported from sherpas-flock)", () => {
  it("requesting a fresh assignment starts pending and writes the projection", async () => {
    const store = makeStore();
    const stream = streamId(PROFILE_ID, UDID);

    const result = await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: null, commandUuid: "cmd-1", error: null }, () => "33333333-3333-4333-8333-333333333333"),
    );

    expect(result.appended).toBe(true);
    const row = store.writer.table("assignments").get(stream);
    expect(row?.status).toBe("pending");
  });

  it("a direct assignment survives a group's withdrawal", async () => {
    const store = makeStore();
    const stream = streamId(PROFILE_ID, UDID);

    await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: GROUP_ID, commandUuid: "cmd-1", error: null }, () => "44444444-4444-4444-8444-444444444444"),
    );
    // A human directly assigns the same binding: sourceGroupId becomes null and outranks the group forever.
    await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: null, commandUuid: "cmd-2", error: null }, () => "44444444-4444-4444-8444-444444444444"),
    );

    const withdrawResult = await decide(store, assignment, stream, (state) => withdrawAssignment(state, GROUP_ID));
    expect(withdrawResult).toEqual({ appended: false, reason: "refused" });

    const row = store.writer.table("assignments").get(stream);
    expect(row?.sourceGroupId).toBeNull();
    expect(row?.status).toBe("pending");
  });

  it("a group-sourced assignment is withdrawn by its own group", async () => {
    const store = makeStore();
    const stream = streamId(PROFILE_ID, UDID);

    await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: GROUP_ID, commandUuid: "cmd-1", error: null }, () => "44444444-4444-4444-8444-444444444444"),
    );

    const result = await decide(store, assignment, stream, (state) => withdrawAssignment(state, GROUP_ID));
    expect(result.appended).toBe(true);

    const row = store.writer.table("assignments").get(stream);
    expect(row?.status).toBe("removed");
  });

  it("install acknowledgment for the matching command moves pending to installed", async () => {
    const store = makeStore();
    const stream = streamId(PROFILE_ID, UDID);

    await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: null, commandUuid: "cmd-1", error: null }, () => "44444444-4444-4444-8444-444444444444"),
    );
    await decide(store, assignment, stream, () => resolveInstall("cmd-1", ACKNOWLEDGED, ACKNOWLEDGED));

    const row = store.writer.table("assignments").get(stream);
    expect(row?.status).toBe("installed");
  });

  it("observed_present is a no-op once already installed (elideNoOps drops it)", async () => {
    const store = makeStore();
    const stream = streamId(PROFILE_ID, UDID);

    await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: null, commandUuid: "cmd-1", error: null }, () => "44444444-4444-4444-8444-444444444444"),
    );
    await decide(store, assignment, stream, () => resolveInstall("cmd-1", ACKNOWLEDGED, ACKNOWLEDGED));
    const positionAfterInstall = await store.currentPosition();

    // A ProfileList check-in observes the profile present on an already-installed binding:
    // evolve returns the same reference, so elideNoOps must drop this fact from the log.
    const result = await decide(store, assignment, stream, () => observeProfile(true));

    expect(result).toEqual({ appended: false, reason: "no-op" });
    expect(await store.currentPosition()).toBe(positionAfterInstall);
  });

  it("revoking then re-requesting starts a fresh assignment id", async () => {
    const store = makeStore();
    const stream = streamId(PROFILE_ID, UDID);

    await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: null, commandUuid: "cmd-1", error: null }, () => "44444444-4444-4444-8444-444444444444"),
    );
    await decide(store, assignment, stream, (state) => revokeAssignment(state));

    // After revocation, the projection has no live row for this stream.
    expect(store.writer.table("assignments").has(stream)).toBe(false);

    const result = await decide(store, assignment, stream, (state) =>
      requestAssignment(state, { profileId: PROFILE_ID, udid: UDID, sourceGroupId: null, commandUuid: "cmd-2", error: null }, () => "55555555-5555-4555-8555-555555555555"),
    );
    expect(result.appended).toBe(true);
    const row = store.writer.table("assignments").get(stream);
    expect(row?.assignmentId).toBe("55555555-5555-4555-8555-555555555555");
  });
});
