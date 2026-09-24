/**
 * A condensed port of sherpas-api's MinutesWallet, exercising `decide` with a synthetic clock
 * and the upcast path: `wallet.provisioned` is declared at revision 2 with a `previous` entry
 * that transforms revision 1's stored payload to the latest shape.
 */
import { defineEvent } from "../../../src/events.js";
import { z } from "zod";

export const walletCreated = defineEvent({
  eventType: "wallet.created" as const,
  latestRevision: 1,
  latest: z.object({ ownerId: z.string() }),
});

export const walletBooked = defineEvent({
  eventType: "wallet.booked" as const,
  latestRevision: 1,
  latest: z.object({ minutes: z.number().positive() }),
});

export const walletCancelled = defineEvent({
  eventType: "wallet.cancelled" as const,
  latestRevision: 1,
  latest: z.object({ minutes: z.number().positive() }),
});

export const walletFinished = defineEvent({
  eventType: "wallet.finished" as const,
  latestRevision: 1,
  latest: z.object({ minutes: z.number().positive() }),
});

const walletProvisionedV1 = z.object({ minutes: z.number() });
const walletProvisionedLatest = z.object({ minutes: z.number(), source: z.string() });

export const walletProvisioned = defineEvent({
  eventType: "wallet.provisioned" as const,
  latestRevision: 2,
  latest: walletProvisionedLatest,
  previous: [
    {
      revision: 1,
      parse: walletProvisionedV1.transform((value) => ({ minutes: value.minutes, source: "legacy" })),
    },
  ],
});

export const walletEvents = [
  walletCreated,
  walletBooked,
  walletCancelled,
  walletFinished,
  walletProvisioned,
] as const;
