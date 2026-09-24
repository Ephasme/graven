import { defineAggregate } from "../../../src/aggregate.js";
import { walletEvents } from "./events.js";

export type WalletState = {
  readonly ownerId: string | null;
  readonly balanceMinutes: number;
  readonly provisionedMinutes: number;
} | null;

export const wallet = defineAggregate({
  type: "wallet",
  events: walletEvents,
  initialState: null,
  evolve: (state: WalletState, event): WalletState => {
    switch (event.eventType) {
      case "wallet.created":
        return { ownerId: event.payload.ownerId, balanceMinutes: 0, provisionedMinutes: 0 };
      case "wallet.provisioned":
        return {
          ownerId: state?.ownerId ?? null,
          balanceMinutes: (state?.balanceMinutes ?? 0) + event.payload.minutes,
          provisionedMinutes: (state?.provisionedMinutes ?? 0) + event.payload.minutes,
        };
      case "wallet.booked":
        return {
          ownerId: state?.ownerId ?? null,
          balanceMinutes: (state?.balanceMinutes ?? 0) - event.payload.minutes,
          provisionedMinutes: state?.provisionedMinutes ?? 0,
        };
      case "wallet.cancelled":
      case "wallet.finished":
        return state;
      default: {
        const exhaustive: never = event;
        throw new Error(`unhandled wallet event: ${JSON.stringify(exhaustive)}`);
      }
    }
  },
});
