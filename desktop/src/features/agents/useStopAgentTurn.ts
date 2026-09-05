import * as React from "react";
import { toast } from "sonner";

import { awaitCancelTurnOutcome } from "./lib/cancelTurnOutcome";
import {
  ensureRelayObserverSubscription,
  subscribeControlResults,
} from "./observerRelayStore";
import { cancelManagedAgentTurn } from "@/shared/api/agentControl";

/** Shared Activity and hover-card stop flow; relay delivery is not completion. */
export function useStopAgentTurn(agent: { pubkey: string; name: string }) {
  const pendingRef = React.useRef(false);
  const [pendingChannelId, setPendingChannelId] = React.useState<string | null>(
    null,
  );

  React.useEffect(() => {
    void ensureRelayObserverSubscription();
  }, []);

  async function stopTurn(channelId: string | null) {
    if (!channelId || pendingRef.current) return;
    pendingRef.current = true;
    setPendingChannelId(channelId);
    try {
      const requestId = crypto.randomUUID();
      const outcome = await awaitCancelTurnOutcome({
        requestId,
        channelId,
        subscribe: (listener) =>
          subscribeControlResults(agent.pubkey, listener),
        sendCancel: () =>
          cancelManagedAgentTurn(agent.pubkey, channelId, requestId),
        scheduleTimeout: (onTimeout) => {
          const timeout = window.setTimeout(onTimeout, 8_000);
          return () => window.clearTimeout(timeout);
        },
      });
      if (outcome === "ambiguous_target") {
        toast.error(
          "This channel has multiple agent sessions. Stopping a specific thread isn't available here yet.",
        );
      } else if (outcome === "no_active_turn") {
        toast.info("No active turn to stop.");
      } else if (outcome === "unconfirmed") {
        toast.info("Stop requested, but the agent hasn't confirmed it.");
      } else {
        toast.success(
          `Stop signal sent to ${agent.name}. It may take a moment to respond.`,
        );
      }
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : `Failed to stop ${agent.name}'s current turn.`,
      );
    } finally {
      pendingRef.current = false;
      setPendingChannelId(null);
    }
  }

  return { pendingChannelId, stopTurn };
}
