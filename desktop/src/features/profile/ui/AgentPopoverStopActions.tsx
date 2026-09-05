import { Octagon } from "lucide-react";

import {
  getAgentWorkingState,
  useAgentWorking,
} from "@/features/agents/agentWorkingSignal";
import { useStopAgentTurn } from "@/features/agents/useStopAgentTurn";
import type { ManagedAgent } from "@/shared/api/types";
import { resolvePopoverStopChannels } from "../lib/popoverStopChannels";
import { Button } from "@/shared/ui/button";

/** Stop the hovered managed agent's active work without changing the open pane. */
export function AgentPopoverStopActions({
  agent,
  channelId: contextChannelId,
  channelNames,
  onBeforeAction,
}: {
  agent: Pick<ManagedAgent, "pubkey" | "name" | "status">;
  channelId?: string | null;
  channelNames: Record<string, string>;
  onBeforeAction: () => void;
}) {
  const { channels } = useAgentWorking(agent.pubkey);
  const { pendingChannelId, stopTurn } = useStopAgentTurn(agent);
  const isLive = agent.status === "running" || agent.status === "deployed";
  const activeChannelIds = resolvePopoverStopChannels(
    isLive,
    channels,
    contextChannelId,
  );
  // A terminal working update can precede the control result. Keep its pending
  // action visible until the correlated result or timeout settles the request.
  const channelIds = [
    ...new Set([
      ...activeChannelIds,
      ...(pendingChannelId ? [pendingChannelId] : []),
    ]),
  ];
  if (channelIds.length === 0) return null;

  return (
    <div
      className="flex flex-col gap-1.5"
      data-testid="agent-popover-stop-actions"
    >
      {channelIds.map((channelId) => {
        const pending = pendingChannelId === channelId;
        const channelLabel = channelNames[channelId]
          ? `#${channelNames[channelId]}`
          : "this channel";
        return (
          <Button
            key={channelId}
            aria-busy={pending}
            aria-label={`${pending ? "Stopping" : "Stop"} ${agent.name}'s current run in ${channelLabel}`}
            className="w-full justify-start"
            data-testid={`agent-popover-stop-${channelId}`}
            disabled={pendingChannelId !== null}
            onClick={() => {
              onBeforeAction();
              // Re-read at activation: do not target a stale channel after the
              // agent moved on, or substitute whichever channel became active.
              if (
                isLive &&
                getAgentWorkingState(agent.pubkey, channelId).working
              ) {
                void stopTurn(channelId);
              }
            }}
            size="sm"
            title={`Interrupt ${agent.name}'s current run in ${channelLabel}. The agent stays online.`}
            type="button"
            variant="outline"
          >
            <Octagon aria-hidden="true" />
            {pending
              ? "Stopping…"
              : channelIds.length > 1
                ? `Stop in ${channelLabel}`
                : "Stop"}
          </Button>
        );
      })}
    </div>
  );
}
