import * as React from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  AlertCircle,
  Bot,
  CheckCircle2,
  LoaderCircle,
  UserMinus,
  UserPlus,
} from "lucide-react";

import { relayAgentsQueryKey } from "@/features/agents/hooks";
import {
  a2aPeerLabel,
  verifiedA2aProjectPeers,
} from "@/features/agents/lib/a2aGrantSettings";
import { canAddChannelMembers } from "@/features/channels/lib/channelMemberAdmission";
import {
  invalidateChannelState,
  useChannelsQuery,
} from "@/features/channels/hooks";
import { normalizeRelayUrl } from "@/features/communities/communityStorage";
import { useCommunities } from "@/features/communities/useCommunities";
import { useIdentityQuery } from "@/shared/api/hooks";
import { addChannelMembers, removeChannelMember } from "@/shared/api/tauri";
import type { ChannelMember, RelayAgent } from "@/shared/api/types";
import { normalizePubkey } from "@/shared/lib/pubkey";
import { Button } from "@/shared/ui/button";

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function A2aProjectAssignments({
  agents,
  agentsError,
  agentsPending,
  homeChannel,
  members,
  membersError,
  membersPending,
}: {
  agents: readonly RelayAgent[];
  agentsError: unknown;
  agentsPending: boolean;
  homeChannel: string;
  members: readonly ChannelMember[] | null;
  membersError: unknown;
  membersPending: boolean;
}) {
  const queryClient = useQueryClient();
  const { activeCommunity } = useCommunities();
  const identityQuery = useIdentityQuery();
  const channelsQuery = useChannelsQuery();
  const [notice, setNotice] = React.useState<string | null>(null);
  const channel =
    channelsQuery.data?.find((candidate) => candidate.id === homeChannel) ??
    null;
  const signerPubkey = identityQuery.data?.pubkey
    ? normalizePubkey(identityQuery.data.pubkey)
    : null;
  const relayUrl = activeCommunity?.relayUrl
    ? normalizeRelayUrl(activeCommunity.relayUrl)
    : null;
  const selfRole = signerPubkey
    ? (members?.find(
        (member) => normalizePubkey(member.pubkey) === signerPubkey,
      )?.role ?? null)
    : null;
  const peers = React.useMemo(
    () =>
      verifiedA2aProjectPeers(
        agents,
        homeChannel,
        members?.map((member) => member.pubkey) ?? null,
      ),
    [agents, homeChannel, members],
  );
  const unverifiedCount = Math.max(0, agents.length - peers.length);
  const canAdd = canAddChannelMembers({
    channelType: channel?.channelType,
    visibility: channel?.visibility,
    selfRole,
  });

  const assignmentMutation = useMutation({
    mutationFn: async ({
      action,
      agent,
    }: {
      action: "add" | "remove";
      agent: RelayAgent;
    }) => {
      if (!relayUrl || !signerPubkey) {
        throw new Error(
          "The active community and signing identity must be loaded first.",
        );
      }
      if (action === "add") {
        const result = await addChannelMembers({
          channelId: homeChannel,
          pubkeys: [agent.pubkey],
          role: "bot",
          expectedRelayUrl: relayUrl,
          expectedSignerPubkey: signerPubkey,
        });
        const error = result.errors.find(
          (failure) =>
            normalizePubkey(failure.pubkey) === normalizePubkey(agent.pubkey),
        );
        if (error) throw new Error(error.error);
        if (
          !result.added.some(
            (pubkey) =>
              normalizePubkey(pubkey) === normalizePubkey(agent.pubkey),
          )
        ) {
          throw new Error("The relay did not confirm the Project assignment.");
        }
      } else {
        await removeChannelMember(homeChannel, agent.pubkey, {
          expectedRelayUrl: relayUrl,
          expectedSignerPubkey: signerPubkey,
        });
      }
      return { action, agent };
    },
    onSuccess: async ({ action, agent }) => {
      await Promise.all([
        invalidateChannelState(queryClient, homeChannel),
        queryClient.invalidateQueries({ queryKey: relayAgentsQueryKey }),
      ]);
      setNotice(
        action === "add"
          ? `${a2aPeerLabel(agent)} is assigned to this Project. Add a bounded grant below before dispatching repository work.`
          : `${a2aPeerLabel(agent)} was removed from this Project. Stored grants do not restore Project access.`,
      );
    },
  });

  const loading = agentsPending || membersPending;
  const loadError = agentsError ?? membersError;

  return (
    <section
      className="space-y-3 rounded-xl border border-border/70 bg-muted/10 p-4"
      data-testid="a2a-project-assignments"
    >
      <div className="space-y-1">
        <h3 className="text-sm font-semibold text-foreground">
          Project agent assignment
        </h3>
        <p className="text-xs text-muted-foreground">
          Assignment gives an agent direct access to the Project home channel
          and is required by the relay for A2A. A local grant below separately
          limits repository access.
        </p>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <LoaderCircle className="h-4 w-4 animate-spin" /> Verifying Project
          membership…
        </div>
      ) : null}
      {loadError ? (
        <div
          className="flex items-start gap-2 text-sm text-destructive"
          role="alert"
        >
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            Project assignment could not be verified: {message(loadError)}
          </span>
        </div>
      ) : null}
      {!loading && !loadError && peers.length === 0 ? (
        <div className="flex items-start gap-2 text-sm text-muted-foreground">
          <Bot className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            No relay-verified agents are available. Create or register an agent
            in this community first.
          </span>
        </div>
      ) : null}

      {!loading && !loadError
        ? peers.map(({ agent, isProjectMember }) => {
            const ownsAgent = agent.ownerPubkey === signerPubkey;
            const canRemove =
              selfRole === "owner" || selfRole === "admin" || ownsAgent;
            const disabledReason = isProjectMember
              ? canRemove
                ? null
                : "Only a Project owner/admin or this agent’s verified owner can remove it."
              : canAdd
                ? null
                : "The current identity cannot add members to this Project home channel.";
            const pending =
              assignmentMutation.isPending &&
              assignmentMutation.variables.agent.pubkey === agent.pubkey;
            return (
              <article
                className="space-y-3 rounded-lg border border-border/60 bg-background/70 p-3"
                key={agent.pubkey}
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="font-medium text-foreground">
                      {a2aPeerLabel(agent)}
                    </div>
                    <div
                      className={
                        isProjectMember
                          ? "text-xs text-emerald-600"
                          : "text-xs text-amber-600"
                      }
                    >
                      {isProjectMember
                        ? "Direct Project member"
                        : "Not assigned to this Project"}
                    </div>
                  </div>
                  <Button
                    disabled={
                      disabledReason != null || assignmentMutation.isPending
                    }
                    onClick={() => {
                      setNotice(null);
                      assignmentMutation.mutate({
                        action: isProjectMember ? "remove" : "add",
                        agent,
                      });
                    }}
                    size="sm"
                    title={disabledReason ?? undefined}
                    type="button"
                    variant={isProjectMember ? "outline" : "default"}
                  >
                    {pending ? (
                      <LoaderCircle className="animate-spin" />
                    ) : isProjectMember ? (
                      <UserMinus />
                    ) : (
                      <UserPlus />
                    )}
                    {pending
                      ? isProjectMember
                        ? "Removing…"
                        : "Adding…"
                      : isProjectMember
                        ? "Remove from Project"
                        : "Add to Project"}
                  </Button>
                </div>
                <dl className="grid gap-1 text-xs sm:grid-cols-[7rem_1fr]">
                  <dt className="text-muted-foreground">Agent public key</dt>
                  <dd className="break-all font-mono">{agent.pubkey}</dd>
                  <dt className="text-muted-foreground">Verified owner</dt>
                  <dd className="break-all font-mono">{agent.ownerPubkey}</dd>
                </dl>
                {disabledReason ? (
                  <p className="text-xs text-muted-foreground">
                    {disabledReason}
                  </p>
                ) : null}
              </article>
            );
          })
        : null}

      {unverifiedCount > 0 ? (
        <p className="text-xs text-muted-foreground">
          {unverifiedCount} agent{" "}
          {unverifiedCount === 1 ? "identity was" : "identities were"} hidden
          because the relay did not provide a verified owner binding.
        </p>
      ) : null}
      {assignmentMutation.error ? (
        <div className="text-sm text-destructive" role="alert">
          Assignment failed: {message(assignmentMutation.error)}
        </div>
      ) : null}
      {notice ? (
        <div
          className="flex items-start gap-2 text-sm text-emerald-600"
          role="status"
        >
          <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{notice}</span>
        </div>
      ) : null}
    </section>
  );
}
