import * as React from "react";
import { Users } from "lucide-react";
import { useRelayAgentsQuery } from "@/features/agents/hooks";
import { useChannelMembersQuery } from "@/features/channels/hooks";
import {
  resolveUserLabel,
  type UserProfileLookup,
} from "@/features/profile/lib/identity";
import type { Channel } from "@/shared/api/types";
import { Button } from "@/shared/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/shared/ui/popover";
import type { ConversationOrganization } from "./OrganizationHistory";
import {
  eligibleThreadAgents,
  participantThreadRoot,
  toggleThreadParticipant,
} from "./threadParticipants";

const EMPTY_PARTICIPANTS: string[] = [];

/** The normal organization write adds/removes future automatic thread participants. */
export function ThreadPeople({
  channel,
  threadRootId,
  organization,
  profiles,
}: {
  channel: Channel;
  threadRootId: string;
  organization: ConversationOrganization;
  profiles?: UserProfileLookup;
}) {
  const [open, setOpen] = React.useState(false);
  const [saveError, setSaveError] = React.useState<string | null>(null);
  const directMembersRequired =
    channel.channelType === "dm" || channel.visibility === "private";
  const agentsQuery = useRelayAgentsQuery({ enabled: open });
  const membersQuery = useChannelMembersQuery(
    channel.id,
    open && directMembersRequired,
  );
  const root = participantThreadRoot(threadRootId, organization.state);
  const selected =
    organization.state.participants.get(root) ?? EMPTY_PARTICIPANTS;
  const eligible = React.useMemo(
    () =>
      eligibleThreadAgents(
        channel,
        agentsQuery.data ?? [],
        membersQuery.data ?? [],
      ),
    [channel, agentsQuery.data, membersQuery.data],
  );
  const eligibleKeys = new Set(eligible.map((agent) => agent.pubkey));
  const directory = new Map(
    (agentsQuery.data ?? []).map((agent) => [
      agent.pubkey.toLowerCase(),
      agent,
    ]),
  );
  const rows = [
    ...eligible.map((agent) => ({
      pubkey: agent.pubkey,
      name: agent.name || resolveUserLabel({ pubkey: agent.pubkey, profiles }),
      eligible: true,
      status: agent.status,
    })),
    ...selected
      .filter((pubkey) => !eligibleKeys.has(pubkey))
      .map((pubkey) => ({
        pubkey,
        name:
          directory.get(pubkey)?.name || resolveUserLabel({ pubkey, profiles }),
        eligible: false,
        status: directory.get(pubkey)?.status,
      })),
  ];
  const isLoading =
    organization.isLoadingHistory ||
    agentsQuery.isPending ||
    agentsQuery.isFetching ||
    (directMembersRequired &&
      (membersQuery.isPending || membersQuery.isFetching));
  const loadError =
    agentsQuery.error ?? (directMembersRequired ? membersQuery.error : null);
  const canEdit =
    channel.isMember && !channel.archivedAt && /^[0-9a-f]{64}$/.test(root);
  const saving = organization.isSaving;
  const scope = `${channel.id}:${root}`;
  const scopeRef = React.useRef(scope);
  scopeRef.current = scope;
  React.useEffect(() => {
    if (scopeRef.current === scope) {
      setOpen(false);
      setSaveError(null);
    }
  }, [scope]);

  async function saveParticipants(agentPubkeys: string[]) {
    const capturedScope = scope;
    setSaveError(null);
    try {
      await organization.apply({
        type: "participants",
        thread_root_id: root,
        agent_pubkeys: agentPubkeys,
      });
    } catch (error) {
      if (scopeRef.current === capturedScope)
        setSaveError(error instanceof Error ? error.message : String(error));
    }
  }

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        setOpen(nextOpen);
        if (nextOpen) {
          void agentsQuery.refetch();
          if (directMembersRequired) void membersQuery.refetch();
        }
      }}
    >
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className="h-8 gap-1.5 px-2"
          data-testid="thread-people-open"
          aria-label={`People in thread${selected.length ? ` (${selected.length} agents)` : ""}`}
        >
          <Users aria-hidden="true" className="h-4 w-4" />
          <span>People{selected.length ? ` · ${selected.length}` : ""}</span>
        </Button>
      </PopoverTrigger>
      <PopoverContent
        align="end"
        className="w-[min(22rem,calc(100vw-2rem))] p-0"
        data-testid="thread-people-popover"
      >
        <div className="border-b border-border px-4 py-3">
          <h2 className="text-sm font-semibold">Agents in this thread</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Selected agents automatically follow future human messages.
          </p>
        </div>
        <div className="max-h-72 overflow-y-auto p-2">
          {isLoading ? (
            <p
              role="status"
              className="px-2 py-3 text-sm text-muted-foreground"
            >
              Loading eligible agents…
            </p>
          ) : null}
          {loadError ? (
            <p role="alert" className="px-2 py-2 text-sm text-destructive">
              Couldn’t load eligible agents.{" "}
              <button
                className="underline"
                type="button"
                onClick={() => {
                  void agentsQuery.refetch();
                  if (directMembersRequired) void membersQuery.refetch();
                }}
              >
                Retry
              </button>
            </p>
          ) : null}
          {rows.map((agent) => {
            const checked = selected.includes(agent.pubkey);
            return (
              <label
                key={agent.pubkey}
                className="flex cursor-pointer items-center gap-3 rounded-md px-2 py-2 hover:bg-accent/50"
              >
                <input
                  type="checkbox"
                  className="h-4 w-4 accent-primary"
                  checked={checked}
                  disabled={
                    !canEdit ||
                    saving ||
                    organization.isLoadingHistory ||
                    (!checked &&
                      (!agent.eligible ||
                        isLoading ||
                        !!loadError ||
                        selected.length >= 100))
                  }
                  onChange={(event) =>
                    void saveParticipants(
                      toggleThreadParticipant(
                        selected,
                        agent.pubkey,
                        event.currentTarget.checked,
                      ),
                    )
                  }
                />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-medium">
                    {agent.name}
                  </span>
                  <span className="block text-xs text-muted-foreground">
                    {agent.eligible
                      ? agent.status === "offline"
                        ? "Offline"
                        : "Enrolled agent"
                      : "No longer eligible · remove from this thread"}
                  </span>
                </span>
              </label>
            );
          })}
          {!isLoading && !loadError && rows.length === 0 ? (
            <p className="px-2 py-3 text-sm text-muted-foreground">
              No eligible agents are available in this conversation.
            </p>
          ) : null}
        </div>
        <div className="space-y-2 border-t border-border px-4 py-3 text-xs text-muted-foreground">
          {saving ? (
            <p role="status">Saving…</p>
          ) : selected.length === 0 ? (
            <p>
              {organization.state.participants.has(root)
                ? "No agents participate automatically."
                : "No participant list has been saved yet."}
            </p>
          ) : null}
          {selected.length > 0 ? (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 px-0 text-xs"
              disabled={!canEdit || saving || organization.isLoadingHistory}
              onClick={() => void saveParticipants([])}
            >
              Remove all agents
            </Button>
          ) : null}
          {saveError ? (
            <p role="alert" className="text-destructive">
              {saveError}
            </p>
          ) : null}
          <p>
            Removing an agent stops future automatic participation. Current work
            continues.
          </p>
          <p>Explicit mentions can still reach other eligible agents.</p>
        </div>
      </PopoverContent>
    </Popover>
  );
}
