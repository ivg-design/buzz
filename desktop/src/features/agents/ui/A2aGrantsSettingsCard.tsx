import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertCircle,
  Bot,
  CheckCircle2,
  Clock3,
  LoaderCircle,
  RefreshCw,
  ShieldCheck,
  Trash2,
} from "lucide-react";

import { useRelayAgentsQuery } from "@/features/agents/hooks";
import { useChannelMembersQuery } from "@/features/channels/hooks";
import {
  a2aPeerLabel,
  buildA2aRepositoryChoices,
  parseA2aPathPrefixes,
  validateA2aCapability,
  validateA2aWorktreeId,
  verifiedA2aPeers,
  workspaceProjectMatchesScope,
} from "@/features/agents/lib/a2aGrantSettings";
import { WorkspaceProjectSettingsCard } from "@/features/agents/ui/WorkspaceProjectSettingsCard";
import { useCommunities } from "@/features/communities/useCommunities";
import { normalizeRelayUrl } from "@/features/communities/communityStorage";
import { useProjectsQuery } from "@/features/projects/hooks";
import { useIdentityQuery } from "@/shared/api/hooks";
import {
  getA2aGrants,
  listA2aCheckouts,
  removeA2aGrant,
  type A2aGrant,
  type A2aGrantScope,
  upsertA2aGrant,
} from "@/shared/api/tauriA2aGrants";
import type { RelayAgent } from "@/shared/api/types";
import {
  getWorkspaceProject,
  type WorkspaceProject,
  type WorkspaceProjectState,
} from "@/shared/api/tauriWorkspaceProject";
import { normalizePubkey } from "@/shared/lib/pubkey";
import { Button } from "@/shared/ui/button";
import { Input } from "@/shared/ui/input";
import { Textarea } from "@/shared/ui/textarea";
import { A2aProjectAssignments } from "@/features/agents/ui/A2aProjectAssignments";
import {
  SettingsOptionGroup,
  SettingsOptionRow,
} from "@/features/settings/ui/SettingsOptionGroup";

const selectClassName =
  "h-9 w-full rounded-lg border border-input/40 bg-background px-3 text-sm text-foreground focus-visible:outline-hidden focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function grantQueryKey(scope: A2aGrantScope | null, checkoutRoot: string) {
  return [
    "a2a-grants",
    scope?.projectAddress ?? "none",
    scope?.homeChannel ?? "none",
    scope?.repository ?? "none",
    checkoutRoot || "none",
  ] as const;
}

function Field({
  children,
  description,
  label,
}: {
  children: React.ReactNode;
  description?: React.ReactNode;
  label: React.ReactNode;
}) {
  return (
    <div className="space-y-1.5">
      <div className="text-sm font-medium text-foreground">{label}</div>
      {children}
      {description ? (
        <p className="text-xs text-muted-foreground/70">{description}</p>
      ) : null}
    </div>
  );
}

function PeerIdentity({ peer }: { peer: RelayAgent }) {
  return (
    <dl
      className="grid gap-1 rounded-lg border border-border/60 bg-muted/20 p-3 text-xs"
      data-testid="a2a-peer-identity"
    >
      <div className="grid gap-0.5 sm:grid-cols-[7rem_1fr]">
        <dt className="text-muted-foreground">Agent public key</dt>
        <dd className="break-all font-mono text-foreground">{peer.pubkey}</dd>
      </div>
      <div className="grid gap-0.5 sm:grid-cols-[7rem_1fr]">
        <dt className="text-muted-foreground">Verified owner</dt>
        <dd className="break-all font-mono text-foreground">
          {peer.ownerPubkey}
        </dd>
      </div>
    </dl>
  );
}

function GrantPeerNames({
  grant,
  peersByPubkey,
}: {
  grant: A2aGrant;
  peersByPubkey: ReadonlyMap<string, RelayAgent>;
}) {
  return (
    <div className="space-y-1">
      {grant.requesterPubkeys.map((pubkey) => (
        <div key={pubkey}>
          <span className="font-medium">
            {peersByPubkey.get(pubkey)?.name || "Unknown agent"}
          </span>
          <div className="break-all font-mono text-xs text-muted-foreground">
            {pubkey}
          </div>
        </div>
      ))}
    </div>
  );
}

export function A2aGrantsSettingsCard() {
  const queryClient = useQueryClient();
  const { activeCommunity } = useCommunities();
  const identityQuery = useIdentityQuery();
  const projectsQuery = useProjectsQuery();
  const agentsQuery = useRelayAgentsQuery({ enabled: true });
  const activeRelayUrl = activeCommunity?.relayUrl
    ? normalizeRelayUrl(activeCommunity.relayUrl)
    : null;
  const signerPubkey = identityQuery.data?.pubkey
    ? normalizePubkey(identityQuery.data.pubkey)
    : null;
  const workspaceProjectQueryKey = [
    "workspace-project",
    activeCommunity?.id ?? "none",
  ] as const;
  const workspaceProjectQuery = useQuery({
    enabled: activeRelayUrl != null,
    queryKey: workspaceProjectQueryKey,
    queryFn: getWorkspaceProject,
    staleTime: 5_000,
    retry: 1,
  });
  const repositoryChoices = React.useMemo(
    () => buildA2aRepositoryChoices(projectsQuery.data ?? []),
    [projectsQuery.data],
  );
  const [choiceId, setChoiceId] = React.useState("");
  const choice =
    repositoryChoices.find((candidate) => candidate.id === choiceId) ?? null;
  const scope = React.useMemo<A2aGrantScope | null>(
    () =>
      choice
        ? {
            ...choice.scope,
            reposDir: activeCommunity?.reposDir ?? null,
          }
        : null,
    [activeCommunity?.reposDir, choice],
  );

  React.useEffect(() => {
    if (!repositoryChoices.some((candidate) => candidate.id === choiceId)) {
      const stored = workspaceProjectQuery.data?.project;
      const storedChoice = repositoryChoices.find((candidate) =>
        workspaceProjectMatchesScope(stored, candidate.scope),
      );
      setChoiceId(storedChoice?.id ?? repositoryChoices[0]?.id ?? "");
    }
  }, [choiceId, repositoryChoices, workspaceProjectQuery.data?.project]);

  const checkoutsQuery = useQuery({
    enabled: scope != null,
    queryKey: ["a2a-grants", "checkouts", scope],
    queryFn: () => {
      if (!scope) throw new Error("Choose a project repository.");
      return listA2aCheckouts(scope);
    },
    staleTime: 10_000,
    retry: 1,
  });
  const [checkoutRoot, setCheckoutRoot] = React.useState("");
  const checkout =
    checkoutsQuery.data?.find((candidate) => candidate.path === checkoutRoot) ??
    null;

  React.useEffect(() => {
    if (
      !checkoutsQuery.data?.some((candidate) => candidate.path === checkoutRoot)
    ) {
      setCheckoutRoot(checkoutsQuery.data?.[0]?.path ?? "");
    }
  }, [checkoutRoot, checkoutsQuery.data]);

  const workspaceCandidate = React.useMemo<WorkspaceProject | null>(
    () =>
      choice && scope && checkout
        ? {
            projectAddress: scope.projectAddress,
            homeChannel: scope.homeChannel,
            repository: scope.repository,
            displayName: choice.projectName,
            instructionRevision: checkout.baseSha,
          }
        : null,
    [checkout, choice, scope],
  );
  const workspaceConfigured = workspaceProjectMatchesScope(
    workspaceProjectQuery.data?.project,
    scope,
  );
  const authorizedScope = workspaceConfigured ? scope : null;

  const grantsQuery = useQuery({
    enabled: authorizedScope != null && checkout != null,
    queryKey: grantQueryKey(authorizedScope, checkoutRoot),
    queryFn: () => {
      if (!authorizedScope || !checkout) {
        throw new Error("Configure this Workspace Project first.");
      }
      return getA2aGrants(authorizedScope, checkout.path);
    },
    staleTime: 5_000,
    retry: 1,
  });

  const membersQuery = useChannelMembersQuery(
    authorizedScope?.homeChannel ?? null,
    authorizedScope != null,
  );

  const peers = React.useMemo(
    () =>
      verifiedA2aPeers(
        agentsQuery.data ?? [],
        authorizedScope?.homeChannel,
        membersQuery.data?.map((member) => member.pubkey) ?? null,
      ),
    [agentsQuery.data, authorizedScope?.homeChannel, membersQuery.data],
  );
  const peersByPubkey = React.useMemo(
    () =>
      new Map((agentsQuery.data ?? []).map((agent) => [agent.pubkey, agent])),
    [agentsQuery.data],
  );
  const [peerPubkey, setPeerPubkey] = React.useState("");
  const peer =
    peers.find((candidate) => candidate.pubkey === peerPubkey) ?? null;

  React.useEffect(() => {
    if (!peers.some((candidate) => candidate.pubkey === peerPubkey)) {
      setPeerPubkey(peers[0]?.pubkey ?? "");
    }
  }, [peerPubkey, peers]);

  const [capability, setCapability] = React.useState("implementation");
  const [pathText, setPathText] = React.useState("");
  const [worktreeId, setWorktreeId] = React.useState("");
  const [notice, setNotice] = React.useState<string | null>(null);

  React.useEffect(() => {
    setWorktreeId(checkout?.suggestedWorktreeId ?? "");
  }, [checkout?.suggestedWorktreeId]);

  const capabilityError = validateA2aCapability(capability);
  const worktreeError = validateA2aWorktreeId(worktreeId);
  const parsedPaths = parseA2aPathPrefixes(pathText);
  const formReady =
    authorizedScope != null &&
    checkout != null &&
    peer != null &&
    capabilityError == null &&
    worktreeError == null &&
    parsedPaths.error == null;

  const saveMutation = useMutation({
    mutationFn: async () => {
      if (!authorizedScope || !checkout || !peer || parsedPaths.error) {
        throw new Error("Complete the bounded grant fields before saving.");
      }
      if (!activeRelayUrl || !signerPubkey) {
        throw new Error(
          "The active community and signing identity must be loaded first.",
        );
      }
      return upsertA2aGrant({
        scope: authorizedScope,
        checkoutRoot: checkout.path,
        expectedBranch: checkout.branch,
        expectedBaseSha: checkout.baseSha,
        peerPubkey: peer.pubkey,
        capability,
        pathPrefixes: parsedPaths.paths,
        worktreeId,
        expectedRelayUrl: activeRelayUrl,
        expectedSignerPubkey: signerPubkey,
      });
    },
    onSuccess: (state) => {
      queryClient.setQueryData(
        grantQueryKey(authorizedScope, checkoutRoot),
        state,
      );
      setNotice("Grant saved. Restart running agents to apply it.");
    },
  });
  const removeMutation = useMutation({
    mutationFn: async (grantId: string) => {
      if (!authorizedScope || !checkout) {
        throw new Error("Configure this Workspace Project first.");
      }
      if (!activeRelayUrl || !signerPubkey) {
        throw new Error(
          "The active community and signing identity must be loaded first.",
        );
      }
      return removeA2aGrant({
        scope: authorizedScope,
        checkoutRoot: checkout.path,
        grantId,
        expectedRelayUrl: activeRelayUrl,
        expectedSignerPubkey: signerPubkey,
      });
    },
    onSuccess: (state) => {
      queryClient.setQueryData(
        grantQueryKey(authorizedScope, checkoutRoot),
        state,
      );
      setNotice("Grant revoked. Restart running agents to apply it.");
    },
  });
  const operationError = saveMutation.error ?? removeMutation.error;
  const busy = saveMutation.isPending || removeMutation.isPending;

  return (
    <>
      <SettingsOptionGroup
        data-testid="a2a-grants-settings"
        description="Allow verified project agents to exchange narrowly scoped work through Buzz."
        title="Agent-to-agent collaboration"
      >
        <div className="space-y-5 px-4 py-4">
          {projectsQuery.isPending ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <LoaderCircle className="h-4 w-4 animate-spin" /> Loading
              projects…
            </div>
          ) : null}
          {projectsQuery.isError ? (
            <div
              className="flex items-start gap-2 text-sm text-destructive"
              role="alert"
            >
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>
                Projects could not be loaded:{" "}
                {errorMessage(projectsQuery.error)}
              </span>
            </div>
          ) : null}
          {!projectsQuery.isPending &&
          !projectsQuery.isError &&
          repositoryChoices.length === 0 ? (
            <div className="rounded-lg border border-border/60 bg-muted/20 p-3 text-sm text-muted-foreground">
              No eligible repository found. A2A requires an announced project
              with a home channel, a canonical GitHub repository, and a local
              checkout.
            </div>
          ) : null}

          {repositoryChoices.length > 0 ? (
            <Field
              label={
                <label htmlFor="a2a-project-repository">
                  Project and repository
                </label>
              }
            >
              <select
                className={selectClassName}
                id="a2a-project-repository"
                onChange={(event) => {
                  setChoiceId(event.target.value);
                  setNotice(null);
                }}
                value={choiceId}
              >
                {repositoryChoices.map((candidate) => (
                  <option key={candidate.id} value={candidate.id}>
                    {candidate.label}
                  </option>
                ))}
              </select>
            </Field>
          ) : null}

          {scope && checkoutsQuery.isPending ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <LoaderCircle className="h-4 w-4 animate-spin" /> Finding matching
              checkouts…
            </div>
          ) : null}
          {checkoutsQuery.isError ? (
            <div
              className="space-y-2 rounded-lg border border-destructive/40 p-3 text-sm"
              role="alert"
            >
              <div className="flex items-start gap-2 text-destructive">
                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                <span>{errorMessage(checkoutsQuery.error)}</span>
              </div>
              <Button
                onClick={() => void checkoutsQuery.refetch()}
                size="sm"
                variant="outline"
              >
                <RefreshCw /> Try again
              </Button>
            </div>
          ) : null}
          {scope && checkoutsQuery.data?.length === 0 ? (
            <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm text-foreground">
              No matching checkout exists directly inside this community’s
              repositories folder. Clone the selected repository there, or
              update the folder in Hosted communities.
            </div>
          ) : null}
          {checkoutsQuery.data && checkoutsQuery.data.length > 0 ? (
            <Field label={<label htmlFor="a2a-checkout">Local checkout</label>}>
              <select
                className={selectClassName}
                id="a2a-checkout"
                onChange={(event) => {
                  setCheckoutRoot(event.target.value);
                  setNotice(null);
                }}
                value={checkoutRoot}
              >
                {checkoutsQuery.data.map((candidate) => (
                  <option key={candidate.path} value={candidate.path}>
                    {candidate.path} · {candidate.branch}
                  </option>
                ))}
              </select>
            </Field>
          ) : null}

          <WorkspaceProjectSettingsCard
            candidate={workspaceCandidate}
            codexInstructionError={
              workspaceProjectQuery.data?.codexInstructionError ?? null
            }
            codexInstructionStatus={
              workspaceProjectQuery.data?.codexInstructionStatus ?? null
            }
            current={workspaceProjectQuery.data?.project ?? null}
            expectedRelayUrl={activeRelayUrl}
            expectedSignerPubkey={signerPubkey}
            isError={workspaceProjectQuery.isError}
            isPending={workspaceProjectQuery.isPending}
            loadError={workspaceProjectQuery.error}
            onSaved={(result) => {
              const state: WorkspaceProjectState = {
                relayUrl: result.relayUrl,
                project: result.project,
                codexInstructionStatus: result.codexInstructionStatus,
                codexInstructionError: result.codexInstructionError,
              };
              queryClient.setQueryData(workspaceProjectQueryKey, state);
              setNotice(null);
            }}
          />

          {scope && !workspaceConfigured && !workspaceProjectQuery.isPending ? (
            <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm">
              Pin the selected Project as the Workspace Project before assigning
              agents or creating repository grants.
            </div>
          ) : null}

          {authorizedScope ? (
            <A2aProjectAssignments
              agents={agentsQuery.data ?? []}
              agentsError={agentsQuery.error}
              agentsPending={agentsQuery.isPending}
              homeChannel={authorizedScope.homeChannel}
              members={membersQuery.data ?? null}
              membersError={membersQuery.error}
              membersPending={membersQuery.isPending}
            />
          ) : null}

          {authorizedScope && checkout ? (
            <div className="grid gap-4 sm:grid-cols-2">
              <Field
                label={<label htmlFor="a2a-peer">Verified peer agent</label>}
              >
                <select
                  className={selectClassName}
                  disabled={agentsQuery.isPending || peers.length === 0}
                  id="a2a-peer"
                  onChange={(event) => setPeerPubkey(event.target.value)}
                  value={peerPubkey}
                >
                  {peers.length === 0 ? (
                    <option value="">No verified project agents</option>
                  ) : null}
                  {peers.map((candidate) => (
                    <option key={candidate.pubkey} value={candidate.pubkey}>
                      {a2aPeerLabel(candidate)}
                    </option>
                  ))}
                </select>
              </Field>
              <Field
                description={capabilityError}
                label={<label htmlFor="a2a-capability">Capability</label>}
              >
                <Input
                  aria-invalid={capabilityError != null}
                  id="a2a-capability"
                  maxLength={64}
                  onChange={(event) => setCapability(event.target.value)}
                  placeholder="implementation"
                  value={capability}
                />
              </Field>
              <Field
                description={
                  worktreeError ?? `Current branch: ${checkout.branch}`
                }
                label={<label htmlFor="a2a-worktree-id">Worktree ID</label>}
              >
                <Input
                  aria-invalid={worktreeError != null}
                  id="a2a-worktree-id"
                  maxLength={128}
                  onChange={(event) => setWorktreeId(event.target.value)}
                  value={worktreeId}
                />
              </Field>
              <Field
                description={
                  parsedPaths.error ??
                  "One existing file or directory per line. Repository root and .git are never allowed."
                }
                label={<label htmlFor="a2a-paths">Allowed paths</label>}
              >
                <Textarea
                  aria-invalid={parsedPaths.error != null}
                  className="min-h-20 font-mono text-sm"
                  id="a2a-paths"
                  onChange={(event) => setPathText(event.target.value)}
                  placeholder={"src\ncrates/render"}
                  value={pathText}
                />
              </Field>
            </div>
          ) : null}

          {peer ? <PeerIdentity peer={peer} /> : null}
          {authorizedScope &&
          checkout &&
          peers.length === 0 &&
          !agentsQuery.isPending &&
          !membersQuery.isPending &&
          !agentsQuery.isError &&
          !membersQuery.isError ? (
            <div className="flex items-start gap-2 rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm">
              <Bot className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
              <span>
                Assign a relay-verified agent to this Project above before
                granting repository access.
              </span>
            </div>
          ) : null}
          {agentsQuery.isError ? (
            <div className="text-sm text-destructive" role="alert">
              Verified agents could not be loaded:{" "}
              {errorMessage(agentsQuery.error)}
            </div>
          ) : null}

          <div className="flex items-start gap-2 rounded-lg border border-border/60 bg-muted/20 p-3 text-xs text-muted-foreground">
            <Clock3 className="mt-0.5 h-4 w-4 shrink-0" />
            <span>
              Each dispatched job carries its own expiry. The MCP default is 1
              hour and the hard maximum is 7 days. This local grant remains
              until you revoke it.
            </span>
          </div>
          <div
            className="space-y-1 rounded-lg border border-border/60 bg-muted/20 p-3 text-xs text-muted-foreground"
            data-testid="a2a-agent-contract"
          >
            <p>
              Agents collaborate through five typed tools: buzz_a2a_dispatch,
              buzz_a2a_inbox, buzz_a2a_status, buzz_a2a_cancel, and
              buzz_a2a_handoff.
            </p>
            <p>
              A relay ACK confirms event receipt only. Agents must use status
              until the recipient reports processed, then accepted, and must
              cancel or hand off blocked work instead of duplicating it.
            </p>
            <p>
              Keep parallel worktrees and allowed paths disjoint. Unknown peers,
              stale scopes, conflicts, and invalid lifecycle chains fail closed.
            </p>
          </div>
          {operationError ? (
            <div
              className="flex items-start gap-2 text-sm text-destructive"
              role="alert"
            >
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>{errorMessage(operationError)}</span>
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
          {checkout ? (
            <Button
              disabled={!formReady || busy}
              onClick={() => {
                setNotice(null);
                saveMutation.mutate();
              }}
              type="button"
            >
              {saveMutation.isPending ? (
                <LoaderCircle className="animate-spin" />
              ) : (
                <ShieldCheck />
              )}
              Save bounded grant
            </Button>
          ) : null}
        </div>
      </SettingsOptionGroup>

      {authorizedScope && checkout ? (
        <SettingsOptionGroup
          data-testid="a2a-configured-grants"
          description={grantsQuery.data?.storage}
          title="Configured grants"
        >
          {grantsQuery.isPending ? (
            <SettingsOptionRow>
              <div className="flex items-center gap-2 text-muted-foreground">
                <LoaderCircle className="h-4 w-4 animate-spin" /> Loading
                grants…
              </div>
            </SettingsOptionRow>
          ) : null}
          {grantsQuery.isError ? (
            <SettingsOptionRow>
              <div className="space-y-2" role="alert">
                <div className="flex items-start gap-2 text-destructive">
                  <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>{errorMessage(grantsQuery.error)}</span>
                </div>
                <Button
                  onClick={() => void grantsQuery.refetch()}
                  size="sm"
                  variant="outline"
                >
                  <RefreshCw /> Try again
                </Button>
              </div>
            </SettingsOptionRow>
          ) : null}
          {grantsQuery.data?.grants.length === 0 ? (
            <SettingsOptionRow>
              <p className="text-muted-foreground">
                No grants for this project checkout.
              </p>
            </SettingsOptionRow>
          ) : null}
          {grantsQuery.data?.grants.map((grant) => (
            <SettingsOptionRow className="items-start" key={grant.id}>
              <div className="min-w-0 flex-1 space-y-2">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-medium">
                    {grant.capabilities.join(", ")}
                  </span>
                  <span
                    className={
                      grant.status === "ready"
                        ? "text-xs text-emerald-600"
                        : "text-xs text-amber-600"
                    }
                  >
                    {grant.status === "ready" ? "Ready" : "Needs refresh"}
                  </span>
                </div>
                <GrantPeerNames grant={grant} peersByPubkey={peersByPubkey} />
                <p className="break-words text-xs text-muted-foreground">
                  {grant.pathPrefixes.join(", ")} · {grant.worktreeId}
                </p>
                {grant.statusMessage ? (
                  <p className="text-xs text-amber-600">
                    {grant.statusMessage}
                  </p>
                ) : null}
              </div>
              <Button
                aria-label={`Revoke ${grant.capabilities.join(", ")} A2A grant`}
                disabled={busy}
                onClick={() => {
                  setNotice(null);
                  removeMutation.mutate(grant.id);
                }}
                size="icon"
                type="button"
                variant="ghost"
              >
                {removeMutation.isPending ? (
                  <LoaderCircle className="animate-spin" />
                ) : (
                  <Trash2 />
                )}
              </Button>
            </SettingsOptionRow>
          ))}
        </SettingsOptionGroup>
      ) : null}
    </>
  );
}
