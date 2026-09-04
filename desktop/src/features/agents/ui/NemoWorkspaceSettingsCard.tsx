import type * as React from "react";
import { useQuery } from "@tanstack/react-query";
import {
  AlertCircle,
  CheckCircle2,
  ChevronRight,
  FileText,
  GitBranch,
  LoaderCircle,
  RefreshCw,
  Share2,
  ShieldCheck,
} from "lucide-react";

import {
  getNemoWorkspaceStatus,
  nemoWorkspaceStatusQueryKey,
  type NemoWorkspaceStatus,
} from "@/shared/api/tauriNemoWorkspace";
import { normalizeRelayUrl } from "@/features/communities/communityStorage";
import { useCommunities } from "@/features/communities/useCommunities";
import { Badge } from "@/shared/ui/badge";
import { Button } from "@/shared/ui/button";
import { SettingsOptionGroup } from "@/features/settings/ui/SettingsOptionGroup";

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function StatusIcon({ ready }: { ready: boolean }) {
  return ready ? (
    <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-600" />
  ) : (
    <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
  );
}

function WorkspaceStatusRow({
  detail,
  error,
  icon,
  label,
  ready,
}: {
  detail: string;
  error: string | null;
  icon: React.ReactNode;
  label: string;
  ready: boolean;
}) {
  return (
    <div
      className="flex items-start gap-3 px-4 py-3"
      data-testid={`nemo-${label.toLowerCase().replaceAll(" ", "-")}-status`}
    >
      <div className="mt-0.5 text-muted-foreground">{icon}</div>
      <div className="min-w-0 flex-1">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-sm font-medium text-foreground">{label}</div>
            <p className="mt-0.5 text-sm text-muted-foreground">{detail}</p>
          </div>
          <StatusIcon ready={ready} />
        </div>
        {!ready && error ? (
          <p className="mt-2 text-xs text-destructive" role="alert">
            {error}
          </p>
        ) : null}
      </div>
    </div>
  );
}

export function NemoWorkspaceStatusView({
  error,
  isPending,
  onRetry,
  status,
}: {
  error: unknown;
  isPending: boolean;
  onRetry: () => void;
  status: NemoWorkspaceStatus | null;
}) {
  if (isPending) {
    return (
      <div className="flex items-center gap-2 px-4 py-5 text-sm text-muted-foreground">
        <LoaderCircle className="h-4 w-4 animate-spin" /> Verifying Nemo
        workspace…
      </div>
    );
  }

  if (error || !status) {
    return (
      <div className="space-y-3 px-4 py-4" role="alert">
        <div className="flex items-start gap-2 text-sm text-destructive">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            Nemo workspace could not be verified:{" "}
            {message(error ?? "No status returned")}
          </span>
        </div>
        <Button onClick={onRetry} size="sm" type="button" variant="outline">
          <RefreshCw /> Try again
        </Button>
      </div>
    );
  }

  const repositoryReady = status.repositoryAccess.status === "ready";
  const a2aReady = status.a2a.status === "ready";
  const instructionsReady = status.instructions.status === "verified";
  const allReady = repositoryReady && a2aReady && instructionsReady;

  return (
    <div data-testid="nemo-workspace-status">
      <div className="flex items-start gap-3 px-4 py-4">
        <div className="rounded-lg bg-primary/10 p-2 text-primary">
          <ShieldCheck className="h-5 w-5" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="font-semibold text-foreground">
              {status.projectName || "Nemo"}
            </h3>
            <Badge variant={allReady ? "success" : "warning"}>
              {allReady ? "Active" : "Needs attention"}
            </Badge>
          </div>
          <p className="mt-1 text-sm text-muted-foreground">
            Shared policy for every managed agent in this community.
          </p>
        </div>
      </div>

      <WorkspaceStatusRow
        detail={
          repositoryReady
            ? "Full Nemo repository read and write access"
            : "Repository access unavailable"
        }
        error={status.repositoryAccess.error}
        icon={<GitBranch className="h-4 w-4" />}
        label="Repository access"
        ready={repositoryReady}
      />
      <WorkspaceStatusRow
        detail={
          a2aReady
            ? "Enabled automatically for Nemo agents"
            : "Agent collaboration unavailable"
        }
        error={status.a2a.error}
        icon={<Share2 className="h-4 w-4" />}
        label="Agent collaboration"
        ready={a2aReady}
      />
      <WorkspaceStatusRow
        detail={
          instructionsReady
            ? "Active shared Nemo instructions"
            : "Nemo instructions unavailable"
        }
        error={status.instructions.error}
        icon={<FileText className="h-4 w-4" />}
        label="Instructions"
        ready={instructionsReady}
      />

      <details
        className="group px-4 py-3"
        data-testid="nemo-instruction-review"
      >
        <summary className="flex cursor-pointer list-none items-center gap-2 text-sm font-medium text-foreground">
          <ChevronRight className="h-4 w-4 transition-transform group-open:rotate-90" />
          Review workspace instructions
        </summary>
        <div className="mt-3 space-y-3 pl-6">
          {instructionsReady && status.instructions.content ? (
            <pre className="max-h-80 overflow-auto whitespace-pre-wrap rounded-lg border border-border/60 bg-muted/20 p-3 text-xs leading-relaxed text-foreground">
              {status.instructions.content}
            </pre>
          ) : (
            <p className="text-sm text-muted-foreground">
              Instruction content is unavailable until verification succeeds.
            </p>
          )}
          <dl className="grid gap-1 text-xs sm:grid-cols-[7rem_1fr]">
            <dt className="text-muted-foreground">Source</dt>
            <dd className="break-words text-foreground">
              {status.instructions.source ?? "Unavailable"}
            </dd>
            {status.instructions.revision ? (
              <>
                <dt className="text-muted-foreground">Revision</dt>
                <dd className="break-all font-mono text-foreground">
                  {status.instructions.revision}
                </dd>
              </>
            ) : null}
          </dl>
        </div>
      </details>

      <details
        className="group px-4 py-3"
        data-testid="nemo-workspace-technical-details"
      >
        <summary className="flex cursor-pointer list-none items-center gap-2 text-xs font-medium text-muted-foreground">
          <ChevronRight className="h-4 w-4 transition-transform group-open:rotate-90" />
          Technical details
        </summary>
        <dl className="mt-3 grid gap-1 pl-6 text-xs sm:grid-cols-[7rem_1fr]">
          <dt className="text-muted-foreground">Repository</dt>
          <dd className="break-all font-mono text-foreground">
            {status.repository}
          </dd>
          <dt className="text-muted-foreground">Local checkout</dt>
          <dd className="break-all font-mono text-foreground">
            {status.checkoutRoot ?? "Unavailable"}
          </dd>
        </dl>
      </details>
    </div>
  );
}

export function NemoWorkspaceSettingsCard() {
  const { activeCommunity } = useCommunities();
  const scope = activeCommunity
    ? {
        communityId: activeCommunity.id,
        relayUrl: normalizeRelayUrl(activeCommunity.relayUrl),
      }
    : null;
  const statusQuery = useQuery({
    enabled: scope !== null,
    queryKey: nemoWorkspaceStatusQueryKey(scope),
    queryFn: () => {
      if (!scope) throw new Error("No active community.");
      return getNemoWorkspaceStatus(scope);
    },
    staleTime: 5_000,
    retry: 1,
  });

  return (
    <SettingsOptionGroup
      data-testid="nemo-workspace-settings"
      description="Nemo access, collaboration, and shared instructions are built in for every enrolled agent."
      title="Nemo workspace"
    >
      <NemoWorkspaceStatusView
        error={scope ? statusQuery.error : new Error("No active community.")}
        isPending={scope !== null && statusQuery.isPending}
        onRetry={() => void statusQuery.refetch()}
        status={statusQuery.data ?? null}
      />
    </SettingsOptionGroup>
  );
}
