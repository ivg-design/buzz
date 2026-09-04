import * as React from "react";
import { useMutation } from "@tanstack/react-query";
import {
  AlertCircle,
  CheckCircle2,
  LoaderCircle,
  Pin,
  RotateCw,
  Trash2,
} from "lucide-react";

import {
  setWorkspaceProject,
  type WorkspaceProject,
  type WorkspaceProjectSaveResult,
} from "@/shared/api/tauriWorkspaceProject";
import { Button } from "@/shared/ui/button";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function sameSelection(
  left: WorkspaceProject | null,
  right: WorkspaceProject | null,
): boolean {
  return (
    left === right ||
    (left != null &&
      right != null &&
      left.projectAddress === right.projectAddress &&
      left.homeChannel === right.homeChannel &&
      left.repository === right.repository &&
      left.displayName === right.displayName &&
      left.instructionRevision === right.instructionRevision)
  );
}

export function WorkspaceProjectSettingsCard({
  candidate,
  current,
  codexInstructionError,
  codexInstructionStatus,
  expectedRelayUrl,
  expectedSignerPubkey,
  isError,
  isPending,
  loadError,
  onSaved,
}: {
  candidate: WorkspaceProject | null;
  current: WorkspaceProject | null;
  codexInstructionError: string | null;
  codexInstructionStatus: "blocked" | "supported" | null;
  expectedRelayUrl: string | null;
  expectedSignerPubkey: string | null;
  isError: boolean;
  isPending: boolean;
  loadError: unknown;
  onSaved: (result: WorkspaceProjectSaveResult) => void;
}) {
  const [notice, setNotice] = React.useState<string | null>(null);
  const mutation = useMutation({
    mutationFn: (project: WorkspaceProject | null) => {
      if (!expectedRelayUrl || !expectedSignerPubkey) {
        throw new Error(
          "The active community and signing identity must be loaded first.",
        );
      }
      return setWorkspaceProject({
        project,
        expectedRelayUrl,
        expectedSignerPubkey,
      });
    },
    onSuccess: (result) => {
      onSaved(result);
      const action = result.project ? "saved" : "cleared";
      if (!result.changed) {
        setNotice("Workspace Project is already current.");
      } else if (result.failedRestartCount > 0) {
        setNotice(
          `Workspace Project ${action}. ${result.restartedCount} managed agent(s) restarted; ${result.failedRestartCount} restart(s) failed and remain marked for restart.`,
        );
      } else {
        setNotice(
          `Workspace Project ${action}. ${result.restartedCount} running managed agent(s) restarted.`,
        );
      }
    },
  });

  const ready = Boolean(expectedRelayUrl && expectedSignerPubkey);
  const exactCandidate = sameSelection(current, candidate);

  return (
    <section
      className="space-y-3 rounded-xl border border-border/70 bg-muted/10 p-4"
      data-testid="workspace-project-settings"
    >
      <div className="space-y-1">
        <h3 className="text-sm font-semibold text-foreground">
          Workspace Project
        </h3>
        <p className="text-xs text-muted-foreground">
          Choose the single reviewed Project whose Nemo instructions every
          managed agent receives in channels, DMs, and background sessions.
          Project membership and bounded grants continue to control authority.
        </p>
      </div>

      {isPending ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <LoaderCircle className="h-4 w-4 animate-spin" /> Loading the
          relay-scoped selection…
        </div>
      ) : null}
      {isError ? (
        <div
          className="flex items-start gap-2 text-sm text-destructive"
          role="alert"
        >
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            Workspace Project could not be loaded: {errorMessage(loadError)}
          </span>
        </div>
      ) : null}
      {!isPending && !isError && !current ? (
        <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm">
          No Workspace Project is configured. Managed agents will not receive
          the Nemo project instructions until an owner pins one below.
        </div>
      ) : null}
      {!isPending && codexInstructionStatus === "blocked" ? (
        <div
          className="flex items-start gap-2 rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive"
          role="alert"
        >
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            {codexInstructionError ??
              "Managed Codex instruction delivery is blocked."}
          </span>
        </div>
      ) : null}

      {current ? (
        <dl className="grid gap-1 rounded-lg border border-border/60 bg-background/70 p-3 text-xs sm:grid-cols-[8rem_1fr]">
          <dt className="text-muted-foreground">Project</dt>
          <dd className="font-medium text-foreground">{current.displayName}</dd>
          <dt className="text-muted-foreground">Project address</dt>
          <dd className="break-all font-mono">{current.projectAddress}</dd>
          <dt className="text-muted-foreground">Home channel</dt>
          <dd className="break-all font-mono">{current.homeChannel}</dd>
          <dt className="text-muted-foreground">Repository</dt>
          <dd className="break-all font-mono">{current.repository}</dd>
          <dt className="text-muted-foreground">Instruction revision</dt>
          <dd className="break-all font-mono">{current.instructionRevision}</dd>
        </dl>
      ) : null}

      {candidate ? (
        <div className="flex flex-wrap gap-2">
          <Button
            disabled={
              !ready || isPending || mutation.isPending || exactCandidate
            }
            onClick={() => {
              setNotice(null);
              mutation.mutate(candidate);
            }}
            size="sm"
            type="button"
          >
            {mutation.isPending && mutation.variables != null ? (
              <LoaderCircle className="animate-spin" />
            ) : current?.projectAddress === candidate.projectAddress ? (
              <RotateCw />
            ) : (
              <Pin />
            )}
            {exactCandidate
              ? "Selected revision pinned"
              : current?.projectAddress === candidate.projectAddress
                ? "Pin selected checkout revision"
                : "Use selected Project"}
          </Button>
        </div>
      ) : (
        <p className="text-xs text-muted-foreground">
          Select a Project repository and matching local checkout to pin an
          immutable instruction revision.
        </p>
      )}
      {current ? (
        <Button
          disabled={!ready || isPending || mutation.isPending}
          onClick={() => {
            setNotice(null);
            mutation.mutate(null);
          }}
          size="sm"
          type="button"
          variant="outline"
        >
          {mutation.isPending && mutation.variables == null ? (
            <LoaderCircle className="animate-spin" />
          ) : (
            <Trash2 />
          )}
          Clear Workspace Project
        </Button>
      ) : null}

      {candidate && !exactCandidate ? (
        <div className="flex items-start gap-2 text-xs text-muted-foreground">
          <Pin className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            Selected immutable revision:{" "}
            <span className="break-all font-mono">
              {candidate.instructionRevision}
            </span>
          </span>
        </div>
      ) : null}
      {mutation.error ? (
        <div
          className="flex items-start gap-2 text-sm text-destructive"
          role="alert"
        >
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{errorMessage(mutation.error)}</span>
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
