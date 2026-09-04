import { Clock3, Link2, Trash2 } from "lucide-react";
import * as React from "react";
import { toast } from "sonner";

import {
  usePendingInvitesQuery,
  useRevokeInviteMutation,
} from "@/features/community-members/hooks";
import type { PendingInvite } from "@/shared/api/invites";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/shared/ui/alert-dialog";
import { Button } from "@/shared/ui/button";
import { Spinner } from "@/shared/ui/spinner";

function formatTimestamp(unixSeconds: number): string {
  return new Date(unixSeconds * 1_000).toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

function useLabel(invite: PendingInvite): string {
  if (invite.usesRemaining === null) {
    return invite.useCount === 0
      ? "No uses yet"
      : `${invite.useCount} ${invite.useCount === 1 ? "use" : "uses"}`;
  }
  return `${invite.usesRemaining} ${invite.usesRemaining === 1 ? "use" : "uses"} left`;
}

function PendingInviteRow({ invite }: { invite: PendingInvite }) {
  const revokeMutation = useRevokeInviteMutation();
  const [confirmOpen, setConfirmOpen] = React.useState(false);

  async function revoke() {
    try {
      await revokeMutation.mutateAsync(invite.id);
      setConfirmOpen(false);
      toast.success("Invite link revoked");
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : "Couldn’t revoke this invite link.",
      );
    }
  }

  return (
    <div
      className="flex min-h-16 items-center gap-3 px-1 py-3"
      data-testid={`pending-invite-${invite.id}`}
    >
      <span className="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-muted text-muted-foreground">
        <Link2 aria-hidden="true" className="h-4 w-4" />
      </span>
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium">Invite link</p>
        <p className="flex flex-wrap items-center gap-x-1.5 text-xs text-muted-foreground">
          <span>{useLabel(invite)}</span>
          <span aria-hidden="true">·</span>
          <span className="inline-flex items-center gap-1">
            <Clock3 aria-hidden="true" className="h-3 w-3" />
            Expires {formatTimestamp(invite.expiresAt)}
          </span>
        </p>
      </div>
      <Button
        aria-label="Revoke invite link"
        data-testid={`revoke-invite-${invite.id}`}
        disabled={revokeMutation.isPending}
        onClick={() => setConfirmOpen(true)}
        size="icon"
        variant="ghost"
      >
        {revokeMutation.isPending ? (
          <Spinner aria-hidden="true" className="h-4 w-4 border-2" />
        ) : (
          <Trash2 aria-hidden="true" className="h-4 w-4" />
        )}
      </Button>
      <AlertDialog onOpenChange={setConfirmOpen} open={confirmOpen}>
        <AlertDialogContent data-testid="revoke-invite-confirmation">
          <AlertDialogHeader>
            <AlertDialogTitle>Revoke this invite link?</AlertDialogTitle>
            <AlertDialogDescription>
              Anyone who has not already joined with this link will lose access.
              Existing members are not removed.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={revokeMutation.isPending}>
              Keep invite
            </AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={revokeMutation.isPending}
              onClick={(event) => {
                event.preventDefault();
                void revoke();
              }}
            >
              Revoke invite
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

/** Metadata-only list of invite links that can still admit a member. */
export function PendingInviteList() {
  const query = usePendingInvitesQuery();
  const invites = query.data ?? [];

  if (query.isLoading) {
    return (
      <p className="p-4 text-sm text-muted-foreground">Loading invite links…</p>
    );
  }
  if (query.error instanceof Error) {
    return (
      <p className="m-4 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
        {query.error.message}
      </p>
    );
  }
  if (invites.length === 0) {
    return (
      <p className="p-4 text-sm text-muted-foreground">
        No pending invite links.
      </p>
    );
  }
  return (
    <div className="divide-y divide-border/60 px-4 sm:px-5">
      {invites.map((invite) => (
        <PendingInviteRow invite={invite} key={invite.id} />
      ))}
    </div>
  );
}
