import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAppNavigation } from "@/app/navigation/useAppNavigation";
import { useChannelsQuery } from "@/features/channels/hooks";
import { useCommunities } from "@/features/communities/useCommunities";
import { useIdentityQuery } from "@/shared/api/hooks";
import { openDm } from "@/shared/api/tauriChannels";
import { Button } from "@/shared/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/shared/ui/dialog";
import { listTimedTasks, saveTimedTask, setTimedTaskStatus } from "./api";
import { draftForTimedTask, timedTaskInput } from "./form";
import { useTimedTaskDestinations } from "./useTimedTaskDestinations";
import { TimedTaskForm } from "./TimedTaskForm";
import { TimedTaskList } from "./TimedTaskList";
import type { TimedTask, TimedTaskScope } from "./types";

const DM_CHOICE = "__direct_message__";

/** Mounted on demand outside the hover surface so dismissing the popover cannot lose the form. */
export function TimedTaskDialog({
  recipientPubkey,
  recipientName,
  channelId,
  originEventId,
  onClose,
}: {
  recipientPubkey: string;
  recipientName: string;
  channelId?: string | null;
  originEventId?: string | null;
  onClose: () => void;
}) {
  const { activeCommunity } = useCommunities();
  const identity = useIdentityQuery();
  const channelsQuery = useChannelsQuery();
  const queryClient = useQueryClient();
  const { goChannel } = useAppNavigation();
  const [scope] = React.useState<TimedTaskScope>(() => ({
    expectedRelayUrl: activeCommunity?.relayUrl ?? "",
    expectedSignerPubkey: identity.data?.pubkey ?? "",
  }));
  const scopeChanged =
    activeCommunity?.relayUrl !== scope.expectedRelayUrl ||
    identity.data?.pubkey !== scope.expectedSignerPubkey;
  const [editing, setEditing] = React.useState<TimedTask | null>(null);
  const [showForm, setShowForm] = React.useState(true);
  const [draft, setDraft] = React.useState(() =>
    draftForTimedTask(undefined, channelId ?? DM_CHOICE),
  );
  const [error, setError] = React.useState<string | null>(null);
  const queryKey = [
    "timed-tasks",
    scope.expectedRelayUrl,
    scope.expectedSignerPubkey,
    recipientPubkey,
  ];
  const tasksQuery = useQuery({
    queryKey,
    queryFn: () => listTimedTasks(recipientPubkey),
    enabled: !scopeChanged,
    refetchInterval: 5_000,
  });
  const availableChannels = (channelsQuery.data ?? []).filter(
    (channel) => channel.isMember && !channel.archivedAt,
  );
  const { dmChannelLabels, threads } = useTimedTaskDestinations(availableChannels, draft.channelId, scope.expectedSignerPubkey, scope.expectedRelayUrl);
  const channelNames = Object.fromEntries(
    availableChannels.map((channel) => [
      channel.id,
      channel.channelType === "dm" ? dmChannelLabels[channel.id] ?? channel.name : `#${channel.name}`,
    ]),
  );
  const channelOptions = [
    { id: DM_CHOICE, name: `Direct message with ${recipientName}` },
    ...availableChannels.map((channel) => ({
      id: channel.id,
      name: channelNames[channel.id],
    })),
  ];
  if (
    editing &&
    !channelOptions.some((channel) => channel.id === editing.channelId)
  )
    channelOptions.push({
      id: editing.channelId,
      name: "Original conversation",
    });
  const mutation = useMutation({
    mutationFn: async (
      action:
        | { type: "save" }
        | {
            type: "status";
            task: TimedTask;
            status: "active" | "paused" | "cancelled";
          },
    ) => {
      if (
        scopeChanged ||
        !scope.expectedRelayUrl ||
        !scope.expectedSignerPubkey
      )
        throw new Error(
          "The community changed. Close this window and open it again.",
        );
      if (action.type === "status")
        return setTimedTaskStatus(action.task.id, action.status, scope);
      const input = timedTaskInput(
        draft,
        recipientPubkey,
        editing ? (draft.channelId === editing.channelId ? editing.originEventId : null) :
          (draft.channelId === channelId ? (originEventId ?? null) : null),
        Date.now(),
        editing ?? undefined,
      );
      input.recipientName = recipientName;
      if (input.channelId === DM_CHOICE)
        input.channelId = (
          await openDm({ pubkeys: [recipientPubkey], ...scope })
        ).id;
      return saveTimedTask(input, scope, editing?.id);
    },
    onSuccess: async () => {
      setError(null);
      setShowForm(false);
      setEditing(null);
      await queryClient.invalidateQueries({ queryKey });
      await queryClient.invalidateQueries({ queryKey: ["channels"] });
    },
    onError: (failure) =>
      setError(failure instanceof Error ? failure.message : String(failure)),
  });
  const newTask = () => {
    setEditing(null);
    setDraft(draftForTimedTask(undefined, channelId ?? DM_CHOICE));
    setError(null);
    setShowForm(true);
  };
  const timeZone =
    editing?.repetition.mode === "until" &&
    editing.repetition.localDateTime === draft.localDateTime
      ? editing.repetition.timeZone
      : Intl.DateTimeFormat().resolvedOptions().timeZone;
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogContent className="max-w-lg" data-testid="timed-task-dialog">
        <DialogHeader>
          <DialogTitle>
            {showForm
              ? editing
                ? "Edit timed task"
                : "Add timed task"
              : "Timed tasks"}
          </DialogTitle>
          <DialogDescription>{recipientName}</DialogDescription>
        </DialogHeader>
        <div className="max-h-[70vh] overflow-y-auto px-1">
          {scopeChanged ? (
            <p role="alert" className="text-sm text-destructive">
              The community changed. Close this window and open it again.
            </p>
          ) : showForm ? (
            <TimedTaskForm
              draft={draft}
              onChange={setDraft}
              onSubmit={() => mutation.mutate({ type: "save" })}
              onCancel={() => {
                setShowForm(false);
                setError(null);
              }}
              channels={channelOptions}
              threads={threads}
              timeZone={timeZone}
              pending={mutation.isPending}
              editing={Boolean(editing)}
              error={error}
            />
          ) : (
            <>
              {tasksQuery.isPending ? (
                <p role="status" className="text-sm text-muted-foreground">
                  Loading timed tasks…
                </p>
              ) : null}
              {tasksQuery.error ? (
                <div role="alert" className="text-sm text-destructive">
                  {tasksQuery.error.message}
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      void tasksQuery.refetch();
                    }}
                  >
                    Retry
                  </Button>
                </div>
              ) : null}
              <TimedTaskList
                tasks={tasksQuery.data ?? []}
                channelNames={channelNames}
                pending={mutation.isPending}
                onEdit={(task) => {
                  setEditing(task);
                  setDraft(draftForTimedTask(task));
                  setError(null);
                  setShowForm(true);
                }}
                onStatus={(task, status) =>
                  mutation.mutate({ type: "status", task, status })
                }
                onOpenThread={(task) => {
                  if (task.threadId) {
                    void goChannel(task.channelId, { thread: task.threadId });
                    onClose();
                  }
                }}
              />
              {error ? (
                <p role="alert" className="text-sm text-destructive">
                  {error}
                </p>
              ) : null}
              <div className="mt-4 flex justify-end">
                <Button onClick={newTask}>Add timed task</Button>
              </div>
            </>
          )}
        </div>
        {showForm && (tasksQuery.data?.length ?? 0) > 0 ? (
          <Button
            variant="ghost"
            className="justify-start"
            disabled={mutation.isPending}
            onClick={() => setShowForm(false)}
          >
            View timed tasks ({tasksQuery.data?.length})
          </Button>
        ) : null}
      </DialogContent>
    </Dialog>
  );
}
