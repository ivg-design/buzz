import { Button } from "@/shared/ui/button";
import { formatTimedTaskTime } from "./form";
import type { TimedTask } from "./types";

const stateLabels: Record<TimedTask["deliveryState"], string> = {
  idle: "Scheduled",
  waiting_offline: "Agent offline",
  pending: "Sending",
  delivered: "Delivered",
};

export function TimedTaskList({
  tasks,
  pending,
  onEdit,
  onStatus,
  onOpenThread,
  channelNames,
}: {
  tasks: TimedTask[];
  pending: boolean;
  onEdit: (task: TimedTask) => void;
  onStatus: (
    task: TimedTask,
    status: "active" | "paused" | "cancelled",
  ) => void;
  onOpenThread: (task: TimedTask) => void;
  channelNames: Record<string, string>;
}) {
  if (tasks.length === 0)
    return (
      <p className="py-3 text-sm text-muted-foreground">
        No timed tasks yet. Add an instruction to send at a regular interval.
      </p>
    );
  return (
    <ul className="divide-y divide-border/60">
      {tasks.map((task) => (
        <li key={task.id} className="flex flex-col gap-2 py-4 first:pt-0">
          <div className="flex items-start justify-between gap-3">
            <p className="line-clamp-3 whitespace-pre-wrap break-words text-sm font-medium">
              {task.instruction}
            </p>
            <span className="shrink-0 text-xs text-muted-foreground">
              {task.status === "active"
                ? stateLabels[task.deliveryState]
                : task.status}
            </span>
          </div>
          <p className="text-xs text-muted-foreground">
            Every {task.interval.value} {task.interval.unit} ·{" "}
            {channelNames[task.channelId] ?? "Conversation"}
          </p>
          <p className="text-xs text-muted-foreground">
            {task.deliveredCount} delivered
            {task.repetition.mode === "count"
              ? ` of ${task.repetition.count}`
              : ""}{" "}
            · {task.missedCount} missed
            {task.repetition.mode === "until"
              ? ` · Ends ${task.repetition.localDateTime.replace("T", " ")} (${task.repetition.timeZone})`
              : ""}
          </p>
          <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <dt>Next</dt>
            <dd>
              {task.status === "active"
                ? formatTimedTaskTime(task.nextRunAt)
                : "—"}
            </dd>
            <dt>Last delivery</dt>
            <dd>{formatTimedTaskTime(task.lastDeliveredAt)}</dd>
          </dl>
          {task.lastError ? (
            <p role="status" className="break-words text-xs text-destructive">
              {task.lastError}
            </p>
          ) : null}
          <div className="flex flex-wrap gap-1">
            {task.threadId && task.rootPublished ? (
              <Button
                size="sm"
                variant="ghost"
                onClick={() => onOpenThread(task)}
              >
                Open thread
              </Button>
            ) : null}
            {task.status === "active" || task.status === "paused" ? (
              <>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={pending}
                  onClick={() => onEdit(task)}
                >
                  Edit
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={pending}
                  onClick={() =>
                    onStatus(
                      task,
                      task.status === "paused" ? "active" : "paused",
                    )
                  }
                >
                  {task.status === "paused" ? "Resume" : "Pause"}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={pending}
                  onClick={() => onStatus(task, "cancelled")}
                >
                  Cancel task
                </Button>
              </>
            ) : null}
          </div>
        </li>
      ))}
    </ul>
  );
}
