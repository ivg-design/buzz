import { invokeTauri } from "@/shared/api/tauri";
import type { TimedTask, TimedTaskInput, TimedTaskScope } from "./types";

export function listTimedTasks(recipientPubkey: string) {
  return invokeTauri<TimedTask[]>("timed_tasks_list", { recipientPubkey });
}

export function saveTimedTask(
  input: TimedTaskInput,
  scope: TimedTaskScope,
  id?: string,
) {
  return invokeTauri<TimedTask>(
    id ? "timed_tasks_update" : "timed_tasks_create",
    {
      ...(id ? { id } : {}),
      input,
      ...scope,
    },
  );
}

export function setTimedTaskStatus(
  id: string,
  status: "active" | "paused" | "cancelled",
  scope: TimedTaskScope,
) {
  return invokeTauri<TimedTask>("timed_tasks_set_status", {
    id,
    status,
    ...scope,
  });
}
