import type { TimedTaskInput, TimedTaskRepetition } from "./types";

export type TimedTaskDraft = {
  instruction: string;
  channelId: string;
  destination: string;
  intervalValue: string;
  intervalUnit: TimedTaskInput["interval"]["unit"];
  repeatMode: TimedTaskRepetition["mode"];
  count: string;
  localDateTime: string;
};

export function draftForTimedTask(
  task?: TimedTaskInput,
  channelId = "",
): TimedTaskDraft {
  return {
    instruction: task?.instruction ?? "",
    channelId: task?.channelId ?? channelId,
    destination: task ? task.threadRootId ?? (task.postToChannel ? "channel" : "new_thread") : "channel",
    intervalValue: String(task?.interval.value ?? 1),
    intervalUnit: task?.interval.unit ?? "hours",
    repeatMode: task?.repetition.mode ?? "forever",
    count: String(
      task?.repetition.mode === "count" ? task.repetition.count : 5,
    ),
    localDateTime:
      task?.repetition.mode === "until" ? task.repetition.localDateTime : "",
  };
}

function positiveInteger(text: string, label: string) {
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(`${label} must be a positive whole number.`);
  return value;
}

/** Resolve a local cutoff once, retaining its actual offset across later clock changes. */
export function localCutoff(
  localDateTime: string,
  now = Date.now(),
): TimedTaskRepetition {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(localDateTime))
    throw new Error("Choose an end date and time.");
  const date = new Date(localDateTime);
  const parts = localDateTime.split(/[-T:]/).map(Number);
  if (
    !Number.isFinite(date.getTime()) ||
    date.getFullYear() !== parts[0] ||
    date.getMonth() + 1 !== parts[1] ||
    date.getDate() !== parts[2] ||
    date.getHours() !== parts[3] ||
    date.getMinutes() !== parts[4]
  ) {
    throw new Error("That local time does not exist. Choose another time.");
  }
  if (date.getTime() <= now)
    throw new Error("The end date and time must be in the future.");
  return {
    mode: "until",
    localDateTime,
    timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    utcOffsetMinutes: -date.getTimezoneOffset(),
  };
}

export function timedTaskInput(
  draft: TimedTaskDraft,
  recipientPubkey: string,
  originEventId: string | null,
  now = Date.now(),
  previous?: TimedTaskInput,
): TimedTaskInput {
  if (!draft.instruction.trim()) throw new Error("Enter an instruction.");
  if (new TextEncoder().encode(draft.instruction).length > 32_000)
    throw new Error("Keep the instruction within 32,000 bytes.");
  if (!draft.channelId) throw new Error("Choose a conversation.");
  let repetition: TimedTaskRepetition;
  if (draft.repeatMode === "count")
    repetition = {
      mode: "count",
      count: positiveInteger(draft.count, "Total runs"),
    };
  else if (draft.repeatMode === "until") {
    // Editing an unchanged cutoff preserves its saved zone, even after travel.
    repetition =
      previous?.repetition.mode === "until" &&
      previous.repetition.localDateTime === draft.localDateTime
        ? previous.repetition
        : localCutoff(draft.localDateTime, now);
  } else repetition = { mode: "forever" };
  const value = positiveInteger(draft.intervalValue, "Interval");
  if (value > 525_600) throw new Error("Interval must be at most 525,600.");
  if (repetition.mode === "count" && repetition.count > 4_294_967_295)
    throw new Error("Choose a smaller total run count.");
  const intervalMs =
    value *
    { minutes: 60_000, hours: 3_600_000, days: 86_400_000 }[draft.intervalUnit];
  if (!Number.isSafeInteger(intervalMs) || now + intervalMs > 8.64e15)
    throw new Error("Choose a shorter interval.");
  if (repetition.mode === "until") {
    const end =
      Date.parse(`${repetition.localDateTime}Z`) -
      repetition.utcOffsetMinutes * 60_000;
    if (end < now + intervalMs)
      throw new Error("The end time must allow at least one full interval.");
  }
  return {
    recipientPubkey,
    threadRootId: /^[0-9a-f]{64}$/.test(draft.destination) ? draft.destination : null,
    postToChannel: draft.destination === "channel",
    originEventId,
    channelId: draft.channelId,
    instruction: draft.instruction,
    interval: { value, unit: draft.intervalUnit },
    repetition,
  };
}

export function formatTimedTaskTime(value: number | null) {
  return value === null
    ? "—"
    : new Date(value).toLocaleString(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      });
}
