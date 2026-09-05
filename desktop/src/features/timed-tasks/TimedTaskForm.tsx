import * as React from "react";
import { Button } from "@/shared/ui/button";
import { Input } from "@/shared/ui/input";
import { Textarea } from "@/shared/ui/textarea";
import type { TimedTaskDraft } from "./form";

const selectClass =
  "h-9 min-w-0 rounded-lg border border-input/40 bg-background px-3 text-sm focus-visible:outline-hidden focus-visible:ring-1 focus-visible:ring-ring";

/** The same editor is used to create and edit persisted schedules. */
export function TimedTaskForm({
  draft,
  onChange,
  onSubmit,
  onCancel,
  channels,
  timeZone,
  pending,
  editing,
  error,
}: {
  draft: TimedTaskDraft;
  onChange: (draft: TimedTaskDraft) => void;
  onSubmit: () => void;
  onCancel: () => void;
  channels: { id: string; name: string }[];
  timeZone: string;
  pending: boolean;
  editing: boolean;
  error: string | null;
}) {
  const id = React.useId();
  const set = <K extends keyof TimedTaskDraft>(
    key: K,
    value: TimedTaskDraft[K],
  ) => onChange({ ...draft, [key]: value });
  return (
    <form
      className="flex flex-col gap-4"
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit();
      }}
    >
      <fieldset disabled={pending} className="flex min-w-0 flex-col gap-4">
        <div className="grid gap-1.5">
          <label className="text-sm font-medium" htmlFor={`${id}-instruction`}>
            Instruction
          </label>
          <Textarea
            id={`${id}-instruction`}
            value={draft.instruction}
            onChange={(event) => set("instruction", event.target.value)}
            rows={4}
            required
            placeholder="What should this agent do?"
            className="resize-y"
          />
        </div>
        <div className="grid gap-1.5">
          <label className="text-sm font-medium" htmlFor={`${id}-channel`}>
            Conversation
          </label>
          <select
            id={`${id}-channel`}
            value={draft.channelId}
            onChange={(event) => set("channelId", event.target.value)}
            className={selectClass}
            disabled={editing}
            required
          >
            <option value="" disabled>
              Choose a conversation
            </option>
            {channels.map((channel) => (
              <option key={channel.id} value={channel.id}>
                {channel.name}
              </option>
            ))}
          </select>
          <p className="text-xs text-muted-foreground">
            Instructions and replies stay together in one task thread.
          </p>
        </div>
        <div className="grid gap-1.5">
          <label className="text-sm font-medium" htmlFor={`${id}-interval`}>
            Send every
          </label>
          <div className="flex gap-2">
            <Input
              id={`${id}-interval`}
              className="w-24"
              type="number"
              min={1}
              step={1}
              required
              value={draft.intervalValue}
              onChange={(event) => set("intervalValue", event.target.value)}
            />
            <select
              aria-label="Interval unit"
              className={`${selectClass} flex-1`}
              value={draft.intervalUnit}
              onChange={(event) =>
                set(
                  "intervalUnit",
                  event.target.value as TimedTaskDraft["intervalUnit"],
                )
              }
            >
              <option value="minutes">Minutes</option>
              <option value="hours">Hours</option>
              <option value="days">Days</option>
            </select>
          </div>
        </div>
        <div className="grid gap-1.5">
          <label className="text-sm font-medium" htmlFor={`${id}-repeat`}>
            Repeat
          </label>
          <select
            id={`${id}-repeat`}
            className={selectClass}
            value={draft.repeatMode}
            onChange={(event) =>
              set(
                "repeatMode",
                event.target.value as TimedTaskDraft["repeatMode"],
              )
            }
          >
            <option value="forever">Until I cancel</option>
            <option value="count">A total number of times</option>
            <option value="until">Until a date and time</option>
          </select>
        </div>
        {draft.repeatMode === "count" ? (
          <div className="grid gap-1.5">
            <label className="text-sm font-medium" htmlFor={`${id}-count`}>
              Total runs
            </label>
            <Input
              id={`${id}-count`}
              type="number"
              min={1}
              step={1}
              required
              value={draft.count}
              onChange={(event) => set("count", event.target.value)}
            />
            {editing ? (
              <p className="text-xs text-muted-foreground">
                Includes instructions already delivered.
              </p>
            ) : null}
          </div>
        ) : null}
        {draft.repeatMode === "until" ? (
          <div className="grid gap-1.5">
            <label className="text-sm font-medium" htmlFor={`${id}-end`}>
              End date and time
            </label>
            <Input
              id={`${id}-end`}
              type="datetime-local"
              step={60}
              required
              value={draft.localDateTime}
              onChange={(event) => set("localDateTime", event.target.value)}
            />
          </div>
        ) : null}
      </fieldset>
      <p className="text-xs leading-relaxed text-muted-foreground">
        Timezone: {timeZone}. First run after one interval; a day is 24 hours.
        Buzz must be running on this host. Missed runs are combined. Offline
        agents wait; delivered prompts use the agent’s normal queue.
      </p>
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      <div className="flex justify-end gap-2">
        <Button
          type="button"
          variant="ghost"
          disabled={pending}
          onClick={onCancel}
        >
          Back
        </Button>
        <Button type="submit" disabled={pending}>
          {pending ? "Saving…" : editing ? "Save changes" : "Add timed task"}
        </Button>
      </div>
    </form>
  );
}
