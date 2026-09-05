export type TimedTaskInterval = {
  value: number;
  unit: "minutes" | "hours" | "days";
};

export type TimedTaskRepetition =
  | { mode: "forever" }
  | { mode: "count"; count: number }
  | {
      mode: "until";
      localDateTime: string;
      timeZone: string;
      /** Minutes east of UTC at the chosen local cutoff. */
      utcOffsetMinutes: number;
    };

export type TimedTaskInput = {
  recipientPubkey: string;
  recipientName?: string;
  threadRootId?: string | null;
  postToChannel?: boolean;
  channelId: string;
  originEventId: string | null;
  instruction: string;
  interval: TimedTaskInterval;
  repetition: TimedTaskRepetition;
};

export type TimedTaskStatus = "active" | "paused" | "cancelled" | "completed";

export type TimedTask = TimedTaskInput & {
  id: string;
  ownerPubkey: string;
  relayUrl: string;
  threadId: string | null;
  rootPublished: boolean;
  status: TimedTaskStatus;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number | null;
  lastDeliveredAt: number | null;
  deliveredCount: number;
  missedCount: number;
  lastError: string | null;
  deliveryState: "idle" | "waiting_offline" | "pending" | "delivered";
};

/** Scope captured when the form opens; backend rejects stale community actions. */
export type TimedTaskScope = {
  expectedRelayUrl: string;
  expectedSignerPubkey: string;
};
