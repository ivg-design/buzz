import type { Channel } from "@/shared/api/types";

/** Keep the existing group distinction when membership later shrinks to two. */
export function timedTaskConversationLabel(
  channel: Pick<Channel, "channelType" | "name">,
  resolvedDmLabel?: string,
) {
  if (channel.channelType !== "dm") return `#${channel.name}`;
  const participants = resolvedDmLabel || channel.name;
  return /^group dm\s*(\(\d+\))?$/i.test(channel.name.trim())
    ? `Group: ${participants}`
    : participants;
}
