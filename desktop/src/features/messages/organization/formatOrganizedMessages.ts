import { formatTimelineMessages } from "@/features/messages/lib/formatTimelineMessages";
import { buildOrganizationState, projectOrganization } from "./projection";

/** Both channel and independent-thread rendering apply this same immutable overlay. */
export function formatOrganizedMessages(
  ...args: Parameters<typeof formatTimelineMessages>
) {
  const [events, channel] = args;
  const messages = formatTimelineMessages(...args);
  const channelId =
    channel?.id ??
    events
      .find((e) => e.tags.some((t) => t[0] === "h"))
      ?.tags.find((t) => t[0] === "h")?.[1] ??
    "";
  return projectOrganization(
    messages,
    buildOrganizationState(events, channelId),
  ).messages;
}
