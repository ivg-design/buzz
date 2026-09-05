import * as React from "react";
import { useDmSidebarMetadata } from "@/features/sidebar/useDmSidebarMetadata";
import { useChannelMessagesQuery } from "@/features/messages/hooks";
import { useConversationOrganization } from "@/features/messages/organization/useConversationOrganization";
import { formatOrganizedMessages } from "@/features/messages/organization/formatOrganizedMessages";
import type { Channel } from "@/shared/api/types";

export function useTimedTaskDestinations(channels: Channel[], channelId: string, signer: string, relay: string) {
  const directMessages = React.useMemo(() => channels.filter(c => c.channelType === "dm"), [channels]);
  const { dmChannelLabels } = useDmSidebarMetadata({ currentPubkey: signer, directMessages });
  const channel = channels.find(c => c.id === channelId) ?? null;
  const messages = useChannelMessagesQuery(channel);
  const organization = useConversationOrganization(channel, relay, signer);
  const rows = formatOrganizedMessages([...(messages.data ?? []), ...organization.events, ...organization.supplementalEvents], channel, signer, null);
  const threads = rows.filter(row => !row.parentId).map(row => ({
    id: row.id,
    name: organization.state.metadata.get(row.id)?.title || row.body.replace(/\s+/g, " ").slice(0, 140) || "Untitled thread",
  }));
  return { dmChannelLabels, threads };
}
