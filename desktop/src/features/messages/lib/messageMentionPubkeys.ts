import type { Channel } from "@/shared/api/types";
import { normalizePubkey } from "@/shared/lib/pubkey";

/**
 * Return the semantic recipients for an outgoing message.
 *
 * Stream messages notify only explicit mentions. A DM addresses every other
 * participant, so it must carry recipient `p` tags even when the composer text
 * contains no `@mention`. Agent harnesses and human notification subscriptions
 * both rely on those tags.
 */
export function messageMentionPubkeys(
  channel: Channel,
  senderPubkey: string,
  explicitMentions: readonly string[] = [],
  rosterPubkeys: readonly string[] = [],
): string[] {
  const candidates =
    channel.channelType === "dm"
      ? [
          ...explicitMentions,
          ...channel.memberPubkeys,
          ...channel.participantPubkeys,
          ...rosterPubkeys,
        ]
      : explicitMentions;
  const sender = normalizePubkey(senderPubkey);

  return [...new Set(candidates.map(normalizePubkey))].filter(
    (pubkey) => pubkey.length > 0 && pubkey !== sender,
  );
}

function dmProjectionNeedsRoster(channel: Channel, senderPubkey: string) {
  const sender = normalizePubkey(senderPubkey);
  const projectedParticipants = new Set(
    [...channel.memberPubkeys, ...channel.participantPubkeys]
      .map(normalizePubkey)
      .filter(Boolean),
  );
  const hasRecipient = [...projectedParticipants].some(
    (pubkey) => pubkey !== sender,
  );

  return (
    !hasRecipient ||
    (channel.memberCount > 0 &&
      projectedParticipants.size < channel.memberCount)
  );
}

/**
 * Resolve recipients for the actual send path.
 *
 * DM metadata projections can be absent from a persisted channel snapshot.
 * The kind:39002 member roster is the authoritative fallback and shares the
 * same query cache as the DM member UI. DM participant sets are immutable, so
 * a complete signed projection can stay on the fast path. Streams never load
 * a roster and retain explicit-mention-only semantics.
 */
export async function resolveMessageRecipientPubkeys(
  channel: Channel,
  senderPubkey: string,
  explicitMentions: readonly string[] = [],
  loadDmRosterPubkeys: (channelId: string) => Promise<readonly string[]>,
): Promise<string[]> {
  const needsRoster =
    channel.channelType === "dm" &&
    dmProjectionNeedsRoster(channel, senderPubkey);
  const rosterPubkeys = needsRoster
    ? await loadDmRosterPubkeys(channel.id)
    : [];
  const recipients = messageMentionPubkeys(
    channel,
    senderPubkey,
    explicitMentions,
    rosterPubkeys,
  );

  if (needsRoster && recipients.length === 0) {
    throw new Error(
      "Direct message participants are unavailable. Refresh the conversation and try again.",
    );
  }

  return recipients;
}
