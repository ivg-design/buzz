/** Prefer this conversation's active work; otherwise expose explicit choices. */
export function resolvePopoverStopChannels(
  isLive: boolean,
  channels: readonly { channelId: string }[],
  contextChannelId?: string | null,
): string[] {
  if (!isLive) return [];
  const active = [...new Set(channels.map((channel) => channel.channelId))];
  return contextChannelId && active.includes(contextChannelId)
    ? [contextChannelId]
    : active;
}
