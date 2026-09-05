import type { Channel, ChannelMember, RelayAgent } from "@/shared/api/types";
import type { OrganizationState } from "./projection";

const key = (value: string) => value.toLowerCase();
const validPubkey = (value: string | null) =>
  value !== null && /^[0-9a-f]{64}$/i.test(value);

/** Open rooms use enrollment; private rooms and DMs require direct membership. */
export function eligibleThreadAgents(
  channel: Channel,
  agents: readonly RelayAgent[],
  members: readonly ChannelMember[],
): RelayAgent[] {
  const memberKeys = new Set(members.map((member) => key(member.pubkey)));
  const requiresMembership =
    channel.channelType === "dm" || channel.visibility === "private";
  return [
    ...new Map(
      agents
        .filter(
          (agent) =>
            validPubkey(agent.pubkey) &&
            validPubkey(agent.ownerPubkey) &&
            (!requiresMembership || memberKeys.has(key(agent.pubkey))),
        )
        .map((agent) => [
          key(agent.pubkey),
          { ...agent, pubkey: key(agent.pubkey) },
        ]),
    ).values(),
  ].sort(
    (a, b) => a.name.localeCompare(b.name) || a.pubkey.localeCompare(b.pubkey),
  );
}

/** Grouped subthreads use their destination's explicit participant policy. */
export function participantThreadRoot(
  threadRootId: string,
  state: OrganizationState,
): string {
  const visited = new Set<string>();
  let current = threadRootId;
  while (!visited.has(current)) {
    visited.add(current);
    const next = state.groups.get(current)?.value;
    if (!next || next === current) return current;
    current = next;
  }
  return current;
}

/** One checkbox change is one complete, deterministic participant-list write. */
export function toggleThreadParticipant(
  current: readonly string[],
  pubkey: string,
  checked: boolean,
): string[] {
  const selected = new Set(current);
  if (checked) selected.add(pubkey);
  else selected.delete(pubkey);
  return [...selected].sort();
}
