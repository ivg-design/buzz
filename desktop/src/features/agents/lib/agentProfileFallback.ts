import type { UserProfileLookup } from "@/features/profile/lib/identity";
import type { ManagedAgent, RelayAgent } from "@/shared/api/types";
import { normalizePubkey } from "@/shared/lib/pubkey";

/**
 * Overlay names known by the local and relay agent directories onto profile
 * results. A managed agent remains nameable when its relay kind:0 profile is
 * missing or temporarily stale, while verified profile metadata still wins
 * when it is available.
 */
export function mergeAgentNamesIntoProfiles(
  profiles: UserProfileLookup,
  managedAgents: ManagedAgent[],
  relayAgents: RelayAgent[],
  currentPubkey?: string | null,
): UserProfileLookup {
  const merged = { ...profiles };
  for (const agent of relayAgents) {
    const key = normalizePubkey(agent.pubkey);
    merged[key] = {
      ...merged[key],
      displayName: merged[key]?.displayName || agent.name,
      avatarUrl: merged[key]?.avatarUrl ?? null,
      nip05Handle: merged[key]?.nip05Handle ?? null,
      isAgent: true,
    };
  }
  for (const agent of managedAgents) {
    const key = normalizePubkey(agent.pubkey);
    merged[key] = {
      ...merged[key],
      displayName: merged[key]?.displayName || agent.name,
      avatarUrl: merged[key]?.avatarUrl ?? agent.avatarUrl,
      nip05Handle: merged[key]?.nip05Handle ?? null,
      ownerPubkey: merged[key]?.ownerPubkey ?? currentPubkey ?? null,
      isAgent: true,
    };
  }
  return merged;
}
