import type { RelayEvent } from "@/shared/api/types";
import type { RelaySubscriptionFilter } from "@/shared/api/relayClientShared";
import { ORGANIZATION_KIND } from "./projection";

export const mergeOrganizationEvents = (a: RelayEvent[], b: RelayEvent[]) =>
  [...new Map([...a, ...b].map((event) => [event.id, event])).values()].sort(
    (x, y) => x.created_at - y.created_at || x.id.localeCompare(y.id),
  );

/** Composite paging retains changes created in the same second, including undo records. */
export async function loadOrganizationHistory(
  channelId: string,
  fetchEvents: (filter: RelaySubscriptionFilter) => Promise<RelayEvent[]>,
  extra: Partial<RelaySubscriptionFilter> = {},
) {
  let events: RelayEvent[] = [];
  let cursor: RelayEvent | undefined;
  for (let page = 0; page < 500; page += 1) {
    const batch = await fetchEvents({
      kinds: [ORGANIZATION_KIND],
      "#h": [channelId],
      limit: 200,
      ...extra,
      ...(cursor ? { until: cursor.created_at, before_id: cursor.id } : {}),
    });
    events = mergeOrganizationEvents(events, batch);
    if (batch.length < 200) return events;
    // Relay pages sort timestamp DESC, id ASC: the final row is the cursor.
    const oldest = [...batch]
      .sort((a, b) => b.created_at - a.created_at || a.id.localeCompare(b.id))
      .at(-1);
    if (!oldest) return events;
    if (oldest.id === cursor?.id)
      throw new Error(
        "Organization history did not advance. Retry to reload it.",
      );
    cursor = oldest;
  }
  throw new Error("Organization history exceeded the supported page limit.");
}
