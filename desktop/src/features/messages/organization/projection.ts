import type { TimelineMessage } from "@/features/messages/types";
import type { RelayEvent } from "@/shared/api/types";

export const ORGANIZATION_KIND = 40009;
export type ThreadMetadata = { title?: string; summary?: string };
export type OrganizationAction =
  | ({
      type: "group";
      message_ids: string[];
      thread_root_id: string;
    } & ThreadMetadata)
  | ({ type: "thread_metadata"; thread_root_id: string } & ThreadMetadata)
  | { type: "hide"; message_ids: string[]; hidden: boolean }
  | { type: "participants"; thread_root_id: string; agent_pubkeys: string[] }
  | { type: "undo"; change_event_id: string };
export type OrganizationRecord = {
  event: RelayEvent;
  action: OrganizationAction;
  undone: boolean;
};
type Ranked<T> = { value: T; rank: number };
export type OrganizationState = {
  records: OrganizationRecord[];
  groups: Map<string, Ranked<string>>;
  hidden: Map<string, Ranked<boolean>>;
  metadata: Map<string, ThreadMetadata>;
  participants: Map<string, string[]>;
};
const validId = (value: unknown): value is string =>
  typeof value === "string" && /^[0-9a-f]{64}$/.test(value);

/** Validate the wire shape defensively; channel/author authority is enforced at relay ingress. */
export function organizationAction(
  event: RelayEvent,
  channelId: string,
): OrganizationAction | null {
  if (
    event.kind !== ORGANIZATION_KIND ||
    event.tags.filter((t) => t[0] === "h").length !== 1 ||
    !event.tags.some((t) => t[0] === "h" && t[1] === channelId)
  )
    return null;
  try {
    const payload = JSON.parse(event.content);
    const a = payload.action;
    if (payload.version !== 1 || !a || typeof a !== "object") return null;
    const selection = () =>
      Array.isArray(a.message_ids) &&
      a.message_ids.length > 0 &&
      a.message_ids.length <= 100 &&
      a.message_ids.every(validId);
    const metadata = () =>
      (a.title === undefined ||
        (typeof a.title === "string" && a.title.length <= 160)) &&
      (a.summary === undefined ||
        (typeof a.summary === "string" && a.summary.length <= 8000));
    if (a.type === "undo" && validId(a.change_event_id)) return a;
    if (
      a.type === "participants" &&
      validId(a.thread_root_id) &&
      Array.isArray(a.agent_pubkeys) &&
      a.agent_pubkeys.length <= 100 &&
      a.agent_pubkeys.every(validId) &&
      new Set(a.agent_pubkeys).size === a.agent_pubkeys.length
    )
      return a;
    if (a.type === "hide" && selection() && typeof a.hidden === "boolean")
      return a;
    if (
      a.type === "group" &&
      selection() &&
      validId(a.thread_root_id) &&
      metadata()
    )
      return a;
    if (a.type === "thread_metadata" && validId(a.thread_root_id) && metadata())
      return a;
  } catch {
    /* Invalid relay content cannot become an organization operation. */
  }
  return null;
}

/** Replay immutable operations in signed event order; undo removes just its target. */
export function buildOrganizationState(
  events: RelayEvent[],
  channelId: string,
): OrganizationState {
  const records = [...new Map(events.map((e) => [e.id, e])).values()]
    .sort((a, b) => a.created_at - b.created_at || a.id.localeCompare(b.id))
    .flatMap((event) => {
      const action = organizationAction(event, channelId);
      return action ? [{ event, action, undone: false }] : [];
    });
  const undone = new Set(
    records.flatMap((r) =>
      r.action.type === "undo" ? [r.action.change_event_id] : [],
    ),
  );
  const state: OrganizationState = {
    records,
    groups: new Map(),
    hidden: new Map(),
    metadata: new Map(),
    participants: new Map(),
  };
  records.forEach((record, rank) => {
    record.undone = undone.has(record.event.id);
    if (record.undone) return;
    const a = record.action;
    if (a.type === "participants")
      state.participants.set(a.thread_root_id, [...a.agent_pubkeys]);
    if (a.type === "group") {
      for (const id of a.message_ids)
        state.groups.set(id, { value: a.thread_root_id, rank });
      state.groups.set(a.thread_root_id, { value: a.thread_root_id, rank });
    }
    if (a.type === "hide") {
      for (const id of a.message_ids)
        state.hidden.set(id, { value: a.hidden, rank });
    }
    if (a.type === "group" || a.type === "thread_metadata") {
      state.metadata.set(a.thread_root_id, {
        ...state.metadata.get(a.thread_root_id),
        ...(a.title !== undefined ? { title: a.title } : {}),
        ...(a.summary !== undefined ? { summary: a.summary } : {}),
      });
    }
  });
  return state;
}

/** Source references needed even when an organized conversation is outside the channel window. */
export function organizationReferenceIds(state: OrganizationState): string[] {
  return [
    ...new Set([
      ...state.groups.keys(),
      ...[...state.groups.values()].map((g) => g.value),
      ...state.hidden.keys(),
      ...state.metadata.keys(),
    ]),
  ];
}

/** Resolve chained moves without changing a source event or its NIP-10 tags. */
export function projectOrganization(
  messages: TimelineMessage[],
  state: OrganizationState,
) {
  if (!state.records.length)
    return { messages, hiddenMessages: [] as TimelineMessage[] };
  const byId = new Map(messages.map((m) => [m.id, m]));
  function newest<T>(id: string, map: Map<string, Ranked<T>>) {
    let winner: Ranked<T> | undefined;
    const visited = new Set<string>();
    let current: string | null | undefined = id;
    while (current && !visited.has(current)) {
      visited.add(current);
      const entry = map.get(current);
      if (entry && (!winner || entry.rank > winner.rank)) winner = entry;
      const message = byId.get(current);
      const parent = message?.parentId;
      if (parent && !visited.has(parent)) {
        // Root tags retain ancestry when an intermediate parent is not loaded yet.
        const rootEntry = message?.rootId ? map.get(message.rootId) : undefined;
        if (rootEntry && (!winner || rootEntry.rank > winner.rank))
          winner = rootEntry;
        current = parent;
      } else
        current =
          message?.rootId && message.rootId !== current ? message.rootId : null;
    }
    return winner;
  }
  const roots = new Map<string, string>();
  function root(id: string): string {
    const cached = roots.get(id);
    if (cached) return cached;
    let current = newest(id, state.groups)?.value ?? byId.get(id)?.rootId ?? id;
    const visited = new Set([id]);
    while (!visited.has(current)) {
      visited.add(current);
      const next = newest(current, state.groups)?.value;
      if (!next || next === current) break;
      current = next;
    }
    roots.set(id, current);
    return current;
  }
  const hiddenMessages = messages.filter(
    (m) => newest(m.id, state.hidden)?.value === true,
  );
  const hiddenIds = new Set(hiddenMessages.map((m) => m.id));
  function visibleRoot(message: TimelineMessage) {
    const resolved = root(message.id);
    if (!hiddenIds.has(resolved)) return resolved;
    let candidate = message.id;
    let parent = message.parentId;
    const visited = new Set([candidate]);
    while (parent && !hiddenIds.has(parent) && !visited.has(parent)) {
      visited.add(parent);
      candidate = parent;
      parent = byId.get(parent)?.parentId;
    }
    return candidate;
  }
  const projected = messages
    .filter((m) => !hiddenIds.has(m.id))
    .map((message) => {
      // A specifically restored descendant of a hidden root must remain reachable.
      const rootId = visibleRoot(message);
      const originalParent = message.parentId;
      const parentId =
        rootId === message.id
          ? null
          : originalParent &&
              !hiddenIds.has(originalParent) &&
              (byId.get(originalParent)
                ? visibleRoot(byId.get(originalParent) as TimelineMessage)
                : root(originalParent)) === rootId
            ? originalParent
            : rootId;
      return {
        ...message,
        organizationMoved:
          state.groups.size > 0 &&
          newest(message.id, state.groups) !== undefined,
        rootId: parentId ? rootId : null,
        parentId,
        depth: parentId ? 1 : 0,
      };
    });
  const projectedById = new Map(projected.map((m) => [m.id, m]));
  for (const message of projected) {
    const visited = new Set([message.id]);
    let parent = message.parentId;
    let depth = 0;
    while (parent && !visited.has(parent)) {
      visited.add(parent);
      depth += 1;
      parent = projectedById.get(parent)?.parentId ?? null;
    }
    message.depth = depth;
  }
  return { messages: projected, hiddenMessages };
}
