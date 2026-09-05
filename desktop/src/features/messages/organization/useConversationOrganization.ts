import { KIND_FORUM_POST, KIND_FORUM_COMMENT } from "@/shared/constants/kinds";
import * as React from "react";
import {
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { relayClient } from "@/shared/api/relayClient";
import { getEventsByIds, invokeTauri } from "@/shared/api/tauri";
import type { Channel, RelayEvent } from "@/shared/api/types";
import { getThreadReference } from "@/features/messages/lib/threading";
import { useThreadRepliesForRoots } from "@/features/messages/useThreadReplies";
import {
  buildOrganizationState,
  ORGANIZATION_KIND,
  organizationReferenceIds,
  type OrganizationAction,
} from "./projection";
import { loadOrganizationHistory, mergeOrganizationEvents } from "./history";

const EMPTY_EVENTS: RelayEvent[] = [];
const combineForumSources = (
  results: readonly {
    data?: RelayEvent[];
    error: unknown;
    isPending: boolean;
  }[],
) => ({
  events: mergeOrganizationEvents(
    [],
    results.flatMap((query) => query.data ?? []),
  ),
  error: results.find((query) => query.error)?.error,
  isPending: results.some((query) => query.isPending),
});

/** Channel-scoped live history, source hydration, and ordinary authenticated undo. */
export function useConversationOrganization(
  channel: Channel | null,
  relayUrl?: string,
  signerPubkey?: string,
) {
  const channelId = channel?.id;
  const client = useQueryClient();
  const key = React.useMemo(
    () => ["conversation-organization", relayUrl, signerPubkey, channelId],
    [channelId, relayUrl, signerPubkey],
  );
  const [subscriptionError, setSubscriptionError] =
    React.useState<unknown>(null);
  const query = useQuery({
    queryKey: key,
    enabled: !!channelId,
    queryFn: async () => {
      if (!channelId) return [];
      const events = await loadOrganizationHistory(channelId, (filter) =>
        relayClient.fetchEvents(filter),
      );
      return mergeOrganizationEvents(
        events,
        client.getQueryData<RelayEvent[]>(key) ?? [],
      );
    },
    staleTime: 30_000,
  });
  React.useEffect(() => {
    if (!channelId) return;
    let disposed = false;
    let unsubscribe: (() => void) | undefined;
    setSubscriptionError(null);
    // Register before backfill settles; live events and history merge by event id.
    void relayClient
      .subscribeLive(
        {
          kinds:
            channel?.channelType === "forum"
              ? [ORGANIZATION_KIND, KIND_FORUM_POST, KIND_FORUM_COMMENT]
              : [ORGANIZATION_KIND],
          "#h": [channelId],
          limit: 200,
        },
        (event) => {
          if (event.kind !== ORGANIZATION_KIND) {
            if (!disposed)
              void client.invalidateQueries({
                queryKey: [...key, "forum-subtree"],
              });
            return;
          }
          if (!disposed)
            client.setQueryData<RelayEvent[]>(key, (current = []) =>
              mergeOrganizationEvents(current, [event]),
            );
        },
        () => {
          if (!disposed) {
            setSubscriptionError(null);
            void client.invalidateQueries({ queryKey: key });
          }
        },
      )
      .then((stop) => {
        if (disposed) stop();
        else unsubscribe = stop;
      })
      .catch((error) => {
        if (!disposed) setSubscriptionError(error);
      });
    const stopReconnect = relayClient.subscribeToReconnects(() => {
      if (!disposed) void client.invalidateQueries({ queryKey: key });
    });
    return () => {
      disposed = true;
      unsubscribe?.();
      stopReconnect();
    };
  }, [channelId, channel?.channelType, client, key]);
  const events = query.data ?? EMPTY_EVENTS;
  const state = React.useMemo(
    () => buildOrganizationState(events, channelId ?? ""),
    [events, channelId],
  );
  const referenceIds = React.useMemo(
    () => organizationReferenceIds(state),
    [state],
  );
  const references = useQuery({
    queryKey: [...key, "sources", referenceIds],
    enabled: !!channelId && referenceIds.length > 0,
    queryFn: async () => {
      if (referenceIds.length > 10_000)
        throw new Error(
          "This channel has too many organization references to load at once.",
        );
      const result: RelayEvent[] = [];
      for (let i = 0; i < referenceIds.length; i += 100)
        result.push(...(await getEventsByIds(referenceIds.slice(i, i + 100))));
      return result.filter((e) =>
        e.tags.some((t) => t[0] === "h" && t[1] === channelId),
      );
    },
    staleTime: 60_000,
  });
  const sourceEvents = references.data ?? EMPTY_EVENTS;
  const subtreeRoots = React.useMemo(
    () =>
      [
        ...new Set(
          sourceEvents
            .filter(
              (event) =>
                state.groups.has(event.id) || state.hidden.has(event.id),
            )
            .map((event) => getThreadReference(event.tags).rootId ?? event.id),
        ),
      ].sort(),
    [state, sourceEvents],
  );
  const subtrees = useThreadRepliesForRoots(channel, subtreeRoots);
  const forumSubtrees = useQueries({
    queries: subtreeRoots.map((rootId) => ({
      queryKey: [...key, "forum-subtree", rootId],
      enabled: !!channelId && channel?.channelType === "forum",
      queryFn: () =>
        loadOrganizationHistory(
          channelId ?? "",
          (filter) => relayClient.fetchEvents(filter),
          { kinds: [KIND_FORUM_POST, KIND_FORUM_COMMENT], "#e": [rootId] },
        ),
      staleTime: 30_000,
    })),
    combine: combineForumSources,
  });
  const supplementalEvents = React.useMemo(
    () =>
      mergeOrganizationEvents(
        sourceEvents,
        channel?.channelType === "forum"
          ? forumSubtrees.events
          : subtrees.events,
      ),
    [sourceEvents, subtrees.events, forumSubtrees.events, channel?.channelType],
  );
  const mutation = useMutation({
    mutationFn: async (action: OrganizationAction) => {
      if (!channelId || !relayUrl || !signerPubkey)
        throw new Error("Connect to this conversation before organizing it.");
      const queryKey = key;
      const event = await invokeTauri<RelayEvent>(
        "apply_conversation_organization",
        {
          channelId,
          action,
          expectedRelayUrl: relayUrl,
          expectedSignerPubkey: signerPubkey,
        },
      );
      return { event, queryKey };
    },
    onSuccess: ({ event, queryKey }) =>
      client.setQueryData<RelayEvent[]>(queryKey, (current = []) =>
        mergeOrganizationEvents(current, [event]),
      ),
  });
  return {
    state,
    events,
    supplementalEvents,
    error:
      query.error ??
      references.error ??
      subtrees.error ??
      forumSubtrees.error ??
      subscriptionError ??
      mutation.error,
    isPending:
      query.isPending ||
      (referenceIds.length > 0 && references.isPending) ||
      (channel?.channelType === "forum"
        ? forumSubtrees.isPending
        : subtrees.isPending),
    isSaving: mutation.isPending,
    apply: mutation.mutateAsync,
    retry: () => {
      void query.refetch();
      if (referenceIds.length) void references.refetch();
      subtrees.refetch();
      void client.invalidateQueries({ queryKey: [...key, "forum-subtree"] });
    },
  };
}
