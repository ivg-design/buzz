import { KIND_FORUM_POST, KIND_FORUM_COMMENT } from "@/shared/constants/kinds";
import type {
  ForumPost,
  ForumThreadResponse,
  RelayEvent,
  ThreadReply,
} from "@/shared/api/types";
import { getThreadReference } from "@/features/messages/lib/threading";
import { projectOrganization, type OrganizationState } from "./projection";
import type { TimelineMessage } from "@/features/messages/types";

/** Reuse organization placement for the forum's existing post/reply view models. */
export function projectForumOrganization(
  posts: ForumPost[],
  thread: ForumThreadResponse | undefined,
  sources: RelayEvent[],
  state: OrganizationState,
  channelId: string,
  selectedPostId: string | null,
) {
  if (!state.records.length) return { posts, thread };
  const originals = new Map<string, ForumPost | ThreadReply>([
    ...sources.map(
      (event) =>
        [
          event.id,
          {
            eventId: event.id,
            pubkey: event.pubkey,
            content: event.content,
            kind: event.kind,
            createdAt: event.created_at,
            channelId,
            tags: event.tags,
            threadSummary: null,
          },
        ] as const,
    ),
    ...posts.map((post) => [post.eventId, post] as const),
    ...(thread
      ? [
          [thread.post.eventId, thread.post] as const,
          ...thread.replies.map((reply) => [reply.eventId, reply] as const),
        ]
      : []),
  ]);
  const rows: TimelineMessage[] = [...originals.values()].map((item) => {
    const ref = getThreadReference(item.tags);
    return {
      id: item.eventId,
      pubkey: item.pubkey,
      author: item.pubkey,
      time: "",
      createdAt: item.createdAt,
      body: item.content,
      kind: item.kind,
      tags: item.tags,
      parentId: ref.parentId,
      rootId: ref.rootId,
      depth: "depth" in item ? item.depth : 0,
    };
  });
  const visible = projectOrganization(rows, state).messages;
  const projectedPosts = visible
    .filter((row) => !row.parentId)
    .map((row): ForumPost => {
      const original = originals.get(row.id);
      const replies = visible.filter(
        (reply) => reply.rootId === row.id && reply.parentId,
      );
      return {
        eventId: row.id,
        pubkey: row.pubkey ?? "",
        content: row.body,
        kind: row.kind ?? KIND_FORUM_POST,
        createdAt: row.createdAt,
        channelId,
        tags: row.tags ?? [],
        threadSummary:
          state.groups.has(row.id) || state.hidden.size
            ? {
                replyCount: replies.length,
                descendantCount: replies.length,
                lastReplyAt: replies.at(-1)?.createdAt ?? null,
                participants: [
                  ...new Set(
                    replies.flatMap((r) => (r.pubkey ? [r.pubkey] : [])),
                  ),
                ],
              }
            : original && "threadSummary" in original
              ? original.threadSummary
              : null,
      };
    })
    .sort(
      (a, b) => b.createdAt - a.createdAt || a.eventId.localeCompare(b.eventId),
    );
  let post = projectedPosts.find((item) => item.eventId === selectedPostId);
  let threadRows = visible;
  let originalView = false;
  // Existing links keep their original root. A moved/hidden root is still a
  // readable original subthread, even though it is absent from the post list.
  const selectedOriginal = selectedPostId
    ? originals.get(selectedPostId)
    : undefined;
  if (!post && selectedOriginal) {
    originalView = true;
    post = {
      ...selectedOriginal,
      threadSummary:
        "threadSummary" in selectedOriginal
          ? selectedOriginal.threadSummary
          : null,
    };
    threadRows = rows;
  }
  const replies: ThreadReply[] = threadRows
    .filter((row) => row.rootId === selectedPostId && row.parentId)
    .map((row) => ({
      eventId: row.id,
      pubkey: row.pubkey ?? "",
      content: row.body,
      kind: row.kind ?? KIND_FORUM_COMMENT,
      createdAt: row.createdAt,
      channelId,
      tags: row.tags ?? [],
      parentEventId: row.parentId ?? null,
      rootEventId: row.rootId ?? null,
      depth: row.depth,
    }))
    .sort(
      (a, b) => a.createdAt - b.createdAt || a.eventId.localeCompare(b.eventId),
    );
  return {
    posts: projectedPosts,
    originalView,
    thread: post
      ? { post, replies, totalReplies: replies.length, nextCursor: null }
      : undefined,
  };
}
