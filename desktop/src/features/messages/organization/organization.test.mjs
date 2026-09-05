import assert from "node:assert/strict";
import test from "node:test";
import { formatOrganizedMessages } from "./formatOrganizedMessages.ts";
import {
  buildIndependentThreadPanel,
  buildIndependentThreadPanelForLink,
} from "../lib/independentThreadPanel.ts";
import { buildMainTimelineEntries } from "../lib/threadPanel.ts";
import { buildOrganizationState } from "./projection.ts";
import { loadOrganizationHistory, mergeOrganizationEvents } from "./history.ts";
import { projectForumOrganization } from "./forumProjection.ts";

const id = (n) => n.toString(16).padStart(64, "0");
const channel = { id: "fixture-channel" };
const author = id(900);
function message(n, parent = null, root = parent, content = `Message ${n}`) {
  return {
    id: id(n),
    kind: 9,
    pubkey: author,
    created_at: n,
    content,
    sig: "fixture",
    tags: [
      ["h", channel.id],
      ...(parent
        ? [
            ["e", id(root), "", "root"],
            ["e", id(parent), "", "reply"],
          ]
        : []),
    ],
  };
}
function change(n, action, at = n) {
  return {
    ...message(n),
    kind: 40009,
    created_at: at,
    content: JSON.stringify({ version: 1, action }),
  };
}
const format = (events) =>
  formatOrganizedMessages(events, channel, author, null);
const group = (n, selected, root, extra = {}) =>
  change(n, {
    type: "group",
    message_ids: selected.map(id),
    thread_root_id: id(root),
    ...extra,
  });

test("production formatter moves replies, preserves source identity, links, and attachments", () => {
  const source = message(
    2,
    null,
    null,
    "[Design](https://example.org/design) attachment.png",
  );
  source.tags.push([
    "imeta",
    "url https://example.org/attachment.png",
    "m image/png",
  ]);
  const events = [
    message(1),
    source,
    message(3, 2),
    message(4, 3, 2),
    group(10, [2], 1),
  ];
  const before = JSON.stringify(events);
  const rows = format(events);
  assert.deepEqual(
    rows.map((m) => [m.id, m.parentId, m.depth]),
    [
      [id(1), null, 0],
      [id(2), id(1), 1],
      [id(3), id(2), 2],
      [id(4), id(3), 3],
    ],
  );
  assert.equal(rows[1].body, source.content);
  assert.deepEqual(rows[1].tags, source.tags);
  assert.equal(rows[1].pubkey, source.pubkey);
  assert.equal(rows[1].createdAt, source.created_at);
  assert.equal(JSON.stringify(events), before);
  assert.deepEqual(
    buildMainTimelineEntries(rows, new Set()).map((e) => e.message.id),
    [id(1)],
  );
});

test("independent thread displays moved subtree including concurrent new replies", () => {
  const root = message(1),
    source = message(2),
    operation = group(10, [2], 1);
  const replies = [source, message(3, 2), message(11, 3, 2)];
  const result = buildIndependentThreadPanel(
    [root, source, operation],
    replies,
    root.id,
    null,
    new Set(),
    channel,
    author,
    null,
  );
  assert.equal(result.threadHead.id, root.id);
  assert.deepEqual(
    result.messages.filter((m) => m.parentId).map((m) => m.rootId),
    [root.id, root.id, root.id],
  );
  assert.equal(result.totalReplyCount, 3);
});

test("undo removes its operation while keeping unrelated concurrent edits and new messages", () => {
  const operations = [
    group(10, [2], 1, { title: "Design" }),
    change(11, {
      type: "thread_metadata",
      thread_root_id: id(1),
      summary: "Final summary",
    }),
    change(12, { type: "undo", change_event_id: id(10) }),
  ];
  const events = [
    message(1),
    message(2),
    message(3, 2),
    message(15, 2),
    ...operations,
  ];
  const state = buildOrganizationState(events, channel.id);
  assert.deepEqual(state.metadata.get(id(1)), { summary: "Final summary" });
  assert.equal(format(events).find((m) => m.id === id(2)).parentId, null);
  assert.equal(format(events).find((m) => m.id === id(15)).parentId, id(2));
});

test("hide applies descendants, later restore and undo preserve other hidden branches", () => {
  const sources = [message(1), message(2, 1), message(3, 2, 1), message(4)];
  const hide = change(10, {
    type: "hide",
    message_ids: [id(1), id(4)],
    hidden: true,
  });
  assert.deepEqual(format([...sources, hide]), []);
  const restore = change(11, {
    type: "hide",
    message_ids: [id(2)],
    hidden: false,
  });
  const restored = format([...sources, hide, restore]);
  assert.deepEqual(
    restored.map((m) => m.id),
    [id(2), id(3)],
  );
  assert.equal(
    restored[0].parentId,
    null,
    "restored child remains reachable while its root is hidden",
  );
  assert.equal(
    restored[1].parentId,
    id(2),
    "restored descendants retain their reply relationships",
  );
  const undo = change(12, { type: "undo", change_event_id: restore.id });
  assert.deepEqual(format([...sources, hide, restore, undo]), []);
});

test("chained moves and root detach remain coherent without cycles", () => {
  const sources = [message(1), message(2), message(3), message(4, 1)];
  const moved = format([...sources, group(10, [1], 2), group(11, [2], 3)]);
  assert.equal(moved.find((m) => m.id === id(1)).rootId, id(3));
  assert.equal(moved.find((m) => m.id === id(4)).rootId, id(3));
  const detached = format([...sources, group(10, [1], 2), group(11, [2], 1)]);
  assert.equal(detached.find((m) => m.id === id(1)).parentId, null);
  assert.equal(detached.find((m) => m.id === id(2)).rootId, id(1));
});

test("latest ancestor operation wins while newer child selections override", () => {
  const sources = [message(1), message(2), message(3, 2), message(4)];
  const rows = format([...sources, group(10, [3], 4), group(11, [2], 1)]);
  assert.equal(rows.find((m) => m.id === id(3)).rootId, id(1));
  const overridden = format([...sources, group(10, [2], 1), group(11, [3], 4)]);
  assert.equal(overridden.find((m) => m.id === id(3)).parentId, id(4));
});

test("malformed and cross-channel operations cannot change the view", () => {
  const forged = group(10, [2], 1);
  forged.tags = [["h", "other-channel"]];
  const malformed = change(11, {
    type: "hide",
    message_ids: ["oops"],
    hidden: true,
  });
  assert.equal(format([message(1), message(2), forged, malformed]).length, 2);
  assert.equal(
    buildOrganizationState([forged, malformed], channel.id).records.length,
    0,
  );
});

test("same-second replay is deterministic regardless of network input order", () => {
  const a = group(10, [2], 1),
    b = group(11, [2], 3);
  a.created_at = b.created_at = 50;
  const sources = [message(1), message(2), message(3)];
  assert.deepEqual(format([...sources, b, a]), format([...sources, a, b]));
  assert.equal(format([...sources, a, b])[1].rootId, id(3));
});

test("composite history cursor retains more than one page created in the same second", async () => {
  const changes = Array.from({ length: 405 }, (_, i) =>
    change(
      i + 100,
      {
        type: "thread_metadata",
        thread_root_id: id(1),
        title: `Revision ${i}`,
      },
      1000,
    ),
  );
  const filters = [];
  const result = await loadOrganizationHistory(channel.id, async (filter) => {
    filters.push(filter);
    return changes
      .filter((e) => !filter.before_id || e.id > filter.before_id)
      .slice(0, filter.limit);
  });
  assert.equal(result.length, 405);
  assert.equal(filters[1].before_id, changes[199].id);
  assert.equal(filters[2].before_id, changes[399].id);
  assert.equal(
    mergeOrganizationEvents(result, [
      changes[0],
      change(999, { type: "undo", change_event_id: changes[0].id }),
    ]).length,
    406,
  );
});

test("an explicit move suppresses original broadcast duplication without editing signed tags", () => {
  const broadcast = message(2, 3);
  broadcast.tags.push(["broadcast"]);
  const originalTags = structuredClone(broadcast.tags);
  const rows = format([message(1), message(3), broadcast, group(10, [2], 1)]);
  assert.equal(rows.find((m) => m.id === broadcast.id).organizationMoved, true);
  assert.deepEqual(rows.find((m) => m.id === broadcast.id).tags, originalTags);
  assert.ok(
    !buildMainTimelineEntries(rows, new Set()).some(
      (e) => e.message.id === broadcast.id,
    ),
  );
});

test("forum posts and replies use the same reversible projection without changing content", () => {
  const sources = [message(1), message(2), message(3, 2), message(4, 3, 2)].map(
    (event) => ({
      ...event,
      kind: event.id === id(1) || event.id === id(2) ? 45001 : 45003,
    }),
  );
  const posts = sources.slice(0, 2).map((event) => ({
    eventId: event.id,
    pubkey: event.pubkey,
    content: event.content,
    kind: event.kind,
    createdAt: event.created_at,
    channelId: channel.id,
    tags: event.tags,
    threadSummary: null,
  }));
  const operation = group(10, [2], 1, {
    title: "Project design",
    summary: "Agreed next steps",
  });
  const state = buildOrganizationState([operation], channel.id);
  const result = projectForumOrganization(
    posts,
    undefined,
    sources,
    state,
    channel.id,
    id(1),
  );
  assert.deepEqual(
    result.posts.map((post) => post.eventId),
    [id(1)],
  );
  assert.deepEqual(
    result.thread.replies.map((reply) => [reply.eventId, reply.parentEventId]),
    [
      [id(2), id(1)],
      [id(3), id(2)],
      [id(4), id(3)],
    ],
  );
  assert.equal(result.thread.replies[0].content, sources[1].content);
  assert.deepEqual(result.thread.replies[0].tags, sources[1].tags);
  const undo = change(11, { type: "undo", change_event_id: operation.id });
  const restored = projectForumOrganization(
    posts,
    undefined,
    sources,
    buildOrganizationState([operation, undo], channel.id),
    channel.id,
    id(2),
  );
  assert.equal(restored.posts.length, 2);
  assert.deepEqual(
    restored.thread.replies.map((reply) => reply.eventId),
    [id(3), id(4)],
  );
});

test("original grouped-root links retain their subtree and hidden reply links provide a restore target", () => {
  const sources = [message(1), message(2), message(3, 2), message(4, 3, 2)];
  const events = [...sources, group(10, [2], 1)];
  const grouped = buildIndependentThreadPanelForLink(
    id(2),
    events,
    sources.slice(1),
    id(2),
    null,
    new Set(),
    channel,
    author,
    null,
  );
  assert.equal(grouped.threadHead.id, id(2));
  assert.equal(grouped.totalReplyCount, 2);
  const hidden = change(11, {
    type: "hide",
    message_ids: [id(3)],
    hidden: true,
  });
  const linked = buildIndependentThreadPanelForLink(
    id(3),
    [...sources, hidden],
    sources.slice(2),
    id(2),
    null,
    new Set(),
    channel,
    author,
    null,
  );
  assert.equal(linked.threadHead.organizationHiddenMessageId, id(3));
  assert.deepEqual(
    linked.messages.map((row) => row.id),
    [id(2), id(3), id(4)],
  );
  assert.deepEqual(linked.messages[1].tags, sources[2].tags);
  const deleted = { ...message(12), kind: 5, tags: [["e", id(3)]] };
  const stillDeleted = buildIndependentThreadPanelForLink(
    id(3),
    [...sources, hidden],
    [...sources.slice(2), deleted],
    id(2),
    null,
    new Set(),
    channel,
    author,
    null,
  );
  assert.ok(
    !stillDeleted.messages.some((row) => row.id === id(3)),
    "deep-link reveal cannot undo a real deletion",
  );
});
