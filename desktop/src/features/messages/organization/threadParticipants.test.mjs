import assert from "node:assert/strict";
import test from "node:test";
import { buildOrganizationState, organizationAction } from "./projection.ts";
import {
  eligibleThreadAgents,
  participantThreadRoot,
  toggleThreadParticipant,
} from "./threadParticipants.ts";

const id = (n) => n.toString(16).padStart(64, "0");
const channel = {
  id: "participants-fixture",
  channelType: "stream",
  visibility: "open",
};
const change = (n, action, at = n) => ({
  id: id(n),
  pubkey: id(900),
  sig: "fixture",
  kind: 40009,
  created_at: at,
  tags: [["h", channel.id]],
  content: JSON.stringify({ version: 1, action }),
});
const participants = (n, root, agents) =>
  change(n, {
    type: "participants",
    thread_root_id: id(root),
    agent_pubkeys: agents.map(id),
  });
const state = (events) => buildOrganizationState(events, channel.id);

test("participant lists replace prior policy; empty and absent remain distinct and Undo restores", () => {
  const first = participants(10, 1, [80, 81]);
  const replacement = participants(11, 1, [82]);
  const removeAll = participants(12, 1, []);
  assert.equal(state([]).participants.has(id(1)), false);
  assert.deepEqual(state([replacement, first]).participants.get(id(1)), [
    id(82),
  ]);
  const empty = state([first, replacement, removeAll]);
  assert.equal(empty.participants.has(id(1)), true);
  assert.deepEqual(empty.participants.get(id(1)), []);
  const undone = state([
    first,
    replacement,
    removeAll,
    participants(13, 2, [83]),
    change(14, { type: "undo", change_event_id: removeAll.id }),
  ]);
  assert.deepEqual(undone.participants.get(id(1)), [id(82)]);
  assert.deepEqual(undone.participants.get(id(2)), [id(83)]);
  assert.equal(
    undone.records.find((r) => r.event.id === removeAll.id).undone,
    true,
  );
  assert.equal(undone.groups.size, 0);
  assert.equal(undone.hidden.size, 0);
  assert.equal(undone.metadata.size, 0);
});

test("same-time signed event IDs determine policy consistently regardless arrival order", () => {
  const a = change(
    10,
    { type: "participants", thread_root_id: id(1), agent_pubkeys: [id(80)] },
    100,
  );
  const b = change(
    11,
    { type: "participants", thread_root_id: id(1), agent_pubkeys: [id(81)] },
    100,
  );
  assert.deepEqual(state([b, a]).participants.get(id(1)), [id(81)]);
  assert.deepEqual(state([a, b]).participants.get(id(1)), [id(81)]);
});

test("grouped threads use destination policy and group Undo restores the original policy", () => {
  const events = [
    participants(10, 1, [80]),
    participants(11, 2, [81]),
    participants(12, 3, [82]),
    change(13, { type: "group", message_ids: [id(1)], thread_root_id: id(2) }),
    change(14, { type: "group", message_ids: [id(2)], thread_root_id: id(3) }),
  ];
  const grouped = state(events);
  assert.equal(participantThreadRoot(id(1), grouped), id(3));
  assert.deepEqual(
    grouped.participants.get(participantThreadRoot(id(1), grouped)),
    [id(82)],
  );
  const restored = state([
    ...events,
    change(15, { type: "undo", change_event_id: id(14) }),
  ]);
  assert.equal(participantThreadRoot(id(1), restored), id(2));
  assert.deepEqual(
    restored.participants.get(participantThreadRoot(id(1), restored)),
    [id(81)],
  );
});

test("participant parsing rejects malformed, duplicate, oversized or cross-channel lists", () => {
  for (const agent_pubkeys of [
    [id(80), id(80)],
    ["bad"],
    ["A".repeat(64)],
    Array.from({ length: 101 }, (_, n) => id(n)),
  ]) {
    assert.equal(
      organizationAction(
        change(10, {
          type: "participants",
          thread_root_id: id(1),
          agent_pubkeys,
        }),
        channel.id,
      ),
      null,
    );
  }
  assert.equal(
    organizationAction(participants(10, 1, []), "other-channel"),
    null,
  );
  assert.equal(
    organizationAction(
      change(10, {
        type: "participants",
        thread_root_id: "bad",
        agent_pubkeys: [],
      }),
      channel.id,
    ),
    null,
  );
  assert.equal(
    organizationAction(participants(10, 1, []), channel.id).type,
    "participants",
  );
});

const agents = [
  { pubkey: id(80), ownerPubkey: id(90), name: "Alpha", status: "offline" },
  { pubkey: id(81), ownerPubkey: id(91), name: "Beta", status: "online" },
  { pubkey: id(82), ownerPubkey: null, name: "Unverified" },
];
const members = [{ pubkey: id(90) }, { pubkey: id(81) }];
test("open channels offer verified directory agents across owners including offline agents", () => {
  assert.deepEqual(
    eligibleThreadAgents(channel, agents, []).map((a) => a.pubkey),
    [id(80), id(81)],
  );
});
test("private channels and DMs require direct agent membership; owner membership is insufficient", () => {
  for (const restricted of [
    { ...channel, visibility: "private" },
    { ...channel, channelType: "dm" },
  ]) {
    assert.deepEqual(
      eligibleThreadAgents(restricted, agents, members).map((a) => a.pubkey),
      [id(81)],
    );
  }
});
test("checkbox edits submit a complete deterministic list and can remove the last participant", () => {
  assert.deepEqual(toggleThreadParticipant([id(81)], id(80), true), [
    id(80),
    id(81),
  ]);
  assert.deepEqual(toggleThreadParticipant([id(81)], id(81), true), [id(81)]);
  assert.deepEqual(toggleThreadParticipant([id(80), id(81)], id(80), false), [
    id(81),
  ]);
  assert.deepEqual(toggleThreadParticipant([id(80)], id(80), false), []);
});
