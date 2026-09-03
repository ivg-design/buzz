import assert from "node:assert/strict";
import test from "node:test";

import {
  messageMentionPubkeys,
  resolveMessageRecipientPubkeys,
} from "./messageMentionPubkeys.ts";

function channel(overrides = {}) {
  return {
    id: "dm-1",
    name: "DM",
    channelType: "dm",
    visibility: "private",
    description: "",
    topic: null,
    purpose: null,
    memberCount: 2,
    memberPubkeys: ["OWNER", "AGENT"],
    participantPubkeys: ["owner", "agent"],
    participants: [],
    lastMessageAt: null,
    archivedAt: null,
    isMember: true,
    ttlSeconds: null,
    ttlDeadline: null,
    ...overrides,
  };
}

test("plain DM messages p-tag every recipient except the sender", () => {
  assert.deepEqual(messageMentionPubkeys(channel(), "owner"), ["agent"]);
});

test("DM recipients and explicit mentions are normalized and deduplicated", () => {
  assert.deepEqual(
    messageMentionPubkeys(channel(), "OWNER", ["AGENT", "third"]),
    ["agent", "third"],
  );
});

test("stream messages preserve explicit-mention semantics", () => {
  assert.deepEqual(
    messageMentionPubkeys(
      channel({ channelType: "stream", memberPubkeys: ["owner", "agent"] }),
      "owner",
      [],
    ),
    [],
  );
});

test("established DM resolves recipients from the roster when cached projections are empty", async () => {
  const loadedChannelIds = [];
  const recipients = await resolveMessageRecipientPubkeys(
    channel({
      memberCount: 2,
      memberPubkeys: [],
      participantPubkeys: [],
    }),
    "OWNER",
    [],
    async (channelId) => {
      loadedChannelIds.push(channelId);
      return ["OWNER", "AGENT"];
    },
  );

  assert.deepEqual(loadedChannelIds, ["dm-1"]);
  assert.deepEqual(recipients, ["agent"]);
});

test("incomplete group DM projection resolves every other roster participant", async () => {
  const recipients = await resolveMessageRecipientPubkeys(
    channel({
      memberCount: 3,
      memberPubkeys: ["OWNER"],
      participantPubkeys: ["AGENT"],
    }),
    "owner",
    ["AGENT"],
    async () => ["OWNER", "Agent", "REVIEWER"],
  );

  assert.deepEqual(recipients, ["agent", "reviewer"]);
});

test("complete DM projection avoids a redundant roster load", async () => {
  const recipients = await resolveMessageRecipientPubkeys(
    channel(),
    "owner",
    [],
    async () => {
      throw new Error("complete DM projection must stay on the fast path");
    },
  );

  assert.deepEqual(recipients, ["agent"]);
});

test("stream recipient resolution neither loads a roster nor adds channel members", async () => {
  const recipients = await resolveMessageRecipientPubkeys(
    channel({ channelType: "stream" }),
    "owner",
    ["EXPLICIT"],
    async () => {
      throw new Error("stream sends must not load a DM roster");
    },
  );

  assert.deepEqual(recipients, ["explicit"]);
});

test("DM send fails closed when neither projections nor roster identify a recipient", async () => {
  await assert.rejects(
    resolveMessageRecipientPubkeys(
      channel({
        memberCount: 2,
        memberPubkeys: [],
        participantPubkeys: [],
      }),
      "owner",
      [],
      async () => ["OWNER"],
    ),
    /Direct message participants are unavailable/,
  );
});
