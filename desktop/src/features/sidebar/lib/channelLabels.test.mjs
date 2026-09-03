import assert from "node:assert/strict";
import test from "node:test";

import { resolveChannelDisplayLabel } from "./channelLabels.ts";

const SELF = "a".repeat(64);
const AGENT = "b".repeat(64);

function dm(overrides = {}) {
  return {
    id: "dm-1",
    name: "DM",
    channelType: "dm",
    visibility: "private",
    description: "",
    topic: null,
    purpose: null,
    memberCount: 2,
    memberPubkeys: [SELF, AGENT],
    lastMessageAt: null,
    archivedAt: null,
    participants: [SELF, AGENT],
    participantPubkeys: [SELF, AGENT],
    isMember: true,
    ttlSeconds: null,
    ttlDeadline: null,
    ...overrides,
  };
}

test("generic DM resolves a managed-agent profile name", () => {
  assert.equal(
    resolveChannelDisplayLabel(dm(), SELF, {
      [AGENT]: {
        displayName: "Codexitron",
        avatarUrl: null,
        nip05Handle: null,
        ownerPubkey: SELF,
        isAgent: true,
      },
    }),
    "Codexitron",
  );
});

test("participant pubkey stored as the DM name is treated as a fallback", () => {
  assert.equal(
    resolveChannelDisplayLabel(dm({ name: AGENT }), SELF, {
      [AGENT]: {
        displayName: "Codexitron",
        avatarUrl: null,
        nip05Handle: null,
        ownerPubkey: SELF,
        isAgent: true,
      },
    }),
    "Codexitron",
  );
});

test("an intentionally named DM preserves its custom title", () => {
  assert.equal(
    resolveChannelDisplayLabel(dm({ name: "Design pair" }), SELF, {
      [AGENT]: {
        displayName: "Codexitron",
        avatarUrl: null,
        nip05Handle: null,
        ownerPubkey: SELF,
        isAgent: true,
      },
    }),
    "Design pair",
  );
});
