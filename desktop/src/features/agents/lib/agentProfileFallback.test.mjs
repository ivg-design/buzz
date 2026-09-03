import assert from "node:assert/strict";
import test from "node:test";

import { mergeAgentNamesIntoProfiles } from "./agentProfileFallback.ts";

const AGENT_PUBKEY = "a".repeat(64);
const OWNER_PUBKEY = "b".repeat(64);

test("managed agent name fills a missing relay profile", () => {
  const merged = mergeAgentNamesIntoProfiles(
    {},
    [
      {
        pubkey: AGENT_PUBKEY.toUpperCase(),
        name: "Codexitron",
        avatarUrl: "file:///agent.png",
      },
    ],
    [],
    OWNER_PUBKEY,
  );

  assert.deepEqual(merged[AGENT_PUBKEY], {
    displayName: "Codexitron",
    avatarUrl: "file:///agent.png",
    nip05Handle: null,
    ownerPubkey: OWNER_PUBKEY,
    isAgent: true,
  });
});

test("verified profile metadata wins over the local fallback", () => {
  const profile = {
    displayName: "Published name",
    avatarUrl: "https://relay.example/agent.png",
    nip05Handle: "agent@example.com",
    ownerPubkey: OWNER_PUBKEY,
  };
  const merged = mergeAgentNamesIntoProfiles(
    { [AGENT_PUBKEY]: profile },
    [{ pubkey: AGENT_PUBKEY, name: "Local name", avatarUrl: null }],
    [],
    "c".repeat(64),
  );

  assert.deepEqual(merged[AGENT_PUBKEY], { ...profile, isAgent: true });
});

test("relay directory name fills a missing kind:0 profile", () => {
  const merged = mergeAgentNamesIntoProfiles(
    {},
    [],
    [{ pubkey: AGENT_PUBKEY, name: "Remote reviewer" }],
  );

  assert.equal(merged[AGENT_PUBKEY]?.displayName, "Remote reviewer");
  assert.equal(merged[AGENT_PUBKEY]?.isAgent, true);
});
