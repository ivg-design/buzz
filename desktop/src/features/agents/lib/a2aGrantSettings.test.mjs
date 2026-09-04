import assert from "node:assert/strict";
import test from "node:test";

import {
  a2aPeerLabel,
  buildA2aRepositoryChoices,
  canonicalGitHubRepository,
  parseA2aPathPrefixes,
  validateA2aCapability,
  validateA2aWorktreeId,
  verifiedA2aPeers,
  verifiedA2aProjectPeers,
  workspaceProjectMatchesScope,
} from "./a2aGrantSettings.ts";

const OWNER = "a".repeat(64);
const AGENT = "b".repeat(64);
const CHANNEL = "3580ca9b-47b4-4af9-b22a-1068778f26c6";

function project(overrides = {}) {
  return {
    id: `30621:${OWNER}:nemo`,
    dtag: "nemo",
    name: "Nemo",
    description: "",
    owner: OWNER,
    createdAt: 1,
    projectChannelId: CHANNEL,
    relatedChannelIds: [],
    status: "active",
    projectAddress: `30621:${OWNER}:nemo`,
    primaryRepositoryAddress: `30617:${OWNER}:nemo`,
    repositoryAddresses: [`30617:${OWNER}:nemo`],
    repositories: [
      {
        id: "nemo",
        dtag: "nemo",
        name: "nemo",
        description: "",
        cloneUrls: ["https://github.com/Mysteropodes/Nemo.git"],
        webUrl: null,
        owner: OWNER,
        contributors: [],
        createdAt: 1,
        status: "active",
        defaultBranch: "main",
        repoAddress: `30617:${OWNER}:nemo`,
      },
    ],
    legacy: false,
    ...overrides,
  };
}

function agent(overrides = {}) {
  return {
    pubkey: AGENT,
    ownerPubkey: OWNER,
    name: "Reviewer",
    agentType: "agent",
    channels: ["Nemo"],
    channelIds: [CHANNEL],
    capabilities: [],
    status: "online",
    respondTo: null,
    respondToAllowlist: [],
    ...overrides,
  };
}

test("canonical GitHub repositories normalize case and .git", () => {
  assert.equal(
    canonicalGitHubRepository("https://github.com/Mysteropodes/Nemo.git"),
    "https://github.com/mysteropodes/nemo",
  );
  assert.equal(
    canonicalGitHubRepository("git@github.com:Mysteropodes/Nemo.git"),
    "https://github.com/mysteropodes/nemo",
  );
  for (const value of [
    "http://github.com/mysteropodes/nemo",
    "https://github.com/mysteropodes/nemo/issues",
    "https://token@github.com/mysteropodes/nemo",
    "https://github.com/mysteropodes/%2e%2e",
  ]) {
    assert.equal(canonicalGitHubRepository(value), null, value);
  }
});

test("repository choices require an explicit project, home channel, and GitHub repo", () => {
  assert.equal(buildA2aRepositoryChoices([project()]).length, 1);
  assert.equal(
    buildA2aRepositoryChoices([project({ legacy: true })]).length,
    0,
  );
  assert.equal(
    buildA2aRepositoryChoices([project({ projectChannelId: null })]).length,
    0,
  );
});

test("peer choices require both a verified owner and project channel membership", () => {
  assert.deepEqual(verifiedA2aPeers([agent()], CHANNEL), [agent()]);
  assert.deepEqual(
    verifiedA2aPeers([agent({ ownerPubkey: null })], CHANNEL),
    [],
  );
  assert.deepEqual(
    verifiedA2aPeers([agent({ channelIds: ["another-channel"] })], CHANNEL),
    [],
  );
  assert.equal(a2aPeerLabel(agent()), `Reviewer · ${AGENT.slice(0, 8)}`);
});

test("project assignment candidates keep membership separate from verification", () => {
  const outside = agent({ channelIds: ["another-channel"] });
  assert.deepEqual(verifiedA2aProjectPeers([outside], CHANNEL, []), [
    { agent: outside, isProjectMember: false },
  ]);
  assert.deepEqual(
    verifiedA2aProjectPeers([outside], CHANNEL, [AGENT.toUpperCase()]),
    [{ agent: outside, isProjectMember: true }],
  );
  assert.deepEqual(
    verifiedA2aProjectPeers(
      [outside, agent({ pubkey: "c".repeat(64), ownerPubkey: null })],
      CHANNEL,
      null,
    ),
    [{ agent: outside, isProjectMember: false }],
  );
  assert.deepEqual(verifiedA2aPeers([outside], CHANNEL, null), []);
});

test("Workspace Project matching requires the exact Project and repository scope", () => {
  const scope = buildA2aRepositoryChoices([project()])[0].scope;
  const configured = {
    projectAddress: scope.projectAddress,
    homeChannel: scope.homeChannel,
    repository: scope.repository,
    displayName: "Nemo",
    instructionRevision: "d".repeat(40),
  };
  assert.equal(workspaceProjectMatchesScope(configured, scope), true);
  assert.equal(
    workspaceProjectMatchesScope(
      { ...configured, homeChannel: "another-channel" },
      scope,
    ),
    false,
  );
  assert.equal(workspaceProjectMatchesScope(null, scope), false);
});

test("capability and worktree tokens fail closed on shell-like or broad input", () => {
  assert.equal(validateA2aCapability("rust.review"), null);
  assert.ok(validateA2aCapability("Rust review"));
  assert.ok(validateA2aCapability("../review"));
  assert.equal(validateA2aWorktreeId("nemo-review.1"), null);
  assert.ok(validateA2aWorktreeId("../nemo"));
});

test("allowed paths normalize lines, remove duplicates, and reject traversal", () => {
  assert.deepEqual(parseA2aPathPrefixes("src\ncrates, src"), {
    paths: ["src", "crates"],
    error: null,
  });
  for (const value of ["", ".", "../src", "/src", ".git/hooks", "src/"]) {
    assert.ok(parseA2aPathPrefixes(value).error, value);
  }
});
