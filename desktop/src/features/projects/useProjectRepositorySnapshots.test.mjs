import assert from "node:assert/strict";
import test from "node:test";

import {
  projectLocalRepoDiffQueryKey,
  projectLocalRepoSnapshotQueryKey,
  projectRepoDiffQueryKey,
  projectRepoSnapshotQueryKey,
} from "./hooks.ts";
import { projectRepositoryQueryIdentity } from "./lib/projectRepositoryQueryIdentity.ts";
import { buildRepositoryFileContentSource } from "./ui/useRepositoryFileContentSource.ts";
import { projectCommitDiffQueryKey } from "./useProjectCommitDiff.ts";
import { projectRepositorySnapshotQueryKey } from "./useProjectRepositorySnapshots.ts";
import { projectsRepoSnapshotsQueryKey } from "./useProjectsRepoSnapshots.ts";

const OWNER = "ab".repeat(32);
const RELAY_ORIGIN = "https://relay.example";
const REPOS_DIR = "/Users/dev/github";

function repository(cloneUrl) {
  return {
    id: `${OWNER}:nemo`,
    dtag: "nemo",
    name: "Nemo",
    description: "",
    cloneUrls: [cloneUrl],
    webUrl: null,
    owner: OWNER,
    contributors: [],
    createdAt: 1,
    status: "active",
    defaultBranch: "main",
    repoAddress: `30617:${OWNER}:nemo`,
  };
}

function project(repositoryValue) {
  return {
    id: "project:nemo",
    repositories: [repositoryValue],
    primaryRepositoryAddress: repositoryValue.repoAddress,
  };
}

function pullRequest() {
  return {
    id: "cd".repeat(32),
    cloneUrls: [],
    commit: "12".repeat(20),
    initialCommit: "34".repeat(20),
  };
}

function productionKeys(repositoryValue) {
  const review = pullRequest();
  const fileSource = buildRepositoryFileContentSource(
    {
      activeBranch: "main",
      activeTag: null,
      pullRequest: review,
      repository: repositoryValue,
      reposDir: REPOS_DIR,
      selectedTag: null,
      source: "remote",
    },
    RELAY_ORIGIN,
  );
  assert.ok(fileSource);
  return {
    commit: projectCommitDiffQueryKey(
      repositoryValue,
      "56".repeat(20),
      "remote",
      REPOS_DIR,
      RELAY_ORIGIN,
    ),
    diff: projectRepoDiffQueryKey(
      repositoryValue,
      RELAY_ORIGIN,
      "main",
      review,
    ),
    fileContent: fileSource.cacheKey,
    localDiff: projectLocalRepoDiffQueryKey(
      repositoryValue,
      RELAY_ORIGIN,
      REPOS_DIR,
      "main",
      review,
    ),
    localSnapshot: projectLocalRepoSnapshotQueryKey(
      repositoryValue,
      RELAY_ORIGIN,
      REPOS_DIR,
      "main",
    ),
    overview: projectsRepoSnapshotsQueryKey(
      [project(repositoryValue)],
      REPOS_DIR,
      RELAY_ORIGIN,
    ),
    projectHome: projectRepositorySnapshotQueryKey(
      repositoryValue,
      REPOS_DIR,
      RELAY_ORIGIN,
    ),
    snapshot: projectRepoSnapshotQueryKey(
      repositoryValue,
      RELAY_ORIGIN,
      "main",
      review,
      null,
    ),
  };
}

test("repository identity includes every output-affecting dimension", () => {
  const githubUrl = "https://github.com/mysteropodes/nemo.git";
  assert.deepEqual(
    projectRepositoryQueryIdentity({
      baseCommit: "base",
      branch: "feature/cache-key",
      relayOrigin: RELAY_ORIGIN,
      repository: repository(githubUrl),
      reposDir: REPOS_DIR,
      source: "local",
      targetCommit: "head",
      targetRef: "refs/heads/feature/cache-key",
    }),
    {
      baseBranch: "main",
      baseCommit: "base",
      branch: "feature/cache-key",
      cloneUrl: githubUrl,
      host: "external:github.com",
      localRoot: REPOS_DIR,
      repositoryAddress: `30617:${OWNER}:nemo`,
      repositoryId: `${OWNER}:nemo`,
      source: "local",
      targetCommit: "head",
      targetRef: "refs/heads/feature/cache-key",
      version: 1,
    },
  );
});

test("a replaceable repository URL invalidates every production data key", () => {
  const before = productionKeys(
    repository("https://github.com/mysteropodes/nemo.git"),
  );
  const after = productionKeys(
    repository("https://github.com/mysteropodes/nemo-next.git"),
  );

  for (const key of Object.keys(before)) {
    assert.notDeepEqual(after[key], before[key], `${key} reused stale data`);
  }
});

test("a replaceable repository host invalidates every production data key", () => {
  const before = productionKeys(
    repository("https://github.com/mysteropodes/nemo.git"),
  );
  const after = productionKeys(repository(`${RELAY_ORIGIN}/git/${OWNER}/nemo`));

  for (const key of Object.keys(before)) {
    assert.notDeepEqual(after[key], before[key], `${key} reused stale data`);
  }
});

test("local roots affect local data without fragmenting remote caches", () => {
  const repositoryValue = repository(
    "https://github.com/mysteropodes/nemo.git",
  );
  const remoteA = projectCommitDiffQueryKey(
    repositoryValue,
    "56".repeat(20),
    "remote",
    "/tmp/a",
    RELAY_ORIGIN,
  );
  const remoteB = projectCommitDiffQueryKey(
    repositoryValue,
    "56".repeat(20),
    "remote",
    "/tmp/b",
    RELAY_ORIGIN,
  );
  const localA = projectLocalRepoSnapshotQueryKey(
    repositoryValue,
    RELAY_ORIGIN,
    "/tmp/a",
    "main",
  );
  const localB = projectLocalRepoSnapshotQueryKey(
    repositoryValue,
    RELAY_ORIGIN,
    "/tmp/b",
    "main",
  );

  assert.deepEqual(remoteA, remoteB);
  assert.notDeepEqual(localA, localB);
});
