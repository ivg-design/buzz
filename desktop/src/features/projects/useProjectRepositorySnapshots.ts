import { useQueries } from "@tanstack/react-query";

import type {
  ProjectRepoSnapshot,
  Repository,
} from "@/features/projects/hooks";
import { fetchRepoState } from "@/features/projects/hooks";
import { resolveProjectDefaultBranch } from "@/features/projects/lib/projectBranches";
import { projectRepoHostForRepository } from "@/features/projects/lib/projectRepoHost";
import { projectRepositoryQueryIdentity } from "@/features/projects/lib/projectRepositoryQueryIdentity";
import {
  getProjectLocalRepoSnapshot,
  getProjectRepoSnapshot,
} from "@/shared/api/projectGit";
import { useRelayOrigin } from "@/shared/lib/useRelayOrigin";

type ProjectRepositorySnapshotSource = "local" | "remote";

type LoadedProjectRepositorySnapshot = {
  effectiveBranch: string | null;
  snapshot: ProjectRepoSnapshot;
  source: ProjectRepositorySnapshotSource;
};

/**
 * Cache identity for the local-first project-home snapshot. Repository
 * announcements are replaceable, so the stable repository id alone cannot
 * distinguish a later event that points at a different clone URL or host.
 */
export function projectRepositorySnapshotQueryKey(
  repository: Repository,
  reposDir: string | null | undefined,
  relayOrigin: string | null,
) {
  return [
    "project",
    repository.id,
    "project-home-repo-snapshot",
    projectRepositoryQueryIdentity({
      branch: repository.defaultBranch,
      relayOrigin,
      repository,
      reposDir,
      source: "local-first",
    }),
  ] as const;
}

export type ProjectRepositorySnapshotResult = {
  effectiveBranch: string | null;
  error: unknown;
  isLoading: boolean;
  repository: Repository;
  snapshot: ProjectRepoSnapshot | null;
  source: ProjectRepositorySnapshotSource;
};

function snapshotHasData(snapshot: ProjectRepoSnapshot | null | undefined) {
  return Boolean(
    snapshot && (snapshot.files.length > 0 || snapshot.latestCommit),
  );
}

async function loadProjectRepositorySnapshot(
  repository: Repository,
  reposDir: string | null | undefined,
  relayOrigin: string | null,
): Promise<LoadedProjectRepositorySnapshot> {
  let localError: unknown;
  try {
    const local = await getProjectLocalRepoSnapshot({
      reposDir,
      projectDtag: repository.dtag,
      cloneUrl: repository.cloneUrls[0] ?? null,
      defaultBranch: repository.defaultBranch,
      baseBranch: repository.defaultBranch,
    });
    if (local && snapshotHasData(local.snapshot)) {
      return {
        effectiveBranch: repository.defaultBranch,
        snapshot: local.snapshot,
        source: "local",
      };
    }
    if (local) {
      localError = new Error("The local checkout has no repository content.");
    }
  } catch (error) {
    localError = error;
  }

  const host = projectRepoHostForRepository(repository, relayOrigin);
  const canReadRemote =
    host.kind === "buzz" ||
    (host.kind === "external" && host.host === "github.com");
  if (!canReadRemote) {
    throw (
      localError ??
      new Error("No local checkout found for this external repository.")
    );
  }

  const cloneUrl = repository.cloneUrls[0];
  if (!cloneUrl) {
    throw localError ?? new Error("Repository not found on the relay.");
  }
  const repoState = await fetchRepoState(repository);
  const defaultBranch = resolveProjectDefaultBranch(
    repository.defaultBranch,
    repoState,
  );
  const snapshot = await getProjectRepoSnapshot({
    baseBranch: defaultBranch,
    cloneUrl,
    defaultBranch,
  });
  return { effectiveBranch: defaultBranch, snapshot, source: "remote" };
}

/** Loads each project repository independently so one failure stays partial. */
export function useProjectRepositorySnapshots(
  repositories: Repository[],
  enabled = true,
  reposDir?: string | null,
): ProjectRepositorySnapshotResult[] {
  const relayOrigin = useRelayOrigin();
  const hosts = repositories.map((repository) =>
    projectRepoHostForRepository(repository, relayOrigin),
  );
  const queries = useQueries({
    queries: repositories.map((repository) => ({
      enabled,
      queryFn: () =>
        loadProjectRepositorySnapshot(repository, reposDir, relayOrigin),
      queryKey: projectRepositorySnapshotQueryKey(
        repository,
        reposDir,
        relayOrigin,
      ),
      retry: 1,
      staleTime: 30_000,
    })),
  });

  return repositories.map((repository, index) => {
    const query = queries[index];
    return {
      effectiveBranch: query?.data?.effectiveBranch ?? repository.defaultBranch,
      error: query?.error,
      isLoading: query?.isLoading ?? false,
      repository,
      snapshot: query?.data?.snapshot ?? null,
      source:
        query?.data?.source ??
        (hosts[index]?.kind === "buzz" ? "remote" : "local"),
    };
  });
}
