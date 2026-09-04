import { useQuery } from "@tanstack/react-query";
import * as React from "react";

import {
  getProjectLocalRepoSnapshot,
  getProjectRepoSnapshot,
} from "@/shared/api/projectGit";
import type { ProjectRepoSnapshot } from "@/shared/api/types";
import type { Project } from "./hooks";
import { projectRepositoryQueryIdentity } from "./lib/projectRepositoryQueryIdentity";
import { selectProjectRepository } from "./projectModels";
import {
  type ProjectRepoUnavailableReason,
  projectRepoUnavailableReason,
} from "./lib/projectRepoAvailability";
import { useRelayOrigin } from "@/shared/lib/useRelayOrigin";

// Remote snapshots are backed by a blobless `git clone` per repository, so the
// overview scan is deliberately throttled and cached for a long time.
const OVERVIEW_SNAPSHOT_CONCURRENCY = 3;

function snapshotHasData(snapshot: ProjectRepoSnapshot | null | undefined) {
  return Boolean(
    snapshot && (snapshot.files.length > 0 || snapshot.latestCommit),
  );
}

/**
 * Local checkouts are instant (no network, no clone) and keep working when
 * the relay's git storage is empty or unreachable, so they are preferred.
 * Only repositories without usable local data fall back to a remote clone.
 */
async function fetchProjectSnapshot(
  project: Project,
  reposDir: string | null | undefined,
): Promise<ProjectRepoSnapshot | null> {
  const repository = selectProjectRepository(project, null);
  if (!repository) return null;
  try {
    const local = await getProjectLocalRepoSnapshot({
      reposDir,
      projectDtag: repository.dtag,
      cloneUrl: repository.cloneUrls[0] ?? null,
      defaultBranch: repository.defaultBranch,
      baseBranch: repository.defaultBranch,
    });
    if (snapshotHasData(local?.snapshot)) return local?.snapshot ?? null;
  } catch {
    // Best-effort: fall through to the remote snapshot.
  }

  const cloneUrl = repository.cloneUrls[0];
  if (!cloneUrl) return null;
  return getProjectRepoSnapshot({
    cloneUrl,
    defaultBranch: repository.defaultBranch,
    baseBranch: repository.defaultBranch,
  });
}

async function fetchProjectsRepoSnapshots(
  projects: Project[],
  reposDir: string | null | undefined,
): Promise<{
  snapshots: Record<string, ProjectRepoSnapshot>;
  unavailable: Record<string, ProjectRepoUnavailableReason>;
}> {
  const snapshots: Record<string, ProjectRepoSnapshot> = {};
  const unavailable: Record<string, ProjectRepoUnavailableReason> = {};
  const queue = [...projects];

  const workers = Array.from(
    { length: Math.min(OVERVIEW_SNAPSHOT_CONCURRENCY, queue.length) },
    async () => {
      for (;;) {
        const project = queue.shift();
        if (!project) return;
        try {
          const snapshot = await fetchProjectSnapshot(project, reposDir);
          if (snapshot) {
            snapshots[project.id] = snapshot;
          } else {
            unavailable[project.id] = "missing";
          }
        } catch (error) {
          unavailable[project.id] = projectRepoUnavailableReason(error);
        }
      }
    },
  );

  await Promise.all(workers);
  return { snapshots, unavailable };
}

/** Complete cache key for the local-first repository overview aggregate. */
export function projectsRepoSnapshotsQueryKey(
  projects: Project[],
  reposDir: string | null | undefined,
  relayOrigin: string | null,
) {
  const repositoryIdentities = projects
    .map((project) => {
      const repository = selectProjectRepository(project, null);
      return [
        project.id,
        projectRepositoryQueryIdentity({
          branch: repository?.defaultBranch,
          relayOrigin,
          repository,
          reposDir,
          source: "local-first",
        }),
      ] as const;
    })
    .sort(([left], [right]) => left.localeCompare(right));
  return ["projects", "repo-snapshots", repositoryIdentities] as const;
}

/**
 * Fetches repo snapshots for a set of projects (throttled, failure-tolerant)
 * for community-wide aggregates like the overview language breakdown.
 * Prefers local checkouts under `reposDir`; falls back to remote clones.
 * Callers should pre-filter and cap `projects` — up to one git clone per entry.
 */
export function useProjectsRepoSnapshotsQuery(
  projects: Project[],
  reposDir?: string | null,
) {
  const relayOrigin = useRelayOrigin();
  const queryKey = React.useMemo(
    () => projectsRepoSnapshotsQueryKey(projects, reposDir, relayOrigin),
    [projects, relayOrigin, reposDir],
  );

  return useQuery({
    enabled: projects.length > 0,
    queryKey,
    queryFn: () => fetchProjectsRepoSnapshots(projects, reposDir),
    staleTime: 15 * 60_000,
    retry: 0,
  });
}
