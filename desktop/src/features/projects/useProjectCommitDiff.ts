import { useQuery } from "@tanstack/react-query";

import {
  getProjectLocalRepoDiff,
  getProjectRepoDiff,
} from "@/shared/api/projectGit";
import type { ProjectRepoDiff } from "@/shared/api/types";
import type { Repository as Project } from "./hooks";
import { projectRepositoryQueryIdentity } from "./lib/projectRepositoryQueryIdentity";
import { useProjectRepoHost } from "./useProjectRepoHost";
import { useRelayOrigin } from "@/shared/lib/useRelayOrigin";

async function fetchProjectCommitDiff(
  project: Project,
  commitHash: string,
  repoSource: "remote" | "local",
  reposDir: string | null | undefined,
  allowRemote: boolean,
): Promise<ProjectRepoDiff> {
  if (repoSource === "local") {
    // Passing only the target commit (no base branch/commit) makes the
    // backend diff the commit against its parent.
    const local = await getProjectLocalRepoDiff({
      reposDir,
      projectDtag: project.dtag,
      cloneUrl: project.cloneUrls[0] ?? null,
      targetCommit: commitHash,
    });
    if (local) return local;
  }

  if (!allowRemote) {
    throw new Error("No local checkout found for this external repository.");
  }

  const cloneUrl = project.cloneUrls[0];
  if (!cloneUrl) {
    throw new Error("This project has no clone URL to load the commit from.");
  }
  return getProjectRepoDiff({
    cloneUrl,
    defaultBranch: project.defaultBranch,
    targetCommit: commitHash,
  });
}

/** Complete cache key for a commit-against-parent diff query. */
export function projectCommitDiffQueryKey(
  project: Project | null | undefined,
  commitHash: string | null,
  repoSource: "remote" | "local",
  reposDir: string | null | undefined,
  relayOrigin: string | null,
) {
  return [
    "project",
    project?.id ?? "none",
    "commit-diff",
    projectRepositoryQueryIdentity({
      branch: project?.defaultBranch,
      relayOrigin,
      repository: project,
      reposDir,
      source: repoSource,
      targetCommit: commitHash,
    }),
  ] as const;
}

/**
 * Diff of a single commit against its parent, for the commit detail view.
 * Prefers the local checkout when the repository source is "local" and falls
 * back to a remote fetch when no checkout exists.
 */
export function useProjectCommitDiffQuery(
  project: Project | null | undefined,
  commitHash: string | null,
  repoSource: "remote" | "local",
  reposDir?: string | null,
) {
  const host = useProjectRepoHost(project);
  const relayOrigin = useRelayOrigin();
  const canReadRemote =
    host.kind === "buzz" ||
    (host.kind === "external" && host.host === "github.com");
  return useQuery({
    enabled: Boolean(project && commitHash),
    queryKey: projectCommitDiffQueryKey(
      project,
      commitHash,
      repoSource,
      reposDir,
      relayOrigin,
    ),
    queryFn: () => {
      if (!project || !commitHash) {
        return Promise.reject(new Error("No commit selected."));
      }
      return fetchProjectCommitDiff(
        project,
        commitHash,
        repoSource,
        reposDir,
        canReadRemote,
      );
    },
    // A commit's diff is immutable, so never refetch it while cached.
    staleTime: Number.POSITIVE_INFINITY,
    retry: 1,
  });
}
