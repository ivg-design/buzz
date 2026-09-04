import { effectiveCloneUrls } from "./projectCloneUrl";
import { projectRepoHost } from "./projectRepoHost";

export type RepositoryQuerySource = "local" | "remote" | "local-first";

type RepositoryQueryIdentityRepository = {
  cloneUrls: string[];
  defaultBranch: string;
  dtag: string;
  id: string;
  owner: string;
  repoAddress?: string;
};

type RepositoryQueryIdentityInput = {
  baseCommit?: string | null;
  branch?: string | null;
  cloneUrl?: string | null;
  relayOrigin: string | null | undefined;
  repository: RepositoryQueryIdentityRepository | null | undefined;
  reposDir?: string | null;
  source: RepositoryQuerySource;
  targetCommit?: string | null;
  targetRef?: string | null;
};

/**
 * Complete cache and retention identity for data read from a repository.
 * Replaceable repository announcements keep a stable address, so URL and host
 * are first-class dimensions alongside the selected source and git target.
 */
export function projectRepositoryQueryIdentity({
  baseCommit,
  branch,
  cloneUrl: cloneUrlOverride,
  relayOrigin,
  repository,
  reposDir,
  source,
  targetCommit,
  targetRef,
}: RepositoryQueryIdentityInput) {
  const cloneUrl =
    cloneUrlOverride ??
    (repository
      ? (effectiveCloneUrls(
          repository.cloneUrls,
          relayOrigin,
          repository.owner,
          repository.dtag,
        )[0] ?? null)
      : null);
  const host = projectRepoHost(cloneUrl, relayOrigin);
  const hostKey =
    host.kind === "external" ? `${host.kind}:${host.host}` : host.kind;
  return {
    baseBranch: repository?.defaultBranch ?? "default",
    baseCommit: baseCommit ?? "none",
    branch: branch ?? repository?.defaultBranch ?? "default",
    cloneUrl: cloneUrl ?? "none",
    host: hostKey,
    localRoot: source === "remote" ? "none" : (reposDir ?? "default"),
    repositoryAddress: repository?.repoAddress ?? repository?.id ?? "none",
    repositoryId: repository?.id ?? "none",
    source,
    targetCommit: targetCommit ?? "none",
    targetRef: targetRef ?? "none",
    version: 1,
  } as const;
}
