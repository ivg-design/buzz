import * as React from "react";

import type { ProjectPullRequest, Repository } from "@/features/projects/hooks";
import { projectRepositoryQueryIdentity } from "@/features/projects/lib/projectRepositoryQueryIdentity";
import {
  getProjectLocalRepoFileContent,
  getProjectRepoFileContent,
} from "@/shared/api/projectGit";
import type { RepositoryFileContentSource } from "./useRepositoryFileContent";
import { useRelayOrigin } from "@/shared/lib/useRelayOrigin";

type RepositoryFileContentSourceInput = {
  activeBranch: string | null;
  activeTag: { commit: string; name: string } | null;
  pullRequest: ProjectPullRequest | null;
  repository: Repository | null;
  reposDir?: string;
  selectedTag: string | null;
  source: "local" | "remote";
};

/** Builds the file loader and its complete cache identity for one repository. */
export function buildRepositoryFileContentSource(
  {
    activeBranch,
    activeTag,
    pullRequest,
    repository,
    reposDir,
    selectedTag,
    source,
  }: RepositoryFileContentSourceInput,
  relayOrigin: string | null,
) {
  if (!repository) return undefined;
  const effectiveSource = selectedTag ? "remote" : source;

  if (effectiveSource === "local") {
    return {
      cacheKey: [
        projectRepositoryQueryIdentity({
          branch: activeBranch,
          relayOrigin,
          repository,
          reposDir,
          source: "local",
        }),
      ],
      load: (path: string) =>
        getProjectLocalRepoFileContent({
          reposDir,
          projectDtag: repository.dtag,
          cloneUrl: repository.cloneUrls[0] ?? null,
          defaultBranch: activeBranch ?? repository.defaultBranch,
          path,
        }),
    } satisfies RepositoryFileContentSource;
  }

  const contentPullRequest = selectedTag ? null : pullRequest;
  const cloneUrl = contentPullRequest?.cloneUrls[0] ?? repository.cloneUrls[0];
  if (!cloneUrl) return undefined;
  const targetRef = activeTag
    ? `refs/tags/${activeTag.name}`
    : contentPullRequest
      ? `refs/nostr/${contentPullRequest.id}`
      : null;
  const targetCommit = activeTag?.commit ?? contentPullRequest?.commit ?? null;
  return {
    cacheKey: [
      projectRepositoryQueryIdentity({
        branch: activeBranch,
        cloneUrl,
        relayOrigin,
        repository,
        source: "remote",
        targetCommit,
        targetRef,
      }),
    ],
    load: (path: string) =>
      getProjectRepoFileContent({
        cloneUrl,
        defaultBranch: activeBranch ?? repository.defaultBranch,
        targetRef,
        targetCommit,
        path,
      }),
  } satisfies RepositoryFileContentSource;
}

export function useRepositoryFileContentSource({
  activeBranch,
  activeTag,
  pullRequest,
  repository,
  reposDir,
  selectedTag,
  source,
}: RepositoryFileContentSourceInput) {
  const relayOrigin = useRelayOrigin();
  return React.useMemo(
    () =>
      buildRepositoryFileContentSource(
        {
          activeBranch,
          activeTag,
          pullRequest,
          repository,
          reposDir,
          selectedTag,
          source,
        },
        relayOrigin,
      ),
    [
      activeBranch,
      activeTag,
      pullRequest,
      relayOrigin,
      repository,
      reposDir,
      selectedTag,
      source,
    ],
  );
}
