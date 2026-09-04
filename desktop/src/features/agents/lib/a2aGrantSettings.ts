import type { RelayAgent } from "@/shared/api/types";
import type { Project } from "@/features/projects/projectModels";
import type { A2aGrantScope } from "@/shared/api/tauriA2aGrants";
import type { WorkspaceProject } from "@/shared/api/tauriWorkspaceProject";

export type A2aRepositoryChoice = {
  id: string;
  label: string;
  projectName: string;
  repositoryName: string;
  scope: Omit<A2aGrantScope, "reposDir">;
};

export type A2aProjectPeer = {
  agent: RelayAgent;
  isProjectMember: boolean;
};

const LOWER_HEX_64 = /^[0-9a-f]{64}$/;
const CAPABILITY = /^[a-z][a-z0-9._-]{0,63}$/;
const WORKTREE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function canonicalGitHubRepository(value: string): string | null {
  const trimmed = value.trim().replace(/\/$/, "");
  const sshPath = trimmed.startsWith("git@github.com:")
    ? trimmed.slice("git@github.com:".length)
    : null;
  try {
    const path =
      sshPath ??
      (() => {
        const url = new URL(trimmed);
        if (
          url.protocol !== "https:" ||
          url.hostname !== "github.com" ||
          url.username ||
          url.password ||
          url.port ||
          url.search ||
          url.hash
        ) {
          throw new Error("non-canonical GitHub remote");
        }
        return url.pathname;
      })();
    const parts = path
      .replace(/^\/+|\/+$/g, "")
      .replace(/\.git$/i, "")
      .split("/");
    if (
      parts.length !== 2 ||
      parts.some(
        (part) =>
          !part ||
          part === "." ||
          part === ".." ||
          !/^[A-Za-z0-9._-]+$/.test(part),
      )
    ) {
      return null;
    }
    return `https://github.com/${parts[0].toLowerCase()}/${parts[1].toLowerCase()}`;
  } catch {
    return null;
  }
}

export function buildA2aRepositoryChoices(
  projects: readonly Project[],
): A2aRepositoryChoice[] {
  const choices = new Map<string, A2aRepositoryChoice>();
  for (const project of projects) {
    if (
      project.legacy ||
      !project.projectAddress.startsWith("30621:") ||
      !project.projectChannelId
    ) {
      continue;
    }
    for (const repository of project.repositories) {
      const canonical = repository.cloneUrls
        .map(canonicalGitHubRepository)
        .find((value): value is string => value != null);
      if (!canonical) continue;
      const id = `${project.projectAddress}|${repository.repoAddress}|${canonical}`;
      choices.set(id, {
        id,
        label: `${project.name} / ${repository.name}`,
        projectName: project.name,
        repositoryName: repository.name,
        scope: {
          projectDtag: project.dtag,
          projectAddress: project.projectAddress,
          homeChannel: project.projectChannelId,
          repository: canonical,
        },
      });
    }
  }
  return [...choices.values()].sort((left, right) =>
    left.label.localeCompare(right.label),
  );
}

export function verifiedA2aPeers(
  agents: readonly RelayAgent[],
  homeChannel: string | null | undefined,
  memberPubkeys?: readonly string[] | null,
): RelayAgent[] {
  return verifiedA2aProjectPeers(agents, homeChannel, memberPubkeys)
    .filter((candidate) => candidate.isProjectMember)
    .map((candidate) => candidate.agent);
}

/**
 * Relay-verified agents that can be assigned to the selected Project.
 *
 * When an authoritative member roster is supplied, it decides membership.
 * Passing `null` means the roster is unavailable and fails closed. Callers
 * without a roster retain the relay directory's signed channel projection.
 */
export function verifiedA2aProjectPeers(
  agents: readonly RelayAgent[],
  homeChannel: string | null | undefined,
  memberPubkeys?: readonly string[] | null,
): A2aProjectPeer[] {
  if (!homeChannel) return [];
  const members =
    memberPubkeys === undefined
      ? null
      : new Set((memberPubkeys ?? []).map((pubkey) => pubkey.toLowerCase()));
  return agents
    .filter(
      (agent) =>
        LOWER_HEX_64.test(agent.pubkey) &&
        agent.ownerPubkey != null &&
        LOWER_HEX_64.test(agent.ownerPubkey),
    )
    .map((agent) => ({
      agent,
      isProjectMember:
        memberPubkeys === undefined
          ? agent.channelIds.includes(homeChannel)
          : members?.has(agent.pubkey) === true,
    }))
    .sort((left, right) =>
      (left.agent.name || left.agent.pubkey).localeCompare(
        right.agent.name || right.agent.pubkey,
      ),
    );
}

export function a2aPeerLabel(agent: RelayAgent): string {
  const name = agent.name.trim() || `Agent ${agent.pubkey.slice(0, 8)}`;
  return `${name} · ${agent.pubkey.slice(0, 8)}`;
}

export function workspaceProjectMatchesScope(
  project: WorkspaceProject | null | undefined,
  scope: Pick<
    A2aGrantScope,
    "projectAddress" | "homeChannel" | "repository"
  > | null,
): boolean {
  return (
    project != null &&
    scope != null &&
    project.projectAddress === scope.projectAddress &&
    project.homeChannel === scope.homeChannel &&
    project.repository === scope.repository
  );
}

export function validateA2aCapability(value: string): string | null {
  return CAPABILITY.test(value)
    ? null
    : "Start with a lowercase letter; use lowercase letters, numbers, dots, underscores, or hyphens.";
}

export function validateA2aWorktreeId(value: string): string | null {
  return WORKTREE_ID.test(value)
    ? null
    : "Start with a letter or number; use letters, numbers, dots, underscores, or hyphens.";
}

export function parseA2aPathPrefixes(
  value: string,
): { paths: string[]; error: null } | { paths: []; error: string } {
  const paths = [
    ...new Set(
      value
        .split(/[\n,]/)
        .map((path) => path.trim())
        .filter(Boolean),
    ),
  ];
  if (paths.length === 0) {
    return { paths: [], error: "Add at least one repository-relative path." };
  }
  if (paths.length > 128) {
    return { paths: [], error: "A grant can contain at most 128 paths." };
  }
  for (const path of paths) {
    const parts = path.split("/");
    if (
      path.length > 1024 ||
      path.startsWith("/") ||
      path.endsWith("/") ||
      path.includes("\\") ||
      path.includes("//") ||
      parts.some(
        (part) =>
          !part ||
          part === "." ||
          part === ".." ||
          part.toLowerCase() === ".git",
      )
    ) {
      return {
        paths: [],
        error: `“${path}” must be a normalized repository-relative path outside .git.`,
      };
    }
  }
  return { paths, error: null };
}
