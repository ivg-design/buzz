import { invokeTauri } from "@/shared/api/tauri";

export type NemoWorkspaceAvailability = {
  status: "ready" | "unavailable";
  error: string | null;
};

export type NemoWorkspaceInstructionStatus = {
  status: "verified" | "unavailable";
  source: string | null;
  revision: string | null;
  content: string | null;
  error: string | null;
};

export type NemoWorkspaceStatus = {
  mode: "nemo";
  projectName: string;
  repository: string;
  checkoutRoot: string | null;
  repositoryAccess: NemoWorkspaceAvailability;
  a2a: NemoWorkspaceAvailability;
  instructions: NemoWorkspaceInstructionStatus;
};

export type NemoWorkspaceScope = {
  communityId: string;
  relayUrl: string;
};

export function nemoWorkspaceStatusQueryKey(scope: NemoWorkspaceScope | null) {
  return [
    "nemo-workspace-status",
    scope?.communityId ?? "none",
    scope?.relayUrl ?? "none",
  ] as const;
}

/** Effective Nemo policy reported by the desktop backend. */
export async function getNemoWorkspaceStatus(
  scope: NemoWorkspaceScope,
): Promise<NemoWorkspaceStatus> {
  return invokeTauri<NemoWorkspaceStatus>("get_nemo_workspace_status", scope);
}
