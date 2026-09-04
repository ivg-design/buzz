import { invokeTauri } from "@/shared/api/tauri";

export type WorkspaceProject = {
  projectAddress: string;
  homeChannel: string;
  repository: string;
  displayName: string;
  instructionRevision: string;
};

export type WorkspaceProjectState = {
  relayUrl: string;
  project: WorkspaceProject | null;
  codexInstructionStatus: "verified" | "unavailable";
  codexInstructionError: string | null;
};

export type WorkspaceProjectSaveResult = WorkspaceProjectState & {
  changed: boolean;
  restartedCount: number;
  failedRestartCount: number;
};

export function getWorkspaceProject(): Promise<WorkspaceProjectState> {
  return invokeTauri<WorkspaceProjectState>("get_workspace_project");
}

export function setWorkspaceProject(input: {
  project: WorkspaceProject | null;
  expectedRelayUrl: string;
  expectedSignerPubkey: string;
}): Promise<WorkspaceProjectSaveResult> {
  return invokeTauri<WorkspaceProjectSaveResult>("set_workspace_project", {
    input,
  });
}
