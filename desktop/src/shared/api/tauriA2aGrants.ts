import { invokeTauri } from "@/shared/api/tauri";

export type A2aGrantScope = {
  reposDir?: string | null;
  projectDtag: string;
  projectAddress: string;
  homeChannel: string;
  repository: string;
};

export type A2aCheckoutInfo = {
  path: string;
  branch: string;
  baseSha: string;
  suggestedWorktreeId: string;
};

export type A2aGrant = {
  id: string;
  requesterPubkeys: string[];
  capabilities: string[];
  pathPrefixes: string[];
  worktreeId: string;
  status: "ready" | "stale";
  statusMessage: string | null;
};

export type A2aGrantState = {
  storage: string;
  checkout: A2aCheckoutInfo;
  grants: A2aGrant[];
};

export async function listA2aCheckouts(
  scope: A2aGrantScope,
): Promise<A2aCheckoutInfo[]> {
  return invokeTauri<A2aCheckoutInfo[]>("list_a2a_checkouts", { scope });
}

export async function getA2aGrants(
  scope: A2aGrantScope,
  checkoutRoot: string,
): Promise<A2aGrantState> {
  return invokeTauri<A2aGrantState>("get_a2a_grants", {
    input: { scope, checkoutRoot },
  });
}

export async function upsertA2aGrant(input: {
  scope: A2aGrantScope;
  checkoutRoot: string;
  expectedBranch: string;
  expectedBaseSha: string;
  peerPubkey: string;
  capability: string;
  pathPrefixes: string[];
  worktreeId: string;
  expectedRelayUrl: string;
  expectedSignerPubkey: string;
}): Promise<A2aGrantState> {
  return invokeTauri<A2aGrantState>("upsert_a2a_grant", { input });
}

export async function removeA2aGrant(input: {
  scope: A2aGrantScope;
  checkoutRoot: string;
  grantId: string;
  expectedRelayUrl: string;
  expectedSignerPubkey: string;
}): Promise<A2aGrantState> {
  return invokeTauri<A2aGrantState>("remove_a2a_grant", { input });
}
