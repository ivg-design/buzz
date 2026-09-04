import type { ManagedAgent } from "@/shared/api/types";

export type ManagedAgentReadiness = Pick<
  ManagedAgent,
  "runtimeLifecycle" | "setupMode" | "status"
>;

/**
 * Whether an active-turn signal may truthfully render as Working.
 *
 * Local process liveness is intentionally insufficient: a setup listener is
 * alive only to send configuration nudges, and starting/listening/waking
 * lifecycles do not yet have an ACP pool that can execute a turn. Remote
 * deployments have no local lifecycle and continue to use their relay-backed
 * observer signal.
 */
export function canManagedAgentReportWorking(
  agent: ManagedAgentReadiness,
): boolean {
  if (agent.status === "deployed") return true;
  return (
    agent.status === "running" &&
    !agent.setupMode &&
    agent.runtimeLifecycle === "ready"
  );
}
