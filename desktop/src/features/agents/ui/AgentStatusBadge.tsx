import * as React from "react";

import { Badge } from "@/shared/ui/badge";
import type { ManagedAgent, PresenceStatus } from "@/shared/api/types";
import { cn } from "@/shared/lib/cn";
import { canManagedAgentReportWorking } from "@/features/agents/lib/managedAgentReadiness";

/** Grace period after mount before treating "running + no presence" as "Starting…" */
const PRESENCE_GRACE_MS = 15_000;

export function AgentStatusBadge({
  className,
  isWorking,
  presenceLoaded,
  presenceStatus,
  runtimeLifecycle,
  sentenceCase = false,
  setupMode,
  status,
}: {
  className?: string;
  isWorking?: boolean;
  presenceLoaded: boolean;
  presenceStatus: PresenceStatus | undefined;
  runtimeLifecycle: ManagedAgent["runtimeLifecycle"];
  sentenceCase?: boolean;
  setupMode: boolean;
  status: ManagedAgent["status"];
}) {
  const [inGracePeriod, setInGracePeriod] = React.useState(true);

  React.useEffect(() => {
    const timer = setTimeout(() => setInGracePeriod(false), PRESENCE_GRACE_MS);
    return () => clearTimeout(timer);
  }, []);

  const working =
    Boolean(isWorking) &&
    canManagedAgentReportWorking({ runtimeLifecycle, setupMode, status });
  const needsSetup = status === "running" && setupMode;
  const lifecycleStarting =
    status === "running" &&
    (runtimeLifecycle === "starting" || runtimeLifecycle === "waking");
  const lifecycleFailed = status === "running" && runtimeLifecycle === "failed";
  const lifecycleListening =
    status === "running" && runtimeLifecycle === "listening";
  const presenceStarting =
    !inGracePeriod &&
    presenceLoaded &&
    status === "running" &&
    runtimeLifecycle === null &&
    (!presenceStatus || presenceStatus === "offline");
  const isStarting = lifecycleStarting || presenceStarting;
  const isActive =
    status === "deployed" ||
    (status === "running" && !needsSetup && !lifecycleFailed && !isStarting);

  const variant: "default" | "warning" | "secondary" = working
    ? "default"
    : needsSetup || lifecycleFailed || isStarting
      ? "warning"
      : isActive
        ? "default"
        : "secondary";

  const rawLabel = needsSetup
    ? "Needs setup"
    : lifecycleFailed
      ? "Failed"
      : isStarting
        ? "Starting\u2026"
        : lifecycleListening
          ? "Listening"
          : working
            ? "Working"
            : status.replace(/_/g, " ");
  const label = sentenceCase
    ? `${rawLabel.charAt(0).toUpperCase()}${rawLabel.slice(1)}`
    : rawLabel;

  return (
    <Badge
      className={cn(className, working && "motion-safe:animate-pulse")}
      variant={variant}
    >
      {label}
    </Badge>
  );
}
