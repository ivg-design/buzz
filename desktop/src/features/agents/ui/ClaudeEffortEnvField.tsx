import type {
  AcpRuntimeCatalogEntry,
  ManagedAgent,
  RuntimeConfigSurface,
} from "@/shared/api/types";
import { cn } from "@/shared/lib/cn";
import {
  PERSONA_FIELD_CONTROL_CLASS,
  PERSONA_FIELD_SHELL_CLASS,
} from "./agentConfigOptions";
import type { EnvVarsValue } from "./EnvVarsEditor";
import { EffortPickerField } from "./EffortPickerField";
import { BUZZ_AGENT_THINKING_EFFORT } from "./buzzAgentConfig";

export const CLAUDE_CODE_EFFORT_LEVEL = "CLAUDE_CODE_EFFORT_LEVEL";

export function claudeEffortEnvDescriptor(
  runtime: AcpRuntimeCatalogEntry | undefined,
): { envVar: string; values: readonly string[] } | null {
  if (
    runtime?.thinkingEnvVar !== CLAUDE_CODE_EFFORT_LEVEL ||
    !runtime.effortCanonicalValues?.length
  ) {
    return null;
  }
  return {
    envVar: runtime.thinkingEnvVar,
    values: runtime.effortCanonicalValues,
  };
}

export function claudeEffortHiddenEnvKeys(
  secretEnvVar: string | null,
  runtime: AcpRuntimeCatalogEntry | undefined,
): string[] {
  const descriptor = claudeEffortEnvDescriptor(runtime);
  return [
    ...(secretEnvVar ? [secretEnvVar] : []),
    ...(descriptor ? [descriptor.envVar] : []),
  ];
}

export function updateClaudeEffortEnv(
  envVars: EnvVarsValue,
  envVar: string,
  value: string,
): EnvVarsValue {
  const next = { ...envVars };
  delete next[BUZZ_AGENT_THINKING_EFFORT];
  if (value === "") {
    delete next[envVar];
  } else {
    next[envVar] = value;
  }
  return next;
}

function effortLabel(value: string): string {
  if (value === "xhigh") return "Extra high";
  return value.length > 0 ? value[0].toUpperCase() + value.slice(1) : value;
}

export function ClaudeEffortEnvField({
  disabled,
  envVars,
  inheritedEnvVars = {},
  onEnvVarsChange,
  runtime,
}: {
  disabled?: boolean;
  envVars: EnvVarsValue;
  inheritedEnvVars?: EnvVarsValue;
  onEnvVarsChange: (value: EnvVarsValue) => void;
  runtime: AcpRuntimeCatalogEntry | undefined;
}) {
  const descriptor = claudeEffortEnvDescriptor(runtime);
  if (!descriptor) return null;

  const localValue = envVars[descriptor.envVar] ?? "";
  const inheritedValue = inheritedEnvVars[descriptor.envVar] ?? "";
  const inheritedLabel = inheritedValue
    ? `Use inherited (${effortLabel(inheritedValue)})`
    : "Use host or Claude default";
  const hasUnknownLocalValue =
    localValue !== "" && !descriptor.values.includes(localValue);

  return (
    <div className="space-y-1.5" data-testid="claude-effort-env-field">
      <label
        className="text-sm font-medium text-foreground"
        htmlFor="claude-effort-level"
      >
        Claude effort
      </label>
      <div className={PERSONA_FIELD_SHELL_CLASS}>
        <select
          className={cn(
            "h-11 w-full appearance-none bg-transparent px-3 py-2 text-sm leading-6",
            PERSONA_FIELD_CONTROL_CLASS,
          )}
          data-testid="claude-effort-level"
          disabled={disabled}
          id="claude-effort-level"
          onChange={(event) =>
            onEnvVarsChange(
              updateClaudeEffortEnv(
                envVars,
                descriptor.envVar,
                event.target.value,
              ),
            )
          }
          value={localValue}
        >
          <option value="">{inheritedLabel}</option>
          {hasUnknownLocalValue ? (
            <option value={localValue}>
              {localValue} (unsupported; preserved)
            </option>
          ) : null}
          {descriptor.values.map((value) => (
            <option key={value} value={value}>
              {effortLabel(value)}
            </option>
          ))}
        </select>
      </div>
      <p className="text-xs text-muted-foreground">
        Applies after the agent restarts. Support depends on the selected Claude
        model.
      </p>
    </div>
  );
}

export function AgentInstanceEffortField({
  agent,
  config,
  disabled,
  effortLevel,
  effortTouched,
  envVars,
  inheritedEnvVars,
  runtime,
  setEffortLevel,
  setEnvVars,
}: {
  agent: ManagedAgent;
  config: RuntimeConfigSurface | undefined;
  disabled: boolean;
  effortLevel: string | null;
  effortTouched: { current: boolean };
  envVars: EnvVarsValue;
  inheritedEnvVars: EnvVarsValue;
  runtime: AcpRuntimeCatalogEntry | undefined;
  setEffortLevel: (value: string | null) => void;
  setEnvVars: (value: EnvVarsValue) => void;
}) {
  return claudeEffortEnvDescriptor(runtime) ? (
    <ClaudeEffortEnvField
      disabled={disabled}
      envVars={envVars}
      inheritedEnvVars={inheritedEnvVars}
      onEnvVarsChange={(next) => {
        effortTouched.current = true;
        setEffortLevel(null);
        setEnvVars(next);
      }}
      runtime={runtime}
    />
  ) : (
    <EffortPickerField
      agent={agent}
      config={config}
      disabled={disabled}
      onChange={(level) => {
        effortTouched.current = true;
        setEffortLevel(level);
      }}
      value={
        effortTouched.current
          ? effortLevel
          : (config?.normalized.thinkingEffort?.value ?? null)
      }
    />
  );
}
