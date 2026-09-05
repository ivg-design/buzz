import type {
  AcpRuntimeCatalogEntry,
  ManagedAgent,
  RuntimeConfigSurface,
} from "@/shared/api/types";
import { cn } from "@/shared/lib/cn";
import { readAgentEnvCaseInsensitive } from "../lib/agentEnvironment";
import {
  PERSONA_FIELD_CONTROL_CLASS,
  PERSONA_FIELD_SHELL_CLASS,
} from "./agentConfigOptions";
import type { EnvVarsValue } from "./EnvVarsEditor";
import { EffortPickerField } from "./EffortPickerField";
import { envVarsWithoutKeyCaseInsensitive } from "./providerEnvVarUpdates";
import {
  CLAUDE_CODE_EFFORT_LEVEL,
  EFFORT_ENV_ALIASES,
} from "./runtimeModelProviderSelection";

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
  envVars: EnvVarsValue,
): string[] {
  const descriptor = claudeEffortEnvDescriptor(runtime);
  if (!descriptor) return secretEnvVar ? [secretEnvVar] : [];
  const aliases = new Set(EFFORT_ENV_ALIASES.map((key) => key.toLowerCase()));
  return [
    ...(secretEnvVar ? [secretEnvVar] : []),
    descriptor.envVar,
    ...Object.keys(envVars).filter((key) => aliases.has(key.toLowerCase())),
  ];
}

function effortEnvValue(
  envVars: EnvVarsValue,
  preferredEnvVar: string,
): string | undefined {
  const keys = [
    preferredEnvVar,
    ...EFFORT_ENV_ALIASES.filter((key) => key !== preferredEnvVar),
  ];
  for (const key of keys) {
    const value = readAgentEnvCaseInsensitive(envVars, key);
    if (value !== undefined) return value;
  }
  return undefined;
}

export function updateClaudeEffortEnv(
  envVars: EnvVarsValue,
  envVar: string,
  value: string,
): EnvVarsValue {
  let next = envVars;
  for (const alias of EFFORT_ENV_ALIASES) {
    next = envVarsWithoutKeyCaseInsensitive(next, alias);
  }
  if (value === "") {
    return next;
  }
  return { ...next, [envVar]: value };
}

function effortLabel(value: string): string {
  if (value === "xhigh") return "Extra high";
  return value.length > 0 ? value[0].toUpperCase() + value.slice(1) : value;
}

export function ClaudeEffortEnvField({
  disabled,
  envVars,
  explicitEffort,
  inheritedEnvVars = {},
  onEnvVarsChange,
  runtime,
}: {
  disabled?: boolean;
  envVars: EnvVarsValue;
  explicitEffort?: string;
  inheritedEnvVars?: EnvVarsValue;
  onEnvVarsChange: (value: EnvVarsValue) => void;
  runtime: AcpRuntimeCatalogEntry | undefined;
}) {
  const descriptor = claudeEffortEnvDescriptor(runtime);
  if (!descriptor) return null;

  const nativeValue = readAgentEnvCaseInsensitive(envVars, descriptor.envVar);
  const localValue =
    nativeValue ??
    explicitEffort ??
    effortEnvValue(envVars, descriptor.envVar) ??
    "";
  const inheritedValue =
    effortEnvValue(inheritedEnvVars, descriptor.envVar) ?? "";
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
      explicitEffort={
        config?.normalized.thinkingEffort?.origin === "buzzExplicit"
          ? (config.normalized.thinkingEffort.value ?? "")
          : undefined
      }
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
