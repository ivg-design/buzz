import * as React from "react";

import { canManagedAgentReportWorking } from "@/features/agents/lib/managedAgentReadiness";
import type {
  ManagedAgent,
  ManagedAgentRuntimeLifecycle,
} from "@/shared/api/types";
import { normalizePubkey } from "@/shared/lib/pubkey";
import {
  clearActiveTurnsForAgent,
  type ActiveChannelTurnSummary,
  getActiveTurnsByChannel,
  getActiveTurnsForAgent,
  subscribeActiveAgentTurns,
} from "./activeAgentTurnsStore";

/**
 * Unified "agent is working" signal.
 *
 * Every surface that shows a working affordance (sidebar channel badges,
 * profile badges, agent rows, composer activity bar, activity panel header,
 * future thread ingresses) should read from this module instead of picking
 * one of the underlying pipes. The rule is:
 *
 *   1. Observer-derived active turns (kind 24200 → activeAgentTurnsStore)
 *      are the primary signal — they carry channel scope and a start anchor.
 *   2. Bot typing indicators (kind 20002, mirrored into this module by the
 *      channel typing hooks) are the fallback for agents whose observer
 *      stream is absent for that scope (e.g. remote harness without relay
 *      observer, or frames not yet arrived).
 *
 * Scope rule: with a channelId, "working" means working in that channel;
 * without one, "working" means any active work in any channel (the
 * all-channels rule the activity panel uses).
 */

export type AgentWorkingSource = "observer" | "typing" | "none";

export type AgentWorkingChannel = {
  channelId: string;
  /** Desktop-clock anchor for elapsed displays (turn start / first typing). */
  anchorAt: number;
  source: Exclude<AgentWorkingSource, "none">;
};

export type AgentWorkingState = {
  working: boolean;
  /** Strongest signal backing `working` for the requested scope. */
  source: AgentWorkingSource;
  /** Every channel the agent is working in (unscoped), observer-primary. */
  channels: AgentWorkingChannel[];
};

export type WorkingChannelSummary = ActiveChannelTurnSummary & {
  source: Exclude<AgentWorkingSource, "none">;
};

const IDLE_STATE: AgentWorkingState = {
  working: false,
  source: "none",
  channels: [],
};

// ── Typing registry (fallback input) ────────────────────────────────────────
// channelId → (normalized agent pubkey → first-seen ms). Fed by
// reportChannelBotTyping from the channel typing hooks; entries follow the
// typing TTL because the hooks re-report whenever their entries change.
const typingByChannel = new Map<string, Map<string, number>>();

type AgentWorkingReadiness = Pick<ManagedAgent, "pubkey" | "status"> & {
  runtimeLifecycle?: ManagedAgentRuntimeLifecycle | null;
  setupMode?: boolean;
};

// Known managed/owned agents are gated by their current ACP readiness. An
// unknown pubkey keeps the historical typing fallback for relay bots that are
// not represented by a local ManagedAgent summary.
const workingEligibilityByAgent = new Map<string, boolean>();

const listeners = new Set<() => void>();
let unsubscribeTurns: (() => void) | null = null;

// Reference-stable snapshots for useSyncExternalStore. React reads a snapshot
// before it subscribes, so these must be stable even with no listeners yet.
const stateCache = new Map<string, AgentWorkingState>();
let channelsCache: WorkingChannelSummary[] | null = null;
const channelPubkeysCache = new Map<string, string[]>();

function invalidateCaches() {
  stateCache.clear();
  channelsCache = null;
  channelPubkeysCache.clear();
}

function notify() {
  invalidateCaches();
  for (const listener of listeners) {
    listener();
  }
}

export function subscribeAgentWorkingSignal(listener: () => void) {
  listeners.add(listener);
  if (listeners.size === 1) {
    invalidateCaches();
    unsubscribeTurns = subscribeActiveAgentTurns(notify);
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) {
      unsubscribeTurns?.();
      unsubscribeTurns = null;
    }
  };
}

/**
 * Mirror the current bot typing pubkeys for a channel into the signal.
 * Call with the full current set (empty array clears the channel). First-seen
 * timestamps are preserved across re-reports so elapsed anchors stay stable.
 */
export function reportChannelBotTyping(
  channelId: string,
  pubkeys: readonly string[],
) {
  const current = typingByChannel.get(channelId);
  const next = new Map<string, number>();
  const now = Date.now();
  for (const pubkey of pubkeys) {
    const key = normalizePubkey(pubkey);
    if (workingEligibilityByAgent.get(key) === false) continue;
    next.set(key, current?.get(key) ?? now);
  }

  const unchanged =
    (current?.size ?? 0) === next.size &&
    [...next.keys()].every((key) => current?.has(key));
  if (unchanged) {
    return;
  }

  if (next.size === 0) {
    typingByChannel.delete(channelId);
  } else {
    typingByChannel.set(channelId, next);
  }
  notify();
}

/**
 * Clear every source that can render Working for one agent. Runtime readiness
 * transitions call this before paint, so an old observer turn or typing TTL
 * cannot outlive a switch into setup/listening/waking state.
 */
export function clearAgentWorkingSignalForAgent(agentPubkey: string): void {
  const key = normalizePubkey(agentPubkey);
  let typingChanged = false;
  for (const [channelId, entries] of typingByChannel) {
    if (!entries.delete(key)) continue;
    typingChanged = true;
    if (entries.size === 0) typingByChannel.delete(channelId);
  }

  clearActiveTurnsForAgent(key);
  if (typingChanged) notify();
}

/**
 * Synchronize the authoritative runtime gate used by every Working consumer.
 * A readiness downgrade clears retained observer and typing evidence, and
 * subsequent typing frames remain suppressed until the ACP pool is ready.
 */
export function syncAgentWorkingEligibility(
  agents: readonly AgentWorkingReadiness[],
): void {
  const nextKeys = new Set<string>();
  for (const agent of agents) {
    const key = normalizePubkey(agent.pubkey);
    nextKeys.add(key);
    const eligible = canManagedAgentReportWorking({
      status: agent.status,
      runtimeLifecycle:
        agent.runtimeLifecycle === undefined && agent.status === "running"
          ? "ready"
          : (agent.runtimeLifecycle ?? null),
      setupMode: agent.setupMode ?? false,
    });
    workingEligibilityByAgent.set(key, eligible);
    if (!eligible) clearAgentWorkingSignalForAgent(key);
  }

  for (const key of [...workingEligibilityByAgent.keys()]) {
    if (nextKeys.has(key)) continue;
    workingEligibilityByAgent.delete(key);
    clearAgentWorkingSignalForAgent(key);
  }
  invalidateCaches();
}

function computeAgentWorkingState(
  agentPubkey: string,
  channelId: string | null,
): AgentWorkingState {
  const key = normalizePubkey(agentPubkey);
  if (workingEligibilityByAgent.get(key) === false) {
    return IDLE_STATE;
  }
  const turns = getActiveTurnsForAgent(key);

  const channels: AgentWorkingChannel[] = turns.map((turn) => ({
    channelId: turn.channelId,
    anchorAt: turn.anchorAt,
    source: "observer" as const,
  }));
  const observerChannelIds = new Set(turns.map((turn) => turn.channelId));

  for (const [typingChannelId, entries] of typingByChannel) {
    if (observerChannelIds.has(typingChannelId)) {
      continue;
    }
    const since = entries.get(key);
    if (since !== undefined) {
      channels.push({
        channelId: typingChannelId,
        anchorAt: since,
        source: "typing",
      });
    }
  }

  if (channels.length === 0) {
    return IDLE_STATE;
  }

  channels.sort((a, b) => a.channelId.localeCompare(b.channelId));

  const scoped =
    channelId === null
      ? channels
      : channels.filter((channel) => channel.channelId === channelId);
  const source: AgentWorkingSource = scoped.some(
    (channel) => channel.source === "observer",
  )
    ? "observer"
    : scoped.length > 0
      ? "typing"
      : "none";

  return { working: source !== "none", source, channels };
}

/**
 * Working state for one agent, optionally scoped to a channel. Returns a
 * reference-stable snapshot while subscribed (useSyncExternalStore-safe).
 */
export function getAgentWorkingState(
  agentPubkey: string | null | undefined,
  channelId: string | null = null,
): AgentWorkingState {
  if (!agentPubkey) {
    return IDLE_STATE;
  }
  const cacheKey = `${normalizePubkey(agentPubkey)}|${channelId ?? ""}`;
  const cached = stateCache.get(cacheKey);
  if (cached) {
    return cached;
  }
  const state = computeAgentWorkingState(agentPubkey, channelId);
  stateCache.set(cacheKey, state);
  return state;
}

/**
 * All channels with agent work in progress, aggregated across agents and
 * merged observer-primary: typing-only agents fold into an existing observer
 * summary; channels with only typing get a typing-sourced summary anchored to
 * first-seen typing.
 */
export function getWorkingChannels(): WorkingChannelSummary[] {
  if (channelsCache) {
    return channelsCache;
  }

  const byChannel = new Map<string, WorkingChannelSummary>();
  for (const summary of getActiveTurnsByChannel()) {
    const eligiblePubkeys = summary.agentPubkeys.filter(
      (pubkey) =>
        workingEligibilityByAgent.get(normalizePubkey(pubkey)) !== false,
    );
    if (eligiblePubkeys.length === 0) continue;
    byChannel.set(summary.channelId, {
      ...summary,
      agentCount: eligiblePubkeys.length,
      agentPubkeys: eligiblePubkeys,
      source: "observer",
    });
  }

  for (const [channelId, entries] of typingByChannel) {
    const existing = byChannel.get(channelId);
    if (existing) {
      const known = new Set(
        existing.agentPubkeys.map((pubkey) => normalizePubkey(pubkey)),
      );
      const merged = [...existing.agentPubkeys];
      for (const pubkey of entries.keys()) {
        if (workingEligibilityByAgent.get(pubkey) === false) continue;
        if (!known.has(pubkey)) {
          merged.push(pubkey);
        }
      }
      if (merged.length !== existing.agentPubkeys.length) {
        byChannel.set(channelId, {
          ...existing,
          agentPubkeys: merged,
          agentCount: merged.length,
        });
      }
      continue;
    }

    const eligibleEntries = [...entries].filter(
      ([pubkey]) => workingEligibilityByAgent.get(pubkey) !== false,
    );
    if (eligibleEntries.length === 0) continue;
    let anchorAt = Number.POSITIVE_INFINITY;
    for (const [, since] of eligibleEntries) {
      if (since < anchorAt) {
        anchorAt = since;
      }
    }
    byChannel.set(channelId, {
      channelId,
      anchorAt,
      agentCount: eligibleEntries.length,
      agentPubkeys: eligibleEntries.map(([pubkey]) => pubkey),
      source: "typing",
    });
  }

  const result = [...byChannel.values()].sort((a, b) =>
    a.channelId.localeCompare(b.channelId),
  );
  channelsCache = result;
  return result;
}

const EMPTY_PUBKEYS: string[] = [];

/**
 * Normalized pubkeys of every agent working in the given channel
 * (observer turns ∪ typing fallback). Stable while subscribed.
 */
export function getWorkingAgentPubkeysForChannel(
  channelId: string | null | undefined,
): string[] {
  if (!channelId) {
    return EMPTY_PUBKEYS;
  }
  const cached = channelPubkeysCache.get(channelId);
  if (cached) {
    return cached;
  }
  const merged = new Set<string>();
  for (const summary of getActiveTurnsByChannel()) {
    if (summary.channelId !== channelId) {
      continue;
    }
    for (const pubkey of summary.agentPubkeys) {
      const key = normalizePubkey(pubkey);
      if (workingEligibilityByAgent.get(key) !== false) merged.add(key);
    }
  }
  const typing = typingByChannel.get(channelId);
  if (typing) {
    for (const pubkey of typing.keys()) {
      if (workingEligibilityByAgent.get(pubkey) !== false) merged.add(pubkey);
    }
  }
  const result = merged.size === 0 ? EMPTY_PUBKEYS : [...merged].sort();
  channelPubkeysCache.set(channelId, result);
  return result;
}

// ── Hooks ────────────────────────────────────────────────────────────────────

/** Working state for one agent, optionally scoped to a channel. */
export function useAgentWorking(
  agentPubkey: string | null | undefined,
  channelId: string | null = null,
): AgentWorkingState {
  return React.useSyncExternalStore(subscribeAgentWorkingSignal, () =>
    getAgentWorkingState(agentPubkey, channelId),
  );
}

/** All channels with agent work in progress, across agents. */
export function useWorkingChannels(): WorkingChannelSummary[] {
  return React.useSyncExternalStore(
    subscribeAgentWorkingSignal,
    getWorkingChannels,
  );
}

/** Normalized pubkeys of agents working in a channel. */
export function useChannelWorkingAgentPubkeys(
  channelId: string | null | undefined,
): string[] {
  return React.useSyncExternalStore(subscribeAgentWorkingSignal, () =>
    getWorkingAgentPubkeysForChannel(channelId),
  );
}

/** Community-switch reset (see resetCommunityState in useCommunityInit). */
export function resetAgentWorkingSignal() {
  typingByChannel.clear();
  workingEligibilityByAgent.clear();
  invalidateCaches();
  for (const listener of listeners) {
    listener();
  }
}
