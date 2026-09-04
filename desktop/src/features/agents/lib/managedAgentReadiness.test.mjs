import assert from "node:assert/strict";
import test from "node:test";

import { canManagedAgentReportWorking } from "./managedAgentReadiness.ts";

function state(overrides = {}) {
  return {
    status: "running",
    runtimeLifecycle: "ready",
    setupMode: false,
    ...overrides,
  };
}

test("a ready ACP pool may report observer-backed work", () => {
  assert.equal(canManagedAgentReportWorking(state()), true);
});

test("a setup listener can never report Working", () => {
  assert.equal(canManagedAgentReportWorking(state({ setupMode: true })), false);
});

for (const runtimeLifecycle of [
  "starting",
  "listening",
  "waking",
  "failed",
  "stopped",
  null,
]) {
  test(`local ${runtimeLifecycle ?? "unknown"} lifecycle is not Working`, () => {
    assert.equal(
      canManagedAgentReportWorking(state({ runtimeLifecycle })),
      false,
    );
  });
}

test("a deployed remote agent keeps its relay-backed Working signal", () => {
  assert.equal(
    canManagedAgentReportWorking(
      state({
        status: "deployed",
        runtimeLifecycle: null,
        setupMode: false,
      }),
    ),
    true,
  );
});
