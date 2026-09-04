import assert from "node:assert/strict";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { NemoWorkspaceStatusView } from "./NemoWorkspaceSettingsCard.tsx";

function render(status) {
  return renderToStaticMarkup(
    React.createElement(NemoWorkspaceStatusView, {
      error: null,
      isPending: false,
      onRetry() {},
      status,
    }),
  );
}

function readyStatus(overrides = {}) {
  return {
    mode: "nemo",
    projectName: "Nemo",
    repository: "https://github.com/mysteropodes/nemo",
    checkoutRoot: "/Users/developer/github/nemo",
    repositoryAccess: { status: "ready", error: null },
    a2a: { status: "ready", error: null },
    instructions: {
      status: "verified",
      source: "Bundled Nemo workspace policy",
      revision: null,
      content: "Preserve the active task.\nUse A2A for bounded parallel work.",
      error: null,
    },
    ...overrides,
  };
}

test("ready Nemo workspace shows automatic access and literal instructions", () => {
  const html = render(readyStatus());

  assert.match(html, />Active</);
  assert.match(html, /Full Nemo repository read and write access/);
  assert.match(html, /Enabled automatically for Nemo agents/);
  assert.match(html, /Preserve the active task\.\nUse A2A/);
  assert.match(html, /Bundled Nemo workspace policy/);
  assert.doesNotMatch(
    html,
    /Worktree ID|Path prefixes|Grant saved|Verified owner.*[a-f0-9]{64}/i,
  );
});

test("unavailable backend facts never render the workspace as active", () => {
  const html = render(
    readyStatus({
      repositoryAccess: {
        status: "unavailable",
        error: "Nemo checkout could not be verified.",
      },
      a2a: { status: "unavailable", error: "A2A receiver is offline." },
      instructions: {
        status: "unavailable",
        source: "Bundled Nemo workspace policy",
        revision: null,
        content: null,
        error: "Instruction preload failed.",
      },
    }),
  );

  assert.match(html, /Needs attention/);
  assert.doesNotMatch(html, />Active</);
  assert.match(html, /Nemo checkout could not be verified/);
  assert.match(html, /A2A receiver is offline/);
  assert.match(html, /Instruction preload failed/);
  assert.match(html, /unavailable until verification succeeds/);
});

test("query failure is explicit and retryable", () => {
  const html = renderToStaticMarkup(
    React.createElement(NemoWorkspaceStatusView, {
      error: new Error("desktop command unavailable"),
      isPending: false,
      onRetry() {},
      status: null,
    }),
  );

  assert.match(html, /Nemo workspace could not be verified/);
  assert.match(html, /desktop command unavailable/);
  assert.match(html, /Try again/);
});
