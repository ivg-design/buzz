import assert from "node:assert/strict";
import test from "node:test";
import { resolvePopoverStopChannels } from "./popoverStopChannels.ts";

test("Stop prefers current conversation work, not unrelated active or idle scopes", () => {
  const active = [{ channelId: "other" }, { channelId: "conversation" }];
  assert.deepEqual(resolvePopoverStopChannels(true, active, "conversation"), [
    "conversation",
  ]);
  assert.deepEqual(resolvePopoverStopChannels(true, active, "idle"), [
    "other",
    "conversation",
  ]);
  assert.deepEqual(resolvePopoverStopChannels(true, active), [
    "other",
    "conversation",
  ]);
  assert.deepEqual(
    resolvePopoverStopChannels(true, [{ channelId: "other" }], "idle"),
    ["other"],
  );
});

test("Stop is unavailable for idle and stopped agents", () => {
  assert.deepEqual(resolvePopoverStopChannels(true, [], "conversation"), []);
  assert.deepEqual(
    resolvePopoverStopChannels(
      false,
      [{ channelId: "conversation" }],
      "conversation",
    ),
    [],
  );
});
