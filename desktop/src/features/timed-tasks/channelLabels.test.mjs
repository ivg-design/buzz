import assert from "node:assert/strict";
import { test } from "node:test";
import { timedTaskConversationLabel } from "./channelLabels.ts";

test("DM picker retains resolved group participants and distinguishes a group after a member leaves", () => {
  const dm = { channelType: "dm", name: "DM" };
  const group = { channelType: "dm", name: "Group DM (3)" };
  assert.equal(timedTaskConversationLabel(dm, "Mysteropodes"), "Mysteropodes");
  assert.equal(timedTaskConversationLabel(group, "Mysteropodes, Buzzotron"), "Group: Mysteropodes, Buzzotron");
  assert.equal(timedTaskConversationLabel(group, "Mysteropodes"), "Group: Mysteropodes");
  assert.equal(timedTaskConversationLabel({ ...group, name: "Release planning" }, "Release planning"), "Release planning");
});
