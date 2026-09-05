import assert from "node:assert/strict";
import test from "node:test";

import {
  countTopLevelTimelineRows,
  formatTimelineMessages,
} from "./formatTimelineMessages.ts";
import { projectJobConversation } from "./jobConversationProjection.mjs";
import {
  buildMainTimelineEntries,
  buildThreadPanelData,
} from "./threadPanel.ts";

const CHANNEL_ID = "36411e44-0e2d-4cfe-bd6e-567eb169db9f";
const REQUEST_ID = "a".repeat(64);
const REQUESTER = "1".repeat(64);
const WORKER = "2".repeat(64);

function event(kind, id, payload, overrides = {}) {
  return {
    id,
    pubkey: kind === 43001 ? REQUESTER : WORKER,
    kind,
    created_at: 1_700_000_000 + Number.parseInt(id[0] ?? "0", 16),
    content: typeof payload === "string" ? payload : JSON.stringify(payload),
    tags: [["h", CHANNEL_ID]],
    sig: "sig",
    ...overrides,
  };
}

function followup(kind, id, payload, tags = []) {
  return event(
    kind,
    id,
    { request_event_id: REQUEST_ID, ...payload },
    { tags: [["h", CHANNEL_ID], ...tags] },
  );
}

function request() {
  return event(43001, REQUEST_ID, {
    summary: "Review the reconnect path",
    acceptance: ["Recovery test passes", "No duplicate execution"],
  });
}

test("projects every signed job lifecycle kind into human-readable conversation text", () => {
  const cases = [
    [
      request(),
      "Task: Review the reconnect path\n\nAcceptance:\n- Recovery test passes\n- No duplicate execution",
    ],
    [
      followup(43002, "b".repeat(64), {
        claim: { status: "processed", scope_digest: "d".repeat(64) },
      }),
      "Received the task.",
    ],
    [
      followup(43002, "c".repeat(64), {
        claim: { status: "accepted", scope_digest: "d".repeat(64) },
      }),
      "Accepted the task.",
    ],
    [
      followup(43002, "d".repeat(64), {
        claim: {
          status: "declined",
          scope_digest: "d".repeat(64),
          reason: "unsupported_capability",
        },
      }),
      "Declined the task: unsupported_capability",
    ],
    [
      followup(43003, "e".repeat(64), {
        status: "progress",
        message: "Added the recovery fixture.",
        evidence: [],
      }),
      "Progress: Added the recovery fixture.",
    ],
    [
      followup(43003, "f".repeat(64), {
        status: "blocked",
        message: "Waiting for the Windows runner.",
        evidence: [],
      }),
      "Blocked: Waiting for the Windows runner.",
    ],
    [
      followup(43004, "3".repeat(64), {
        outcome: "success",
        candidate_sha: "9".repeat(40),
        artifacts: ["git:candidate"],
        evidence: ["contract:focused-test"],
      }),
      `Completed successfully.\n\nCandidate commit: ${"9".repeat(40)}\n\nArtifacts:\n- git:candidate\n\nEvidence:\n- contract:focused-test`,
    ],
    [
      followup(43005, "4".repeat(64), {
        action: "cancel",
        reason: "The requirement changed.",
      }),
      "Cancellation requested: The requirement changed.",
    ],
    [
      followup(43006, "5".repeat(64), {
        outcome: "failed",
        code: "test_failed",
        message: "The focused test failed.",
        retryable: true,
      }),
      "Failed: The focused test failed.\n\nCode: test_failed\n\nRetry is available.",
    ],
    [
      followup(43006, "6".repeat(64), {
        outcome: "indeterminate",
        code: "receipt_unknown",
        message: "The durable effect could not be proven.",
        retryable: false,
      }),
      "Outcome indeterminate: The durable effect could not be proven.\n\nCode: receipt_unknown\n\nReconciliation is required before retrying.",
    ],
  ];

  for (const [jobEvent, expectedBody] of cases) {
    const projection = projectJobConversation(jobEvent);
    assert.equal(projection?.body, expectedBody);
    assert.equal(projection?.malformed, false);
  }
});

test("renders the request in the channel and real agent updates in its one task thread", () => {
  const accepted = followup(
    43002,
    "b".repeat(64),
    { claim: { status: "accepted", scope_digest: "d".repeat(64) } },
    [["e", REQUEST_ID, "", "root"]],
  );
  const progress = followup(
    43003,
    "c".repeat(64),
    { status: "progress", message: "Running focused tests.", evidence: [] },
    [
      ["e", REQUEST_ID, "", "root"],
      ["e", accepted.id, "", "reply"],
    ],
  );
  const result = followup(
    43004,
    "d".repeat(64),
    { outcome: "success", artifacts: [], evidence: ["contract:tests-pass"] },
    [
      ["e", REQUEST_ID, "", "root"],
      ["e", progress.id, "", "reply"],
    ],
  );
  const profiles = {
    [REQUESTER]: { displayName: "Ivy", ownerPubkey: null },
    [WORKER]: {
      displayName: "Clauditron",
      ownerPubkey: REQUESTER,
      isAgent: true,
    },
  };
  const members = [
    { pubkey: REQUESTER, role: "member", isAgent: false },
    { pubkey: WORKER, role: "bot", isAgent: true },
  ];

  const messages = formatTimelineMessages(
    [request(), accepted, progress, result],
    null,
    undefined,
    null,
    profiles,
    members,
  );

  assert.deepEqual(
    messages.map((message) => ({
      id: message.id,
      author: message.author,
      isAgent: message.isAgent,
      parentId: message.parentId,
      rootId: message.rootId,
      depth: message.depth,
    })),
    [
      {
        id: REQUEST_ID,
        author: "Ivy",
        isAgent: false,
        parentId: null,
        rootId: null,
        depth: 0,
      },
      {
        id: accepted.id,
        author: "Clauditron",
        isAgent: true,
        parentId: REQUEST_ID,
        rootId: REQUEST_ID,
        depth: 1,
      },
      {
        id: progress.id,
        author: "Clauditron",
        isAgent: true,
        parentId: REQUEST_ID,
        rootId: REQUEST_ID,
        depth: 1,
      },
      {
        id: result.id,
        author: "Clauditron",
        isAgent: true,
        parentId: REQUEST_ID,
        rootId: REQUEST_ID,
        depth: 1,
      },
    ],
  );
  assert.equal(
    messages.some((message) => message.body.includes(WORKER)),
    false,
  );

  const main = buildMainTimelineEntries(messages);
  assert.deepEqual(
    main.map((entry) => entry.message.id),
    [REQUEST_ID],
  );
  assert.equal(main[0].summary?.replyCount, 3);

  const thread = buildThreadPanelData(
    messages,
    REQUEST_ID,
    REQUEST_ID,
    new Set(),
  );
  assert.deepEqual(
    thread.visibleReplies.map((entry) => entry.message.id),
    [accepted.id, progress.id, result.id],
  );
});

test("deduplicates reconnect deliveries without inventing another task or update", () => {
  const root = request();
  const progress = followup(43003, "b".repeat(64), {
    status: "progress",
    message: "Still working.",
    evidence: [],
  });
  const messages = formatTimelineMessages(
    [root, root, progress, progress],
    null,
    undefined,
    null,
  );

  assert.deepEqual(
    messages.map((message) => message.id),
    [root.id, progress.id],
  );
  assert.equal(countTopLevelTimelineRows([root, root, progress, progress]), 1);
  assert.equal(buildMainTimelineEntries(messages)[0].summary?.replyCount, 1);
});

test("malformed job data stays truthful and uses a valid root tag when available", () => {
  const malformedRequest = event(43001, REQUEST_ID, "not json");
  const malformedUpdate = event(
    43003,
    "b".repeat(64),
    { status: "progress" },
    {
      tags: [
        ["h", CHANNEL_ID],
        ["e", REQUEST_ID, "", "root"],
      ],
    },
  );

  const messages = formatTimelineMessages(
    [malformedRequest, malformedUpdate],
    null,
    undefined,
    null,
  );
  assert.equal(
    messages[0].body,
    "Task request could not be displayed because its data is malformed.",
  );
  assert.equal(
    messages[1].body,
    "Task update could not be displayed because its data is malformed.",
  );
  assert.equal(messages[1].parentId, REQUEST_ID);
  assert.equal(messages[1].rootId, REQUEST_ID);
});

test("ordinary chat and explicit nested threads keep their existing text and ancestry", () => {
  const root = event(9, "7".repeat(64), "Human discussion", {
    pubkey: REQUESTER,
  });
  const reply = event(9, "8".repeat(64), "Agent response", {
    pubkey: WORKER,
    tags: [
      ["h", CHANNEL_ID],
      ["e", root.id, "", "reply"],
    ],
  });
  const nested = event(9, "9".repeat(64), "Human follow-up", {
    pubkey: REQUESTER,
    tags: [
      ["h", CHANNEL_ID],
      ["e", root.id, "", "root"],
      ["e", reply.id, "", "reply"],
    ],
  });

  const messages = formatTimelineMessages(
    [root, reply, nested],
    null,
    undefined,
    null,
  );
  assert.deepEqual(
    messages.map(({ body, parentId, rootId, depth }) => ({
      body,
      parentId,
      rootId,
      depth,
    })),
    [
      { body: "Human discussion", parentId: null, rootId: null, depth: 0 },
      {
        body: "Agent response",
        parentId: root.id,
        rootId: root.id,
        depth: 1,
      },
      {
        body: "Human follow-up",
        parentId: reply.id,
        rootId: root.id,
        depth: 2,
      },
    ],
  );
});
