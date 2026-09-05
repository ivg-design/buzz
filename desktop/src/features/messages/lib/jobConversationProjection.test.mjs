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
    [request(), "Task: Review the reconnect path"],
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
          reason: "workspace_setup_failed",
        },
      }),
      "Declined the task. I couldn't set up the workspace needed for this task. Check repository access and the agent's runtime log for details.",
    ],
    [
      followup(43002, "7".repeat(64), {
        claim: {
          status: "declined",
          scope_digest: "d".repeat(64),
          reason: "future_machine_reason",
        },
      }),
      "Declined the task.",
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
        summary: "The reconnect path preserves the provider session.",
        candidate_sha: "9".repeat(40),
        artifacts: ["Review notes attached"],
        evidence: ["Focused tests passed"],
      }),
      "Completed successfully.\n\nThe reconnect path preserves the provider session.\n\nArtifacts:\n- Review notes attached\n\nEvidence:\n- Focused tests passed",
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
      "Failed: The focused test failed.\n\nRetry is available.",
    ],
    [
      followup(43006, "6".repeat(64), {
        outcome: "indeterminate",
        code: "receipt_unknown",
        message: "The durable effect could not be proven.",
        retryable: false,
      }),
      "Outcome indeterminate: The durable effect could not be proven.\n\nReconciliation is required before retrying.",
    ],
  ];

  for (const [jobEvent, expectedBody] of cases) {
    const projection = projectJobConversation(jobEvent);
    assert.equal(projection?.body, expectedBody);
    assert.equal(projection?.malformed, false);
  }
});

test("keeps technical contract fields out of conversational rows", () => {
  const requestProjection = projectJobConversation(request());
  const resultProjection = projectJobConversation(
    followup(43004, "3".repeat(64), {
      outcome: "success",
      candidate_sha: "9".repeat(40),
      artifacts: [],
      evidence: [],
    }),
  );
  const errorProjection = projectJobConversation(
    followup(43006, "5".repeat(64), {
      outcome: "failed",
      code: "workspace_setup_failed",
      message: "I couldn't prepare the workspace.",
      retryable: false,
    }),
  );

  assert.equal(requestProjection?.body, "Task: Review the reconnect path");
  assert.equal(resultProjection?.body, "Completed successfully.");
  assert.equal(
    errorProjection?.body,
    "Failed: I couldn't prepare the workspace.\n\nRetry is not available.",
  );
  for (const body of [
    requestProjection?.body,
    resultProjection?.body,
    errorProjection?.body,
  ]) {
    assert.doesNotMatch(
      body ?? "",
      /Acceptance:|Recovery test passes|Candidate commit:|9999999999999999999999999999999999999999|Code:|workspace_setup_failed/,
    );
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
    {
      outcome: "success",
      summary: "The provider session resumed without replaying the prompt.",
      artifacts: [],
      evidence: ["contract:tests-pass"],
    },
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
  assert.equal(messages[0].taskThread, true);
  assert.equal(
    messages.slice(1).some((message) => message.taskThread),
    false,
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
  assert.equal(
    thread.visibleReplies.at(-1)?.message.body,
    "Completed successfully.\n\nThe provider session resumed without replaying the prompt.\n\nEvidence:\n- contract:tests-pass",
  );
  assert.equal(thread.visibleReplies.at(-1)?.message.author, "Clauditron");
});

test("exposes an empty task thread before the first lifecycle receipt arrives", () => {
  const messages = formatTimelineMessages([request()], null, undefined, null);
  const main = buildMainTimelineEntries(messages);

  assert.equal(messages[0].body, "Task: Review the reconnect path");
  assert.equal(messages[0].taskThread, true);
  assert.deepEqual(main[0].summary, {
    threadHeadId: REQUEST_ID,
    replyCount: 0,
    lastReplyAt: null,
    participants: [],
  });
  assert.deepEqual(
    buildThreadPanelData(messages, REQUEST_ID, REQUEST_ID, new Set())
      .visibleReplies,
    [],
  );
});

test("uses optional task title with the concise summary and omits acceptance internals", () => {
  const titled = event(43001, REQUEST_ID, {
    title: "Reconnect recovery",
    summary: "Verify the provider resumes without replaying the prompt.",
    acceptance: ["Internal criterion that belongs in Activity"],
  });
  const projection = projectJobConversation(titled);

  assert.equal(
    projection?.body,
    "Task: Reconnect recovery\n\nVerify the provider resumes without replaying the prompt.",
  );
  assert.doesNotMatch(projection?.body ?? "", /Internal criterion/);
  assert.equal(projection?.taskRoot, true);
});

test("renders a worker-signed human report as an ordinary reply in the task thread", () => {
  const report = event(9, "9".repeat(64), "Implemented and verified.", {
    pubkey: WORKER,
    tags: [
      ["h", CHANNEL_ID],
      ["e", REQUEST_ID, "", "reply"],
      ["buzz-task", "report", REQUEST_ID],
    ],
  });
  const profiles = {
    [WORKER]: { displayName: "Clauditron", isAgent: true },
  };
  const messages = formatTimelineMessages(
    [request(), report],
    null,
    undefined,
    null,
    profiles,
    [{ pubkey: WORKER, role: "bot", isAgent: true }],
  );
  const thread = buildThreadPanelData(
    messages,
    REQUEST_ID,
    REQUEST_ID,
    new Set(),
  );

  assert.equal(thread.visibleReplies.length, 1);
  assert.equal(
    thread.visibleReplies[0].message.body,
    "Implemented and verified.",
  );
  assert.equal(thread.visibleReplies[0].message.author, "Clauditron");
  assert.equal(thread.visibleReplies[0].message.isAgent, true);
});

test("keeps new machine lifecycle payloads in Activity and shows ordinary task-thread mirrors", () => {
  const taskRootId = "8".repeat(64);
  const conversation = {
    channel_id: CHANNEL_ID,
    thread_root_id: taskRootId,
  };
  const taskRoot = event(9, taskRootId, "Task: Reconnect recovery", {
    pubkey: REQUESTER,
    tags: [
      ["h", CHANNEL_ID],
      ["buzz-task", "root"],
    ],
  });
  const machineRequest = event(43001, REQUEST_ID, {
    conversation,
    summary: "Review the reconnect path",
    acceptance: ["Recovery test passes"],
  });
  const machineAccepted = followup(43002, "b".repeat(64), {
    conversation,
    claim: { status: "accepted", scope_digest: "d".repeat(64) },
  });
  const started = event(9, "7".repeat(64), "Started the task.", {
    pubkey: WORKER,
    tags: [
      ["h", CHANNEL_ID],
      ["e", taskRootId, "", "reply"],
      ["buzz-task", "lifecycle", machineAccepted.id],
    ],
  });

  assert.equal(projectJobConversation(machineRequest)?.hidden, true);
  assert.equal(projectJobConversation(machineAccepted)?.hidden, true);
  const messages = formatTimelineMessages(
    [taskRoot, machineRequest, machineAccepted, started],
    null,
    undefined,
    null,
  );
  assert.deepEqual(
    messages.map((message) => message.id),
    [taskRootId, started.id],
  );
  assert.equal(messages[0].taskThread, true);
  assert.equal(messages[1].body, "Started the task.");
  assert.equal(messages[1].rootId, taskRootId);
  assert.equal(countTopLevelTimelineRows([taskRoot, machineRequest]), 1);
});

test("opens task roots from stable markers or a top-level known-agent address", () => {
  const markedRoot = event(9, "3".repeat(64), "Assigned task", {
    tags: [
      ["h", CHANNEL_ID],
      ["buzz-task", "assignment", "operation-id"],
    ],
  });
  const addressedRoot = event(9, "4".repeat(64), "Please investigate", {
    tags: [
      ["h", CHANNEL_ID],
      ["p", WORKER],
    ],
  });
  const ordinaryMention = event(9, "5".repeat(64), "Hello", {
    tags: [
      ["h", CHANNEL_ID],
      ["p", "6".repeat(64)],
    ],
  });
  const messages = formatTimelineMessages(
    [markedRoot, addressedRoot, ordinaryMention],
    null,
    undefined,
    null,
    { [WORKER]: { displayName: "Clauditron", isAgent: true } },
  );

  assert.equal(messages[0].taskThread, true);
  assert.equal(messages[1].taskThread, true);
  assert.equal(messages[2].taskThread, false);
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
