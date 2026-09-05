const JOB_REQUEST = 43001;
const JOB_ACCEPTED = 43002;
const JOB_PROGRESS = 43003;
const JOB_RESULT = 43004;
const JOB_CONTROL = 43005;
const JOB_ERROR = 43006;

const JOB_KINDS = new Set([
  JOB_REQUEST,
  JOB_ACCEPTED,
  JOB_PROGRESS,
  JOB_RESULT,
  JOB_CONTROL,
  JOB_ERROR,
]);
const EVENT_ID_RE = /^[0-9a-f]{64}$/i;

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function stringList(value) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    return null;
  }
  return value.map((item) => item.trim()).filter(Boolean);
}

function bulletSection(label, values) {
  if (!values?.length) return null;
  return `${label}:\n${values.map((value) => `- ${value}`).join("\n")}`;
}

function joinSections(...sections) {
  return sections.filter(Boolean).join("\n\n");
}

function malformedBody(kind) {
  return kind === JOB_REQUEST
    ? "Task request could not be displayed because its data is malformed."
    : "Task update could not be displayed because its data is malformed.";
}

function requestRootFromTags(tags) {
  if (!Array.isArray(tags)) return null;
  const root = tags.find(
    (tag) =>
      Array.isArray(tag) &&
      tag[0] === "e" &&
      tag[3] === "root" &&
      EVENT_ID_RE.test(tag[1] ?? ""),
  );
  return root?.[1] ?? null;
}

function requestBody(payload) {
  const summary = nonEmptyString(payload.summary);
  const acceptance = stringList(payload.acceptance);
  if (!summary || !acceptance?.length) return null;
  return summary;
}

const DECLINE_EXPLANATIONS = new Map([
  [
    "workspace_setup_failed",
    "I couldn't set up the workspace needed for this task.",
  ],
]);

function acceptedBody(payload) {
  if (!isRecord(payload.claim)) return null;
  const status = payload.claim.status;
  if (status === "processed") return "Received the task.";
  if (status === "accepted") return "Accepted the task.";
  if (status === "declined") {
    const reason = nonEmptyString(payload.claim.reason);
    if (!reason) return null;
    const explanation = DECLINE_EXPLANATIONS.get(reason);
    return explanation
      ? `Declined the task. ${explanation}`
      : "Declined the task.";
  }
  return null;
}

function progressBody(payload) {
  const message = nonEmptyString(payload.message);
  if (!message) return null;
  if (payload.status === "progress") return `Progress: ${message}`;
  if (payload.status === "blocked") return `Blocked: ${message}`;
  return null;
}

function resultBody(payload) {
  if (payload.outcome !== "success") return null;
  const hasSummary = Object.hasOwn(payload, "summary");
  const summary = hasSummary ? nonEmptyString(payload.summary) : null;
  const artifacts = stringList(payload.artifacts);
  const evidence = stringList(payload.evidence);
  if ((hasSummary && !summary) || artifacts === null || evidence === null)
    return null;
  return joinSections(
    "Completed successfully.",
    summary,
    bulletSection("Artifacts", artifacts),
    bulletSection("Evidence", evidence),
  );
}

function controlBody(payload) {
  const reason = nonEmptyString(payload.reason);
  if (!reason) return null;
  switch (payload.action) {
    case "cancel":
      return `Cancellation requested: ${reason}`;
    case "cancelled":
      return `Cancelled: ${reason}`;
    case "release":
      return `Released the task: ${reason}`;
    case "handoff":
      // The destination is a pubkey in the protocol body. The message row
      // already identifies the real author; keep raw identity keys in the
      // Activity detail surface until a verified display-name lookup exists.
      return `Handoff requested: ${reason}`;
    default:
      return null;
  }
}

function errorBody(payload) {
  const message = nonEmptyString(payload.message);
  const code = nonEmptyString(payload.code);
  if (!message || !code || typeof payload.retryable !== "boolean") return null;

  if (payload.outcome === "failed") {
    return joinSections(
      `Failed: ${message}`,
      payload.retryable ? "Retry is available." : "Retry is not available.",
    );
  }
  if (payload.outcome === "indeterminate") {
    if (payload.retryable !== false) return null;
    return joinSections(
      `Outcome indeterminate: ${message}`,
      "Reconciliation is required before retrying.",
    );
  }
  return null;
}

function formatBody(kind, payload) {
  switch (kind) {
    case JOB_REQUEST:
      return requestBody(payload);
    case JOB_ACCEPTED:
      return acceptedBody(payload);
    case JOB_PROGRESS:
      return progressBody(payload);
    case JOB_RESULT:
      return resultBody(payload);
    case JOB_CONTROL:
      return controlBody(payload);
    case JOB_ERROR:
      return errorBody(payload);
    default:
      return null;
  }
}

/**
 * Convert one durable A2A job event into ordinary conversation text and its
 * request-root relationship. This helper intentionally projects only fields
 * that are present on the signed event; lifecycle state is never inferred.
 */
export function projectJobConversation(event) {
  if (!JOB_KINDS.has(event?.kind)) return null;

  let payload = null;
  try {
    const parsed = JSON.parse(event.content);
    if (isRecord(parsed)) payload = parsed;
  } catch {
    // The clean malformed-data row below is the truthful fallback.
  }

  const body = payload ? formatBody(event.kind, payload) : null;
  const bodyRequestId = payload?.request_event_id;
  const requestEventId =
    event.kind === JOB_REQUEST
      ? null
      : EVENT_ID_RE.test(bodyRequestId ?? "")
        ? bodyRequestId
        : requestRootFromTags(event.tags);

  return {
    body: body ?? malformedBody(event.kind),
    requestEventId,
    malformed: body === null,
  };
}

export function isJobConversationKind(kind) {
  return JOB_KINDS.has(kind);
}
