export const INVITE_EXPIRED_ERROR = "invite_expired";
export const INVITE_EXHAUSTED_ERROR = "invite_exhausted";

/**
 * Parsed invite — either a full (relay + code) or bare-code form.
 *
 * URL inputs (`https://`, `http://`, `buzz://join`) always carry a
 * `relayWsUrl` (already normalised to `ws(s)://`).  A bare code (no scheme,
 * no slashes) omits it — the caller decides which relay to target.
 */
export type ParsedInvite =
  | { relayWsUrl: string; code: string }
  | { code: string };

const MAX_INVITE_INPUT_LEN = 4_096;
const MAX_INVITE_CODE_LEN = 1_024;
const BASE64URL_RE = /^[A-Za-z0-9_-]+$/;

function canonicalBase64Url(value: string, expectedBytes?: number): boolean {
  if (!BASE64URL_RE.test(value)) return false;
  try {
    const padded = `${value}${"=".repeat((4 - (value.length % 4)) % 4)}`
      .replaceAll("-", "+")
      .replaceAll("_", "/");
    const bytes = atob(padded);
    if (expectedBytes !== undefined && bytes.length !== expectedBytes)
      return false;
    const encoded = btoa(bytes)
      .replaceAll("+", "-")
      .replaceAll("/", "_")
      .replace(/=+$/, "");
    return encoded === value;
  } catch {
    return false;
  }
}

function isCanonicalInviteCode(code: string): boolean {
  if (!code || code.length > MAX_INVITE_CODE_LEN) return false;
  if (code.startsWith("v2.")) {
    return canonicalBase64Url(code.slice(3), 32);
  }
  const parts = code.split(".");
  return (
    parts.length === 2 &&
    canonicalBase64Url(parts[0] ?? "") &&
    canonicalBase64Url(parts[1] ?? "", 32)
  );
}

function canonicalRelayUrl(raw: string): string | null {
  if (!raw || raw.length > 2_048) return null;
  try {
    const relay = new URL(raw);
    if (
      (relay.protocol !== "ws:" && relay.protocol !== "wss:") ||
      relay.username ||
      relay.password ||
      relay.pathname !== "/" ||
      relay.search ||
      relay.hash ||
      relay.origin !== raw
    ) {
      return null;
    }
    return relay.origin;
  } catch {
    return null;
  }
}

/**
 * Parse an invite input into a structured form.
 *
 * Accepted input forms:
 *  - `https://<relay>/invite#code=<code>` → canonical v2 share link
 *  - `https://<relay>/invite/<code>` → legacy compatibility link
 *  - `buzz://join?relay=<wsUrl>&code=<code>` → `{ relayWsUrl, code }`
 *  - bare code (no `://`, no `/`)    → `{ code }`
 *
 * Returns `null` for empty input or inputs that don't match any form.
 */
export function parseInviteInput(input: string): ParsedInvite | null {
  const trimmed = input.trim();
  if (!trimmed || trimmed.length > MAX_INVITE_INPUT_LEN) return null;

  // Try URL-form parse first.
  try {
    const url = new URL(trimmed);

    // buzz://join?relay=...&code=...
    // Non-special schemes put the authority in `host`, not `pathname`.
    if (url.protocol === "buzz:") {
      const entries = Array.from(url.searchParams.entries());
      if (
        url.host !== "join" ||
        !["", "/"].includes(url.pathname) ||
        url.username ||
        url.password ||
        url.hash ||
        entries.length !== 2 ||
        entries.filter(([key]) => key === "relay").length !== 1 ||
        entries.filter(([key]) => key === "code").length !== 1
      )
        return null;
      const relay = canonicalRelayUrl(url.searchParams.get("relay") ?? "");
      const code = url.searchParams.get("code") ?? "";
      if (!relay || !isCanonicalInviteCode(code)) return null;
      return { relayWsUrl: relay, code };
    }

    // New links keep the bearer in one exact fragment field. URL fragments
    // never enter HTTP request targets or Referer headers.
    if (url.protocol === "https:" || url.protocol === "http:") {
      if (url.username || url.password || url.search) return null;
      const relayWsUrl =
        url.protocol === "https:" ? `wss://${url.host}` : `ws://${url.host}`;
      if (url.pathname === "/invite") {
        const fragment = Array.from(
          new URLSearchParams(url.hash.replace(/^#/, "")).entries(),
        );
        if (
          fragment.length !== 1 ||
          fragment[0]?.[0] !== "code" ||
          url.hash !== `#code=${fragment[0][1]}` ||
          !isCanonicalInviteCode(fragment[0][1])
        )
          return null;
        return { relayWsUrl, code: fragment[0][1] };
      }

      // Compatibility drain: one unescaped canonical v1/v2 code segment.
      if (url.hash) return null;
      const match = url.pathname.match(/^\/invite\/([^/]+)$/);
      if (!match?.[1] || !isCanonicalInviteCode(match[1])) return null;
      return { relayWsUrl, code: match[1] };
    }

    // ws/wss or any other scheme — not an invite URL.
    return null;
  } catch {
    // Not a URL — fall through to bare-code check.
  }

  // Bare codes are accepted for an already-selected relay, but only in a
  // canonical bounded v2/legacy-v1 form.
  if (
    trimmed.includes("://") ||
    trimmed.includes("/") ||
    !isCanonicalInviteCode(trimmed)
  )
    return null;
  return { code: trimmed };
}

/** Convert a ws(s) relay URL to its http(s) equivalent. */
export function relayHttpFromWs(wsUrl: string): string {
  if (wsUrl.startsWith("wss://")) return `https://${wsUrl.slice(6)}`;
  if (wsUrl.startsWith("ws://")) return `http://${wsUrl.slice(5)}`;
  throw new Error(`Expected ws:// or wss:// relay URL, got: ${wsUrl}`);
}

export function inviteErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : `${error}`;
}

export function isInviteExpiredError(error: unknown): boolean {
  return inviteErrorMessage(error) === INVITE_EXPIRED_ERROR;
}

export function isInviteExhaustedError(error: unknown): boolean {
  return inviteErrorMessage(error) === INVITE_EXHAUSTED_ERROR;
}
