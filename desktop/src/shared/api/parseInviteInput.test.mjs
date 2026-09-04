import assert from "node:assert/strict";
import test from "node:test";

import { parseInviteInput } from "./inviteHelpers.ts";

const V2_CODE = `v2.${"A".repeat(43)}`;
const V1_CODE = `e30.${"A".repeat(43)}`;

test("new HTTPS invite uses one exact code fragment", () => {
  assert.deepEqual(
    parseInviteInput(`https://relay.example.com/invite#code=${V2_CODE}`),
    { relayWsUrl: "wss://relay.example.com", code: V2_CODE },
  );
  assert.deepEqual(
    parseInviteInput(`http://localhost:3000/invite#code=${V2_CODE}`),
    { relayWsUrl: "ws://localhost:3000", code: V2_CODE },
  );
});

test("legacy one-segment HTTPS paths remain compatible and bounded", () => {
  for (const code of [V1_CODE, V2_CODE]) {
    assert.deepEqual(
      parseInviteInput(`https://relay.example.com:8443/invite/${code}`),
      { relayWsUrl: "wss://relay.example.com:8443", code },
    );
  }
  assert.equal(
    parseInviteInput(`https://relay.example.com/invite/${V1_CODE}/`),
    null,
  );
  assert.equal(
    parseInviteInput(`https://relay.example.com/invite/${V1_CODE}/extra`),
    null,
  );
});

test("fragment links reject duplicate, unknown, encoded, query, and malformed fields", () => {
  for (const input of [
    `https://relay.example.com/invite#code=${V2_CODE}&code=${V2_CODE}`,
    `https://relay.example.com/invite#code=${V2_CODE}&next=/`,
    `https://relay.example.com/invite#next=/&code=${V2_CODE}`,
    `https://relay.example.com/invite#code=%76%32.${"A".repeat(43)}`,
    `https://relay.example.com/invite?code=${V2_CODE}`,
    "https://relay.example.com/invite#code=v2.short",
    "https://relay.example.com/invite#code=e30.bad",
    "https://relay.example.com/invite#section",
    "https://relay.example.com/invite",
  ]) {
    assert.equal(parseInviteInput(input), null, input);
  }
});

test("HTTPS invite rejects credentials and non-invite paths", () => {
  for (const input of [
    `https://user:pass@relay.example.com/invite#code=${V2_CODE}`,
    `https://relay.example.com/api/invites/${V2_CODE}`,
    "https://relay.example.com/",
    "wss://relay.example.com",
    "ftp://relay.example.com/invite",
  ]) {
    assert.equal(parseInviteInput(input), null, input);
  }
});

test("buzz join accepts an exact canonical relay and invite code", () => {
  assert.deepEqual(
    parseInviteInput(
      `buzz://join?relay=wss%3A%2F%2Frelay.example.com&code=${V2_CODE}`,
    ),
    { relayWsUrl: "wss://relay.example.com", code: V2_CODE },
  );
  assert.deepEqual(
    parseInviteInput(`buzz://join?relay=ws://localhost:3000&code=${V1_CODE}`),
    { relayWsUrl: "ws://localhost:3000", code: V1_CODE },
  );
});

test("buzz join rejects ambiguous fields and noncanonical relay authorities", () => {
  for (const input of [
    `buzz://join?relay=wss://relay.example.com&code=${V2_CODE}&code=${V2_CODE}`,
    `buzz://join?relay=wss://relay.example.com&code=${V2_CODE}&extra=x`,
    `buzz://join/path?relay=wss://relay.example.com&code=${V2_CODE}`,
    `buzz://join?relay=wss://user:pass@relay.example.com&code=${V2_CODE}`,
    `buzz://join?relay=wss://relay.example.com/path&code=${V2_CODE}`,
    `buzz://join?relay=wss://relay.example.com/?x=1&code=${V2_CODE}`,
    `buzz://join?relay=wss://Relay.Example&code=${V2_CODE}`,
    `buzz://join?relay=wss://relay.example.com/&code=${V2_CODE}`,
    `buzz://join?relay=https://relay.example.com&code=${V2_CODE}`,
    `buzz://join?code=${V2_CODE}`,
    "buzz://join?relay=wss://relay.example.com",
  ]) {
    assert.equal(parseInviteInput(input), null, input);
  }
});

test("bare invite codes are canonical and trimmed", () => {
  assert.deepEqual(parseInviteInput(`  ${V2_CODE}  `), { code: V2_CODE });
  assert.deepEqual(parseInviteInput(V1_CODE), { code: V1_CODE });
  for (const input of ["", "abc123", "v2.short", "e30.bad", "not/a/code"]) {
    assert.equal(parseInviteInput(input), null, input);
  }
});

test("oversized invite input and code are rejected before use", () => {
  assert.equal(parseInviteInput("a".repeat(4_097)), null);
  assert.equal(
    parseInviteInput(
      `https://relay.example.com/invite#code=v2.${"A".repeat(1_022)}`,
    ),
    null,
  );
});
