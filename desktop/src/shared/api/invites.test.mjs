import assert from "node:assert/strict";
import test from "node:test";

import {
  getJoinPolicy,
  listPendingInvites,
  mintInvite,
  revokeInvite,
} from "./invites.ts";

function withFetch(response, run) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    assert.equal(url, "https://relay.example/api/join-policy");
    return response;
  };
  return Promise.resolve(run()).finally(() => {
    globalThis.fetch = originalFetch;
  });
}

test("getJoinPolicy maps relay-hosted Markdown and age requirements", async () => {
  await withFetch(
    new Response(
      JSON.stringify({
        policy: {
          terms_markdown: "# Terms",
          privacy_markdown: "# Privacy",
          age_attestation_required: true,
          version: "policy-v1",
        },
      }),
      { status: 200 },
    ),
    async () => {
      assert.deepEqual(await getJoinPolicy("wss://relay.example", "webview"), {
        termsMarkdown: "# Terms",
        privacyMarkdown: "# Privacy",
        ageAttestationRequired: true,
        version: "policy-v1",
      });
    },
  );
});

test("getJoinPolicy preserves opt-in behavior for unconfigured and older relays", async () => {
  await withFetch(new Response(JSON.stringify({}), { status: 200 }), async () =>
    assert.equal(await getJoinPolicy("wss://relay.example", "webview"), null),
  );
  await withFetch(new Response(null, { status: 404 }), async () =>
    assert.equal(await getJoinPolicy("wss://relay.example", "webview"), null),
  );
});

test("getJoinPolicy fails closed on a policy endpoint error", async () => {
  await withFetch(new Response(null, { status: 503 }), async () =>
    assert.rejects(getJoinPolicy("wss://relay.example", "webview"), /HTTP 503/),
  );
});

test("getJoinPolicy maps the native command response", async () => {
  const previousWindow = globalThis.window;
  globalThis.window = {
    __TAURI_INTERNALS__: {
      invoke(command, args) {
        assert.equal(command, "fetch_join_policy");
        assert.deepEqual(args, { relayUrl: "wss://relay.example" });
        return Promise.resolve({
          terms_markdown: "# Terms",
          privacy_markdown: "# Privacy",
          age_attestation_required: true,
          version: "policy-v1",
        });
      },
    },
  };

  try {
    assert.deepEqual(await getJoinPolicy("wss://relay.example", "native"), {
      termsMarkdown: "# Terms",
      privacyMarkdown: "# Privacy",
      ageAttestationRequired: true,
      version: "policy-v1",
    });
  } finally {
    globalThis.window = previousWindow;
  }
});

// --- mintInvite serialization ---

// The test-loader transpiles TS imports. tauri.ts imports `invoke` from
// @tauri-apps/api/core, which calls `window.__TAURI_INTERNALS__.invoke`.
// We stub that here so getRelayHttpUrl() and signRelayEvent() work in node.

function setupTauriStubs(
  httpBase,
  authEvent = {
    id: "x",
    sig: "y",
    pubkey: "z",
    kind: 27235,
    created_at: 1,
    tags: [],
  },
) {
  const calls = { invokeArgs: [] };
  globalThis.window = globalThis.window ?? {};
  globalThis.window.__TAURI_INTERNALS__ = {
    invoke: async (command, args) => {
      calls.invokeArgs.push({ command, args });
      if (command === "get_relay_http_url") return httpBase;
      if (command === "sign_event") return JSON.stringify(authEvent);
      throw new Error(`Unexpected Tauri command: ${command}`);
    },
  };
  return calls;
}

function teardownTauriStubs() {
  delete globalThis.window.__TAURI_INTERNALS__;
}

test("mintInvite serializes bounded max_uses in the request body", async () => {
  setupTauriStubs("https://relay.example");
  try {
    const originalFetch = globalThis.fetch;
    let capturedBody;
    globalThis.fetch = async (_url, init) => {
      capturedBody = JSON.parse(init.body);
      return new Response(
        JSON.stringify({
          id: "123e4567-e89b-12d3-a456-426614174000",
          code: "v2.abc123",
          expires_at: 1785100000,
          url: "https://relay.example/invite/v2.abc123",
          max_uses: 10,
          uses_remaining: 10,
        }),
      );
    };
    try {
      const result = await mintInvite({ ttlSecs: 259200, maxUses: 10 });
      assert.equal(capturedBody.ttl_secs, 259200);
      assert.equal(capturedBody.max_uses, 10);
      assert.equal(result.code, "v2.abc123");
      assert.equal(result.id, "123e4567-e89b-12d3-a456-426614174000");
      assert.equal(result.maxUses, 10);
      assert.equal(result.usesRemaining, 10);
      assert.equal(result.expiresAt, 1785100000);
      assert.equal(result.url, "https://relay.example/invite/v2.abc123");
    } finally {
      globalThis.fetch = originalFetch;
    }
  } finally {
    teardownTauriStubs();
  }
});

test("mintInvite sends the server-enforced single-use default", async () => {
  setupTauriStubs("https://relay.example");
  try {
    const originalFetch = globalThis.fetch;
    let capturedBody;
    globalThis.fetch = async (_url, init) => {
      capturedBody = JSON.parse(init.body);
      return new Response(
        JSON.stringify({
          id: "123e4567-e89b-12d3-a456-426614174001",
          code: "v2.abc123",
          expires_at: 1785100000,
          url: "https://relay.example/invite/v2.abc123",
          max_uses: 1,
          uses_remaining: 1,
        }),
      );
    };
    try {
      const result = await mintInvite({ ttlSecs: 259200 });
      assert.equal(capturedBody.ttl_secs, 259200);
      assert.equal(capturedBody.max_uses, 1);
      assert.equal(result.maxUses, 1);
      assert.equal(result.usesRemaining, 1);
    } finally {
      globalThis.fetch = originalFetch;
    }
  } finally {
    teardownTauriStubs();
  }
});

test("mintInvite defaults max_uses even when no options are provided", async () => {
  setupTauriStubs("https://relay.example");
  try {
    const originalFetch = globalThis.fetch;
    let capturedBody;
    globalThis.fetch = async (_url, init) => {
      capturedBody = JSON.parse(init.body);
      return new Response(
        JSON.stringify({
          id: "123e4567-e89b-12d3-a456-426614174002",
          code: "v2.abc123",
          expires_at: 1785100000,
          url: "https://relay.example/invite/v2.abc123",
          max_uses: 1,
          uses_remaining: 1,
        }),
      );
    };
    try {
      await mintInvite();
      assert.equal(Object.hasOwn(capturedBody, "ttl_secs"), false);
      assert.equal(capturedBody.max_uses, 1);
    } finally {
      globalThis.fetch = originalFetch;
    }
  } finally {
    teardownTauriStubs();
  }
});

test("listPendingInvites uses an exact signed GET and maps redacted metadata", async () => {
  const calls = setupTauriStubs("https://relay.example");
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    assert.equal(url, "https://relay.example/api/invites");
    assert.equal(init.method, "GET");
    assert.equal(init.body, undefined);
    assert.match(init.headers.Authorization, /^Nostr /);
    return new Response(
      JSON.stringify({
        invites: [
          {
            id: "123e4567-e89b-12d3-a456-426614174000",
            expires_at: 1785100000,
            max_uses: 1,
            use_count: 0,
            uses_remaining: 1,
            created_by: "ab".repeat(32),
            created_at: 1785000000,
          },
        ],
      }),
    );
  };
  try {
    assert.deepEqual(await listPendingInvites(), [
      {
        id: "123e4567-e89b-12d3-a456-426614174000",
        expiresAt: 1785100000,
        maxUses: 1,
        useCount: 0,
        usesRemaining: 1,
        createdBy: "ab".repeat(32),
        createdAt: 1785000000,
      },
    ]);
    const signCall = calls.invokeArgs.find(
      ({ command }) => command === "sign_event",
    );
    assert.ok(signCall);
    assert.deepEqual(signCall.args.tags.slice(0, 2), [
      ["u", "https://relay.example/api/invites"],
      ["method", "GET"],
    ]);
    assert.equal(
      signCall.args.tags.some(([name]) => name === "payload"),
      false,
    );
  } finally {
    globalThis.fetch = originalFetch;
    teardownTauriStubs();
  }
});

test("revokeInvite signs DELETE for the exact encoded invite id", async () => {
  const calls = setupTauriStubs("https://relay.example");
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    assert.equal(url, "https://relay.example/api/invites/invite%2Fid");
    assert.equal(init.method, "DELETE");
    assert.equal(init.body, undefined);
    return new Response(JSON.stringify({ status: "revoked" }));
  };
  try {
    await revokeInvite("invite/id");
    const signCall = calls.invokeArgs.find(
      ({ command }) => command === "sign_event",
    );
    assert.ok(signCall);
    assert.deepEqual(signCall.args.tags.slice(0, 2), [
      ["u", "https://relay.example/api/invites/invite%2Fid"],
      ["method", "DELETE"],
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    teardownTauriStubs();
  }
});
