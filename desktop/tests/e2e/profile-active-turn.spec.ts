import { expect, test } from "@playwright/test";

import { installMockBridge } from "../helpers/bridge";

// Charlie is a `bot` member of #agents and authors the seeded "Indexing the
// channel catalog now." message (see e2eBridge.ts). Seeding a managed agent
// with this same pubkey makes the message avatar open a managed-agent profile
// panel — the surface that renders the active-turn badges.
const AGENT_PUBKEY =
  "554cef57437abac34522ac2c9f0490d685b72c80478cf9f7ed6f9570ee8624ea";

// Channel IDs the seeded turns point at. The badge labels resolve these to
// #general / #engineering via the channels query.
const CHANNEL_GENERAL = "9a1657ac-f7aa-5db0-b632-d8bbeb6dfb50";
const CHANNEL_ENGINEERING = "1c7e1c02-87bb-5e88-b2da-5a7a9432d0c9";

function seedAgent() {
  return {
    managedAgents: [
      {
        pubkey: AGENT_PUBKEY,
        name: "Charlie",
        status: "running" as const,
        channelNames: ["agents"],
      },
    ],
  };
}

async function waitForBridge(page: import("@playwright/test").Page) {
  await page.waitForFunction(
    () =>
      typeof (window as Window & { __BUZZ_E2E_SEED_ACTIVE_TURNS__?: unknown })
        .__BUZZ_E2E_SEED_ACTIVE_TURNS__ === "function",
    null,
    { timeout: 10_000 },
  );
}

async function openAgentsChannel(page: import("@playwright/test").Page) {
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await waitForBridge(page);
  await page.getByTestId("channel-agents").click();
  await expect(page.getByTestId("chat-title")).toHaveText("agents");
}

async function seedActiveTurns(
  page: import("@playwright/test").Page,
  turns: { channelId: string; turnId: string }[],
) {
  await page.evaluate(
    ({ pubkey, seeds }) => {
      const win = window as Window & {
        __BUZZ_E2E_SEED_ACTIVE_TURNS__?: (input: {
          agentPubkey: string;
          channelId: string;
          turnId: string;
        }) => void;
      };
      for (const { channelId, turnId } of seeds) {
        win.__BUZZ_E2E_SEED_ACTIVE_TURNS__?.({
          agentPubkey: pubkey,
          channelId,
          turnId,
        });
      }
    },
    { pubkey: AGENT_PUBKEY, seeds: turns },
  );
}

// The agent's avatar is the popover trigger inside its message row; clicking it
// opens the profile panel, hovering opens the popover.
function agentAvatar(page: import("@playwright/test").Page) {
  return page
    .getByTestId("message-row")
    .filter({ has: page.locator('[data-testid^="message-avatar-"]') })
    .last()
    .getByRole("button")
    .first();
}

test.describe("profile active turn indicator", () => {
  test.use({ viewport: { width: 1280, height: 720 } });

  test("01 — profile panel: agent working in one channel", async ({ page }) => {
    await installMockBridge(page, seedAgent());
    await openAgentsChannel(page);
    await seedActiveTurns(page, [
      { channelId: CHANNEL_GENERAL, turnId: "turn-101" },
    ]);

    await agentAvatar(page).click();

    const panel = page.getByTestId("user-profile-panel");
    await expect(panel).toBeVisible();
    const liveActivity = panel.getByTestId(
      `user-profile-live-activity-${AGENT_PUBKEY}`,
    );
    await expect(liveActivity).toBeVisible({ timeout: 5_000 });
    await expect(liveActivity).toContainText("Latest Activity");
    await expect(
      liveActivity.getByTestId("user-profile-activity-channel-label"),
    ).toContainText("#general");
  });

  test("02 — profile panel: agent working in two channels", async ({
    page,
  }) => {
    await installMockBridge(page, seedAgent());
    await openAgentsChannel(page);
    await seedActiveTurns(page, [
      { channelId: CHANNEL_GENERAL, turnId: "turn-201" },
      { channelId: CHANNEL_ENGINEERING, turnId: "turn-202" },
    ]);

    await agentAvatar(page).click();

    const panel = page.getByTestId("user-profile-panel");
    await expect(panel).toBeVisible();
    const liveActivity = panel.getByTestId(
      `user-profile-live-activity-${AGENT_PUBKEY}`,
    );
    await expect(liveActivity).toBeVisible({ timeout: 5_000 });
    await expect(liveActivity).toContainText("Latest Activity");
    // One carousel dot per working channel.
    await expect(
      panel.getByTestId(`user-profile-activity-dot-${CHANNEL_GENERAL}`),
    ).toBeVisible();
    await expect(
      panel.getByTestId(`user-profile-activity-dot-${CHANNEL_ENGINEERING}`),
    ).toBeVisible();
  });

  test("03 — hover popover: agent working", async ({ page }) => {
    await installMockBridge(page, seedAgent());
    await openAgentsChannel(page);
    await seedActiveTurns(page, [
      { channelId: CHANNEL_GENERAL, turnId: "turn-301" },
    ]);

    await agentAvatar(page).hover();

    const popover = page.getByTestId("user-profile-popover");
    await expect(popover).toBeVisible({ timeout: 5_000 });
    await expect(popover).toContainText("Working in #general");
  });
});

const CHANNEL_AGENTS = "94a444a4-c0a3-5966-ab05-530c6ddc2301";

function agentAuthor(page: import("@playwright/test").Page) {
  return page
    .getByTestId("message-row")
    .filter({ hasText: "Indexing the channel catalog now." })
    .first()
    .getByTestId("message-author");
}

async function hoverAgent(page: import("@playwright/test").Page) {
  await agentAuthor(page).hover();
  const popover = page.getByTestId("user-profile-popover");
  await expect(popover).toBeVisible();
  await popover.hover();
  return popover;
}

async function controls(page: import("@playwright/test").Page) {
  return page.evaluate(() => window.__BUZZ_E2E_OBSERVER_CONTROLS__ ?? []);
}

test.describe("agent hover Stop", () => {
  test.use({ viewport: { width: 1280, height: 720 } });

  test("idle agent hides Stop; author hover stops current conversation and preserves it", async ({
    page,
  }) => {
    await installMockBridge(page, seedAgent());
    await openAgentsChannel(page);
    let popover = await hoverAgent(page);
    await expect(popover.getByTestId("agent-popover-stop-actions")).toHaveCount(
      0,
    );
    await seedActiveTurns(page, [
      { channelId: CHANNEL_GENERAL, turnId: "another-conversation" },
      { channelId: CHANNEL_AGENTS, turnId: "current-conversation" },
    ]);
    const before = page.url();
    popover = page.getByTestId("user-profile-popover");
    const stop = popover.getByTestId(`agent-popover-stop-${CHANNEL_AGENTS}`);
    await expect(stop).toHaveText("Stop");
    await expect(
      popover.getByTestId(`agent-popover-stop-${CHANNEL_GENERAL}`),
    ).toHaveCount(0);
    // Keyboard activation exercises the same native button as pointer input.
    await stop.focus();
    await stop.press("Enter");
    await expect
      .poll(() => controls(page))
      .toEqual([
        {
          agentPubkey: AGENT_PUBKEY,
          payload: {
            type: "cancel_turn",
            channelId: CHANNEL_AGENTS,
            requestId: expect.any(String),
          },
        },
      ]);
    await expect(page.getByText(/Stop signal sent to Charlie/)).toBeVisible();
    await expect(popover).toBeVisible();
    expect(page.url()).toBe(before);
    await expect(page.getByTestId("agent-session-thread-panel")).toHaveCount(0);
    await expect(page.getByTestId("user-profile-panel")).toHaveCount(0);
    await expect(page.getByTestId("chat-title")).toHaveText("agents");
  });

  test("Stop from a thread reply keeps the thread and its draft mounted", async ({
    page,
  }) => {
    await installMockBridge(page, seedAgent());
    await openAgentsChannel(page);
    const rootId = await page.evaluate((pubkey) => {
      const root = window.__BUZZ_E2E_EMIT_MOCK_MESSAGE__?.({
        channelName: "agents",
        content: "Hover Stop thread root",
      });
      if (!root) throw new Error("Missing seeded thread");
      window.__BUZZ_E2E_EMIT_MOCK_MESSAGE__?.({
        channelName: "agents",
        content: "Agent work in this thread",
        parentEventId: root.id,
        pubkey,
      });
      return root.id;
    }, AGENT_PUBKEY);
    await page
      .locator(
        `[data-testid="message-thread-summary"][data-thread-head-id="${rootId}"]`,
      )
      .click();
    const thread = page.getByTestId("message-thread-panel");
    await expect(thread).toBeVisible();
    const draft = thread.getByTestId("message-input");
    await draft.fill("Keep this thread draft");
    await seedActiveTurns(page, [
      { channelId: CHANNEL_AGENTS, turnId: "thread-work" },
    ]);
    await thread
      .getByTestId("message-row")
      .filter({ hasText: "Agent work in this thread" })
      .getByTestId("message-author")
      .hover();
    const popover = page.getByTestId("user-profile-popover");
    await expect(popover).toBeVisible();
    const before = page.url();
    await popover.getByTestId(`agent-popover-stop-${CHANNEL_AGENTS}`).click();
    await expect(page.getByText(/Stop signal sent to Charlie/)).toBeVisible();
    await expect(thread).toBeVisible();
    await expect(draft).toHaveText("Keep this thread draft");
    await expect(page.getByTestId("agent-session-thread-panel")).toHaveCount(0);
    expect(page.url()).toBe(before);
    await page.screenshot({ path: "test-results/agent-hover-stop-thread.png" });
  });

  test("literal mention hover prefers its conversation over other active channels", async ({
    page,
  }) => {
    await installMockBridge(page, seedAgent());
    await openAgentsChannel(page);
    await page.evaluate((pubkey) => {
      window.__BUZZ_E2E_EMIT_MOCK_MESSAGE__?.({
        channelName: "agents",
        content: "Literal mention Stop check for @charlie.",
        mentionPubkeys: [pubkey],
      });
    }, AGENT_PUBKEY);
    await seedActiveTurns(page, [
      { channelId: CHANNEL_GENERAL, turnId: "other-channel" },
      { channelId: CHANNEL_AGENTS, turnId: "mention-conversation" },
    ]);
    const message = page
      .getByTestId("message-row")
      .filter({ hasText: "Literal mention Stop check" });
    await message.locator("[data-mention].agent-mention-highlight").hover();
    const popover = page.getByTestId("user-profile-popover");
    await expect(popover).toBeVisible();
    const stop = popover.getByTestId(`agent-popover-stop-${CHANNEL_AGENTS}`);
    await expect(stop).toHaveText("Stop");
    await expect(
      popover.getByTestId(`agent-popover-stop-${CHANNEL_GENERAL}`),
    ).toHaveCount(0);
    const before = page.url();
    await stop.click();
    await expect
      .poll(() => controls(page))
      .toEqual([
        {
          agentPubkey: AGENT_PUBKEY,
          payload: {
            type: "cancel_turn",
            channelId: CHANNEL_AGENTS,
            requestId: expect.any(String),
          },
        },
      ]);
    await expect(page.getByText(/Stop signal sent to Charlie/)).toBeVisible();
    await expect(page.getByTestId("agent-session-thread-panel")).toHaveCount(0);
    expect(page.url()).toBe(before);
  });

  test("multiple other active channels offer an explicit target in the hover card", async ({
    page,
  }) => {
    await installMockBridge(page, seedAgent());
    await openAgentsChannel(page);
    await seedActiveTurns(page, [
      { channelId: CHANNEL_GENERAL, turnId: "general-work" },
      { channelId: CHANNEL_ENGINEERING, turnId: "engineering-work" },
    ]);
    const popover = await hoverAgent(page);
    const stop = popover.getByTestId(
      `agent-popover-stop-${CHANNEL_ENGINEERING}`,
    );
    await expect(stop).toHaveText("Stop in #engineering");
    await expect(
      popover.getByTestId(`agent-popover-stop-${CHANNEL_GENERAL}`),
    ).toHaveText("Stop in #general");
    await stop.click();
    await expect
      .poll(() => controls(page))
      .toEqual([
        {
          agentPubkey: AGENT_PUBKEY,
          payload: {
            type: "cancel_turn",
            channelId: CHANNEL_ENGINEERING,
            requestId: expect.any(String),
          },
        },
      ]);
    await expect(page.getByTestId("chat-title")).toHaveText("agents");
  });

  test("pending Stop ignores repeat clicks and unrelated request results then reports unconfirmed", async ({
    page,
  }) => {
    await installMockBridge(page, {
      ...seedAgent(),
      observerControlResults: [
        { type: "cancel_turn", status: "sent", requestId: "old-request" },
      ],
    });
    await openAgentsChannel(page);
    await seedActiveTurns(page, [
      { channelId: CHANNEL_AGENTS, turnId: "pending-work" },
    ]);
    const popover = await hoverAgent(page);
    const stop = popover.getByTestId(`agent-popover-stop-${CHANNEL_AGENTS}`);
    await page.clock.install();
    await stop.evaluate((button) => {
      button.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      button.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    });
    await expect.poll(() => controls(page)).toHaveLength(1);
    await expect(stop).toBeDisabled();
    await expect(stop).toHaveText("Stopping…");
    await expect(stop).toHaveAttribute("aria-busy", "true");
    await expect(page.getByText(/Stop signal sent to/)).toHaveCount(0);
    await page.clock.fastForward(8_001);
    await expect(page.getByText(/hasn't confirmed it/)).toBeVisible();
    await expect(stop).toBeEnabled();
    await expect(page.getByTestId("chat-title")).toHaveText("agents");
  });

  for (const [status, feedback] of [
    ["ambiguous_target", /multiple agent sessions/],
    ["no_active_turn", /No active turn to stop/],
  ] as const) {
    test(`hover Stop reports ${status} without claiming success`, async ({
      page,
    }) => {
      await installMockBridge(page, {
        ...seedAgent(),
        observerControlResults: [{ type: "cancel_turn", status }],
      });
      await openAgentsChannel(page);
      await seedActiveTurns(page, [
        { channelId: CHANNEL_AGENTS, turnId: "work" },
      ]);
      const popover = await hoverAgent(page);
      const stop = popover.getByTestId(`agent-popover-stop-${CHANNEL_AGENTS}`);
      await stop.click();
      await expect(page.getByText(feedback)).toBeVisible();
      await expect(page.getByText(/Stop signal sent to/)).toHaveCount(0);
      await expect(stop).toBeEnabled();
      await expect(popover).toBeVisible();
    });
  }
});
