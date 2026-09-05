import { expect, test } from "@playwright/test";
import { installMockBridge } from "../helpers/bridge";

const agent =
  "554cef57437abac34522ac2c9f0490d685b72c80478cf9f7ed6f9570ee8624ea";

for (const channelName of ["general", "watercooler"]) {
  test(`${channelName} thread People persists additions, removal and Undo through organization actions`, async ({
    page,
  }, testInfo) => {
    await installMockBridge(page, {
      relayAgents: [
        {
          pubkey: agent,
          name: "Charlie",
          ownerPubkey: "a".repeat(64),
          respondTo: "anyone",
          channelNames: ["agents"],
          status: "offline",
        },
      ],
    });
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.getByTestId(`channel-${channelName}`).click();
    await expect(page.getByTestId("chat-title")).toHaveText(channelName);
    const root = await page.evaluate((channelName) => {
      const emit = window.__BUZZ_E2E_EMIT_MOCK_MESSAGE__;
      if (!emit) throw new Error("Disposable bridge unavailable");
      const id = (n: number) => n.toString(16).padStart(64, "0");
      const root = emit({
        channelName,
        kind: channelName === "general" ? 9 : 45001,
        content: "Persistent participation fixture",
        id: id(8001),
      });
      emit({
        channelName,
        kind: channelName === "general" ? 9 : 45003,
        content: "Thread context stays intact",
        id: id(8002),
        parentEventId: root.id,
      });
      const original = window.__TAURI_INTERNALS__.invoke;
      let seq = 8010;
      window.__TAURI_INTERNALS__.invoke = async (command, args = {}) => {
        if (command !== "apply_conversation_organization")
          return original(command, args);
        const request = args as {
          channelId?: string;
          expectedSignerPubkey?: string;
          expectedRelayUrl?: string;
          action?: unknown;
        };
        if (
          !request.channelId ||
          !request.expectedSignerPubkey ||
          !request.expectedRelayUrl
        )
          throw new Error("Missing captured channel/identity scope");
        return emit({
          channelName,
          kind: 40009,
          id: id(seq++),
          content: JSON.stringify({ version: 1, action: request.action }),
        });
      };
      return root.id;
    }, channelName);
    if (channelName === "general") {
      await page.getByTestId(`reply-message-${root}`).click({ force: true });
      await expect(page.getByTestId("message-thread-panel")).toBeVisible();
    } else {
      await page
        .getByText("Persistent participation fixture", { exact: true })
        .click();
      await expect(
        page.getByRole("button", { name: "Back to posts" }),
      ).toBeVisible();
    }
    await page.getByTestId("thread-people-open").click();
    const people = page.getByTestId("thread-people-popover");
    const charlie = people.getByRole("checkbox", { name: /Charlie/ });
    await expect(charlie).toBeEnabled();
    await expect(people).toContainText(
      "No participant list has been saved yet.",
    );
    await expect(people).toContainText("Current work continues");
    await charlie.click();
    await expect(charlie).toBeChecked();
    await expect(page.getByTestId("thread-people-open")).toContainText(
      "People · 1",
    );
    await page.screenshot({
      path: testInfo.outputPath("thread-people-added.png"),
      animations: "disabled",
    });
    await page.keyboard.press("Escape");
    await page.getByTestId("thread-people-open").click();
    await expect(charlie).toBeChecked();
    await expect(charlie).toBeEnabled();
    await charlie.click();
    await expect(charlie).not.toBeChecked();
    await expect(people).toContainText("No agents participate automatically.");
    await page.keyboard.press("Escape");
    await page.getByTestId("organization-history-open").click();
    await page
      .getByRole("button", {
        name: "Undo: Removed automatic agent participation from a thread",
        exact: true,
      })
      .click();
    await expect(
      page.getByText("Undid an organization change", { exact: true }),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    await page.getByTestId("thread-people-open").click();
    await expect(charlie).toBeChecked();
    await expect(page.getByTestId("thread-people-open")).toContainText(
      "People · 1",
    );
    await page.screenshot({
      path: testInfo.outputPath("thread-people-undo.png"),
      animations: "disabled",
    });
    await people
      .getByRole("button", { name: "Remove all agents", exact: true })
      .click();
    await expect(charlie).not.toBeChecked();
    await expect(people).toContainText("No agents participate automatically.");
  });
}
