import { expect, test } from "@playwright/test";
import { installMockBridge } from "../helpers/bridge";

const id = (n: number) => n.toString(16).padStart(64, "0");

test("organization groups original replies and UI undo restores hidden clutter", async ({
  page,
}, testInfo) => {
  await installMockBridge(page);
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await page.getByTestId("channel-general").click();
  await expect(page.getByTestId("chat-title")).toHaveText("general");
  const fixture = await page.evaluate(() => {
    const emit = window.__BUZZ_E2E_EMIT_MOCK_MESSAGE__;
    if (!emit) throw new Error("Disposable bridge unavailable");
    const eventId = (n: number) => n.toString(16).padStart(64, "0");
    const root = emit({
      channelName: "general",
      content: "Organization fixture destination",
      id: eventId(7001),
    });
    const source = emit({
      channelName: "general",
      content:
        "Original design discussion [source](https://example.org/reference) [file](https://example.org/design)",
      id: eventId(7002),
      extraTags: [
        [
          "imeta",
          "url https://example.org/design",
          "m text/html",
          "filename design.html",
        ],
      ],
    });
    emit({
      channelName: "general",
      content: "Original first reply",
      id: eventId(7003),
      parentEventId: source.id,
    });
    emit({
      channelName: "general",
      content: "Original nested reply",
      id: eventId(7004),
      parentEventId: eventId(7003),
    });
    const clutter = emit({
      channelName: "general",
      content: "Disposable redundant progress line",
      id: eventId(7005),
    });
    const organization = (n: number, action: unknown) =>
      emit({
        channelName: "general",
        kind: 40009,
        id: eventId(n),
        content: JSON.stringify({ version: 1, action }),
      });
    organization(7010, {
      type: "group",
      message_ids: [source.id],
      thread_root_id: root.id,
      title: "Design decisions",
      summary: "Two replies retained with their original discussion.",
    });
    organization(7011, {
      type: "hide",
      message_ids: [clutter.id],
      hidden: true,
    });
    const original = window.__TAURI_INTERNALS__.invoke;
    let seq = 7020;
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
      return organization(seq++, request.action);
    };
    return { root: root.id, source: source.id, clutter: clutter.id };
  });
  await expect(page.getByTestId("organization-history-open")).toContainText(
    "2",
  );
  await expect(
    page.locator(`[data-message-id="${fixture.clutter}"]`),
  ).toHaveCount(0);
  await expect(
    page.getByText("Design decisions", { exact: true }),
  ).toBeVisible();
  await page
    .getByTestId(`reply-message-${fixture.root}`)
    .click({ force: true });
  const thread = page.getByTestId("message-thread-panel");
  await expect(thread).toBeVisible();
  await expect(
    thread.locator(`[data-message-id="${fixture.source}"]`),
  ).toContainText("Original design discussion");
  await expect(
    thread.getByRole("link", { name: "source", exact: true }),
  ).toHaveAttribute("href", "https://example.org/reference");
  await expect(
    thread.getByRole("button", { name: "design.html", exact: true }),
  ).toBeVisible();
  await thread
    .locator(
      `[data-testid="message-thread-summary"][data-thread-head-id="${fixture.source}"]`,
    )
    .click();
  await expect(thread.locator(`[data-message-id="${id(7003)}"]`)).toContainText(
    "Original first reply",
  );
  await thread
    .locator(
      `[data-testid="message-thread-summary"][data-thread-head-id="${id(7003)}"]`,
    )
    .click();
  await expect(thread.locator(`[data-message-id="${id(7004)}"]`)).toContainText(
    "Original nested reply",
  );
  await page.screenshot({
    path: testInfo.outputPath("conversation-organization-thread.png"),
  });
  await page.getByTestId("organization-history-open").click();
  await expect(
    page.getByRole("heading", { name: "Organization history" }),
  ).toBeVisible();
  await page.getByRole("button", { name: /^Undo: Hid 1 message/ }).click();
  await expect(
    page.getByText("Undid an organization change", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: /^Undo: Grouped 1 message/ }).click();
  await expect(
    page.getByText("Undid an organization change", { exact: true }),
  ).toHaveCount(2);
  await page.screenshot({
    path: testInfo.outputPath("conversation-organization-undo.png"),
  });
  await page.keyboard.press("Escape");
  await expect(
    page.locator(`[data-message-id="${fixture.clutter}"]`),
  ).toContainText("Disposable redundant progress line");
  await expect(page.getByText("Design decisions", { exact: true })).toHaveCount(
    0,
  );
});
