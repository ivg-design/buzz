import { expect, test } from "@playwright/test";
import { installMockBridge } from "../helpers/bridge";

const agent =
  "554cef57437abac34522ac2c9f0490d685b72c80478cf9f7ed6f9570ee8624ea";

test("timed task action is available for an agent managed by someone else", async ({
  page,
}) => {
  await installMockBridge(page, {
    relayAgents: [
      {
        pubkey: agent,
        name: "Charlie",
        ownerPubkey: "a".repeat(64),
        respondTo: "anyone",
        channelNames: ["agents"],
      },
    ],
  });
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await page.getByTestId("channel-agents").click();
  await page
    .getByTestId("message-row")
    .filter({ hasText: "Indexing the channel catalog now." })
    .first()
    .getByTestId("message-author")
    .hover();
  await expect(
    page
      .getByTestId("user-profile-popover")
      .getByRole("button", { name: "Add timed task", exact: true }),
  ).toBeVisible();
});

test("timed task popover preserves instruction and supports edit pause resume cancel", async ({
  page,
}, testInfo) => {
  await installMockBridge(page, {
    managedAgents: [
      {
        pubkey: agent,
        name: "Charlie",
        status: "running",
        channelNames: ["agents"],
      },
    ],
  });
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await page.getByTestId("channel-agents").click();
  await expect(page.getByTestId("chat-title")).toHaveText("agents");
  // A disposable IPC fixture binds the production dialog/API calls. Native
  // SQLite scheduling and signed delivery are exercised by Rust tests separately.
  await page.evaluate(() => {
    const win = window as unknown as {
      __TAURI_INTERNALS__: {
        invoke: (
          command: string,
          args?: Record<string, unknown>,
        ) => Promise<unknown>;
      };
      __timedFixtureCalls: { command: string; args: Record<string, unknown> }[];
    };
    const original = win.__TAURI_INTERNALS__.invoke;
    let task: Record<string, unknown> | null = null;
    win.__timedFixtureCalls = [];
    win.__TAURI_INTERNALS__.invoke = async (command, args = {}) => {
      if (!command.startsWith("timed_tasks_")) return original(command, args);
      win.__timedFixtureCalls.push({ command, args });
      if (command === "timed_tasks_list") return task ? [task] : [];
      if (command === "timed_tasks_create")
        task = {
          ...(args.input as object),
          id: "fixture-schedule",
          threadId: "fixture-root",
          status: "active",
          deliveryState: "idle",
          deliveredCount: 0,
          missedCount: 0,
          lastError: null,
          nextRunAt: Date.now() + 60_000,
          lastDeliveredAt: null,
        };
      if (command === "timed_tasks_update")
        task = { ...task, ...(args.input as object) };
      if (command === "timed_tasks_set_status")
        task = { ...task, status: args.status };
      return task;
    };
  });
  const author = page
    .getByTestId("message-row")
    .filter({ hasText: "Indexing the channel catalog now." })
    .first()
    .getByTestId("message-author");
  await author.hover();
  const popover = page.getByTestId("user-profile-popover");
  await expect(popover).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("timed-task-popover.png"),
    animations: "disabled",
  });
  await popover
    .getByRole("button", { name: "Add timed task", exact: true })
    .click();
  const dialog = page.getByTestId("timed-task-dialog");
  await expect(dialog).toBeVisible();
  await expect(popover).not.toBeVisible();
  const instruction =
    "  Check available agents.\nPreserve @agent and `symbols`.  ";
  await dialog.getByLabel("Instruction", { exact: true }).fill(instruction);
  await dialog.getByLabel("Send every").fill("2");
  await dialog.getByLabel("Interval unit").selectOption("minutes");
  await dialog.getByLabel("Repeat", { exact: true }).selectOption("count");
  await dialog.getByLabel("Total runs").fill("3");
  await expect(dialog).toContainText("Timezone:");
  await page.screenshot({
    path: testInfo.outputPath("timed-task-editor.png"),
    animations: "disabled",
  });
  await dialog
    .getByRole("button", { name: "Add timed task", exact: true })
    .click();
  await expect(
    dialog.getByRole("button", { name: "Pause", exact: true }),
  ).toBeVisible();
  const created = await page.evaluate(
    () =>
      (
        window as unknown as {
          __timedFixtureCalls: {
            command: string;
            args: {
              input: {
                instruction: string;
                originEventId: string;
                channelId: string;
              };
            };
          }[];
        }
      ).__timedFixtureCalls.find(
        (call) => call.command === "timed_tasks_create",
      )?.args.input,
  );
  expect(created?.instruction).toBe(instruction);
  expect(created?.originEventId).toBeTruthy();
  expect(created?.channelId).toBe("94a444a4-c0a3-5966-ab05-530c6ddc2301");
  await dialog.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(
    dialog.getByRole("button", { name: "Resume", exact: true }),
  ).toBeVisible();
  await dialog.getByRole("button", { name: "Edit", exact: true }).click();
  await expect(dialog.getByLabel("Instruction", { exact: true })).toHaveValue(
    instruction,
  );
  await expect(
    dialog.getByLabel("Conversation", { exact: true }),
  ).toBeDisabled();
  await dialog.getByLabel("Send every").fill("4");
  await dialog
    .getByRole("button", { name: "Save changes", exact: true })
    .click();
  await expect(dialog).toContainText("Every 4 minutes");
  await dialog.getByRole("button", { name: "Resume", exact: true }).click();
  await expect(
    dialog.getByRole("button", { name: "Pause", exact: true }),
  ).toBeVisible();
  await dialog
    .getByRole("button", { name: "Cancel task", exact: true })
    .click();
  await expect(dialog).toContainText("cancelled");
  await expect(
    dialog.getByRole("button", { name: "Resume", exact: true }),
  ).not.toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("timed-task-list.png") });
});
