import { expect, test } from "@playwright/test";

import { installMockBridge, openNewMessagePage } from "../helpers/bridge";

const AGENT_PUBKEY = "b".repeat(64);

test("DM chrome uses the managed name when the relay profile is missing", async ({
  page,
}) => {
  await installMockBridge(page, {
    managedAgents: [
      {
        name: "Codexitron",
        pubkey: AGENT_PUBKEY,
        status: "stopped",
      },
    ],
  });
  await page.goto("/");
  await openNewMessagePage(page);

  await page.getByTestId(`new-dm-result-${AGENT_PUBKEY}`).click();
  await page.getByTestId("message-input").fill("Name fallback regression");
  await page.getByTestId("send-message").click();

  await expect(page.getByTestId("chat-title")).toHaveText("Codexitron");
  await expect(page.getByTestId("dm-list")).toContainText("Codexitron");
  await expect(page.getByTestId("chat-header")).not.toContainText(AGENT_PUBKEY);
});
