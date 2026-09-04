import { expect, test, type Page } from "@playwright/test";

import { installMockBridge } from "../helpers/bridge";
import { openSettings } from "../helpers/settings";

const INVITE_CODE = `v2.${"A".repeat(43)}`;
const INVITE_URL = `https://relay.example/invite#code=${INVITE_CODE}`;

let invitePayloads: Record<string, unknown>[];

function inviteResponse(sequence = 1) {
  return {
    id: `123e4567-e89b-12d3-a456-${String(sequence).padStart(12, "0")}`,
    code: INVITE_CODE,
    expires_at: Math.floor(Date.now() / 1000) + 86_400,
    max_uses: 1,
    uses_remaining: 1,
    url: sequence === 1 ? INVITE_URL : `${INVITE_URL}-${sequence}`,
  };
}

async function openInviteDialog(page: Page) {
  await openSettings(page, "community-members");
  await page.getByTestId("community-invite-dialog-trigger").click();
  await expect(page.getByTestId("community-invite-dialog")).toBeVisible();
}

test.beforeEach(async ({ page }) => {
  invitePayloads = [];
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"], {
    origin: "http://127.0.0.1:4173",
  });
  await installMockBridge(page, { relayRequiresMembership: true });
  await page.route("**/api/invites", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({
        contentType: "application/json",
        json: { invites: [] },
        status: 200,
      });
      return;
    }
    invitePayloads.push(route.request().postDataJSON());
    await route.fulfill({
      contentType: "application/json",
      json: inviteResponse(),
      status: 200,
    });
  });
});

test("StrictMode mount stays idle and one explicit Create enables Copy", async ({
  page,
}) => {
  await page.goto("/");
  await openInviteDialog(page);

  const action = page.getByTestId("copy-invite-link");
  const field = page.getByTestId("invite-link-url");
  await expect(field).toHaveValue("");
  await expect(field).toHaveAttribute(
    "placeholder",
    "Create a link when you’re ready",
  );
  await expect(action).toHaveText("Create link");
  expect(invitePayloads).toEqual([]);

  // Two same-frame DOM activations exercise the ref fence before React has
  // time to rerender/disable the button.
  await action.evaluate((button) => {
    (button as HTMLButtonElement).click();
    (button as HTMLButtonElement).click();
  });
  await expect(field).toHaveValue(INVITE_URL);
  await expect(field).toHaveAttribute(
    "data-invite-id",
    "123e4567-e89b-12d3-a456-000000000001",
  );
  await expect(action).toHaveText("Copy link");
  expect(invitePayloads).toEqual([{ max_uses: 1, ttl_secs: 3 * 24 * 60 * 60 }]);

  await action.click();
  await expect(action).toContainText("Copied");
  const copied = await page.evaluate(() => {
    const log = (
      window as Window & {
        __BUZZ_E2E_COMMAND_LOG__?: Array<{
          command: string;
          payload: Record<string, unknown> | null;
        }>;
      }
    ).__BUZZ_E2E_COMMAND_LOG__;
    return log?.findLast(({ command }) => command === "copy_text_to_clipboard")
      ?.payload;
  });
  expect(copied).toEqual({ text: INVITE_URL });
});

test("changing link options does not mint until Create", async ({ page }) => {
  await page.goto("/");
  await openInviteDialog(page);

  const maxUsesTrigger = page.getByTestId("invite-link-max-uses-trigger");
  await maxUsesTrigger.click();
  await page.getByTestId("invite-link-max-uses-10").click();
  await expect(maxUsesTrigger).toHaveText("10 uses");
  await page.getByTestId("invite-link-ttl-trigger").click();
  await page.getByTestId("invite-link-ttl-604800").click();
  expect(invitePayloads).toEqual([]);

  await page.getByTestId("copy-invite-link").click();
  await expect(page.getByTestId("invite-link-url")).toHaveValue(INVITE_URL);
  expect(invitePayloads).toEqual([
    { max_uses: 10, ttl_secs: 7 * 24 * 60 * 60 },
  ]);
});

test("a pending-list refresh failure cannot discard a minted bearer", async ({
  page,
}) => {
  await page.unroute("**/api/invites");
  await page.route("**/api/invites", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({ status: 500 });
      return;
    }
    await route.fulfill({ json: inviteResponse(), status: 200 });
  });

  await page.goto("/");
  await openInviteDialog(page);
  await page.getByTestId("copy-invite-link").click();
  await expect(page.getByTestId("invite-link-url")).toHaveValue(INVITE_URL);
  await expect(page.getByTestId("copy-invite-link")).toHaveText("Copy link");
});

test("a failed explicit Create can be retried", async ({ page }) => {
  let attempts = 0;
  await page.unroute("**/api/invites");
  await page.route("**/api/invites", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({ json: { invites: [] }, status: 200 });
      return;
    }
    attempts += 1;
    if (attempts === 1) {
      await route.fulfill({ status: 500 });
      return;
    }
    await route.fulfill({ json: inviteResponse(), status: 200 });
  });

  await page.goto("/");
  await openInviteDialog(page);
  const field = page.getByTestId("invite-link-url");
  const action = page.getByTestId("copy-invite-link");
  expect(attempts).toBe(0);
  await action.click();
  await expect(field).toHaveAttribute(
    "placeholder",
    "Couldn’t create invite link",
  );
  await expect(action).toHaveText("Retry");
  await action.click();
  await expect(field).toHaveValue(INVITE_URL);
  await expect(action).toHaveText("Copy link");
  expect(attempts).toBe(2);
});

test("close and out-of-order responses never mint on reopen or clobber the new link", async ({
  page,
}) => {
  let attempts = 0;
  let getCount = 0;
  let releaseFirst = () => {};
  let firstFinished = false;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  await page.unroute("**/api/invites");
  await page.route("**/api/invites", async (route) => {
    if (route.request().method() === "GET") {
      getCount += 1;
      await route.fulfill({ json: { invites: [] }, status: 200 });
      return;
    }
    attempts += 1;
    const sequence = attempts;
    if (sequence === 1) await firstGate;
    await route.fulfill({ json: inviteResponse(sequence), status: 200 });
    if (sequence === 1) firstFinished = true;
  });

  await page.goto("/");
  await openInviteDialog(page);
  const initialGetCount = getCount;
  await page.getByTestId("copy-invite-link").click();
  await expect.poll(() => attempts).toBe(1);
  await page
    .getByTestId("community-invite-dialog")
    .getByRole("button", { name: "Close" })
    .click();

  await page.getByTestId("community-invite-dialog-trigger").click();
  await expect(page.getByTestId("copy-invite-link")).toHaveText("Create link");
  expect(attempts).toBe(1);
  await page.getByTestId("copy-invite-link").click();
  await expect(page.getByTestId("invite-link-url")).toHaveValue(
    `${INVITE_URL}-2`,
  );
  await expect.poll(() => getCount).toBeGreaterThan(initialGetCount);

  releaseFirst();
  await expect.poll(() => firstFinished).toBe(true);
  await expect(page.getByTestId("invite-link-url")).toHaveValue(
    `${INVITE_URL}-2`,
  );
});
