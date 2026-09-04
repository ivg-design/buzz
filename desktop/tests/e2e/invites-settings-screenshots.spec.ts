import { expect, test } from "@playwright/test";

import { waitForAnimations } from "../helpers/animations";
import { TEST_IDENTITIES, installMockBridge } from "../helpers/bridge";
import { openSettings } from "../helpers/settings";

const OUTDIR = "test-results/invites-settings";
const DIRECT_ADD_HEX =
  "ea9b4d7a7a78a3e3729e5568b14d764d4962be0e1f20f749bcf8d9dbbf9a9328";
const DIRECT_ADD_NPUB =
  "npub1a2d567n60z37xu57245tzntkf4yk90swrus0wjdulrvah0u6jv5qusyp60";
const SECOND_DIRECT_ADD_HEX =
  "554cef57437abac34522ac2c9f0490d685b72c80478cf9f7ed6f9570ee8624ea";
const SECOND_DIRECT_ADD_NPUB =
  "npub124xw746r02avx3fz4skf7pys66zmwtyqg7x0naldd72hpm5xyn4qtjl6z8";
const INVITE_CODE = `v2.${"A".repeat(43)}`;
const INVITE_URL = `https://alpha.example.com/invite#code=${INVITE_CODE}`;
let pendingInviteVisible = true;

test.beforeEach(async ({ page }, testInfo) => {
  pendingInviteVisible = true;
  await installMockBridge(page, {
    relayRequiresMembership: true,
    relayRole: testInfo.title.includes("admin can add members")
      ? "admin"
      : "owner",
  });
  await page.route("**/api/invites", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({
        contentType: "application/json",
        json: {
          invites: pendingInviteVisible
            ? [
                {
                  id: "123e4567-e89b-12d3-a456-426614174000",
                  expires_at: Math.floor(Date.now() / 1000) + 3 * 86_400,
                  max_uses: 1,
                  use_count: 0,
                  uses_remaining: 1,
                  created_by: TEST_IDENTITIES.tyler.pubkey,
                  created_at: Math.floor(Date.now() / 1000),
                },
              ]
            : [],
        },
        status: 200,
      });
      return;
    }
    await route.fulfill({
      contentType: "application/json",
      json: {
        id: "123e4567-e89b-12d3-a456-426614174001",
        code: INVITE_CODE,
        expires_at: Math.floor(Date.now() / 1000) + 3 * 86_400,
        max_uses: 1,
        uses_remaining: 1,
        url: INVITE_URL,
      },
      status: 200,
    });
  });
  await page.goto("/");
  await openSettings(page, "community-members");
});

test("opens a profile from a community member avatar", async ({ page }) => {
  await page.getByRole("button", { name: "Open profile for alice" }).click();

  await expect(page).toHaveURL(
    new RegExp(`/pulse\\?profile=${TEST_IDENTITIES.alice.pubkey}$`),
  );
  await expect(page.getByTestId("user-profile-panel")).toBeVisible();
});

test("capture: consolidated invites settings", async ({ page }) => {
  const panel = page.getByTestId("settings-panel-community-members");

  await expect(
    page.getByTestId("settings-nav-community-members"),
  ).toContainText("Invites");
  await expect(
    page.getByRole("heading", { name: "Invites", exact: true }),
  ).toBeVisible();
  await expect(page.getByTestId("community-icon-settings")).toHaveCount(0);
  await expect(
    page.getByTestId("community-invite-dialog-trigger"),
  ).toBeVisible();
  await expect(page.getByTestId("community-invite-email-field")).toHaveCount(0);
  await expect(page.getByTestId("copy-invite-link")).toHaveCount(0);
  await expect(page.getByText("alice", { exact: true })).toBeVisible();
  await expect(page.getByText("bob", { exact: true })).toBeVisible();
  await expect(
    page.getByText("Manage roles or remove access.", { exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByText("People who use the link join as members."),
  ).toHaveCount(0);
  await expect(page.getByTestId("community-icon-save")).toHaveCount(0);
  await expect(page.getByText("Pending invite links")).toBeVisible();
  await expect(page.getByText("1 use left")).toBeVisible();

  const aliceName = page.getByText("alice", { exact: true });
  const aliceRow = page
    .locator('[data-testid^="relay-member-row-"]')
    .filter({ has: aliceName });
  const aliceNpub = aliceRow.locator('[data-testid^="relay-member-npub-"]');
  await expect(aliceName).toHaveCSS("opacity", "1");
  await expect(aliceNpub).toHaveCSS("opacity", "0");
  await aliceRow.hover();
  await expect(aliceName).toHaveCSS("opacity", "0");
  await expect(aliceNpub).toHaveCSS("opacity", "1");
  await page.mouse.move(0, 0);

  await waitForAnimations(page);
  await panel.screenshot({ path: `${OUTDIR}/01-invites-settings.png` });
});

test("capture: share-style community invite dialog", async ({ page }) => {
  await page.getByTestId("community-invite-dialog-trigger").click();

  const dialog = page.getByTestId("community-invite-dialog");
  await expect(dialog).toBeVisible();
  await expect(
    dialog.getByRole("heading", { name: "Invite to community" }),
  ).toBeVisible();
  await expect(page.getByTestId("community-invite-email-field")).toHaveCount(0);
  await expect(page.getByPlaceholder("Type an email address")).toHaveCount(0);
  await expect(
    dialog.getByText(
      "Add someone directly or share a link they can use to join.",
    ),
  ).toBeVisible();
  await expect(
    dialog.getByRole("heading", { name: "Add someone", exact: true }),
  ).toHaveCount(0);
  await expect(dialog.getByTestId("invite-options-divider")).toBeVisible();
  await expect(
    dialog.getByText("Or, copy a link", { exact: true }),
  ).toBeVisible();
  await expect(dialog.getByText("Link settings", { exact: true })).toHaveCount(
    0,
  );
  await expect(page.getByTestId("member-pubkey-input")).toBeVisible();
  await expect(page.getByTestId("member-role")).toHaveCount(0);
  await expect(page.getByTestId("confirm-add-member")).toHaveCount(0);
  await expect(page.getByTestId("invite-link-url")).toHaveValue("");
  await expect(page.getByTestId("copy-invite-link")).toHaveText("Create link");
  await expect(page.getByTestId("invite-link-ttl-trigger")).toHaveText(
    "3 days",
  );
  await page.getByTestId("copy-invite-link").click();
  await expect(page.getByTestId("invite-link-url")).toHaveValue(INVITE_URL);
  await expect(page.getByTestId("copy-invite-link")).toHaveText("Copy link");

  await page.getByTestId("member-pubkey-input").fill(DIRECT_ADD_NPUB);
  await expect(page.getByTestId("member-search-popover")).toBeVisible();
  await page.getByTestId(`member-search-result-${DIRECT_ADD_HEX}`).click();
  const memberRole = page.getByTestId("member-role");
  const selectedChip = page.getByTestId(
    `member-search-selection-remove-${DIRECT_ADD_HEX}`,
  );
  await expect(memberRole).toHaveText("Member");
  const inviteButton = page.getByTestId("confirm-add-member");
  await expect(inviteButton).toHaveText("Invite");
  await waitForAnimations(page);
  await expect(inviteButton).toHaveCSS("height", "44px");
  await expect(inviteButton).toHaveJSProperty(
    "offsetHeight",
    await page
      .getByTestId("member-recipient-field")
      .evaluate((field) => Math.round(field.getBoundingClientRect().height)),
  );
  const selectedChipRemoveIcon = selectedChip.locator("span.absolute");
  await expect(selectedChipRemoveIcon).toHaveCSS("opacity", "0");
  await selectedChip.hover();
  await expect(selectedChipRemoveIcon).toHaveCSS("opacity", "1");
  const memberSearch = page.getByTestId("member-pubkey-input");
  await expect(memberSearch).toBeFocused();
  await memberSearch.fill(SECOND_DIRECT_ADD_NPUB);
  await expect(page.getByTestId("member-search-popover")).toBeVisible();
  await page
    .getByTestId(`member-search-result-${SECOND_DIRECT_ADD_HEX}`)
    .click();
  await expect(selectedChip).toHaveCount(0);
  await expect(
    page.getByTestId(`member-search-selection-remove-${SECOND_DIRECT_ADD_HEX}`),
  ).toBeVisible();
  await expect(memberSearch).toBeFocused();
  await memberRole.click();
  await expect(
    page.getByRole("menuitemradio", { name: "Admin" }),
  ).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByTestId("confirm-add-member")).toBeEnabled();
  await waitForAnimations(page);
  await page.mouse.move(0, 0);
  await dialog.screenshot({ path: `${OUTDIR}/02-invite-dialog.png` });
});

test("admin can add members but cannot assign the admin role", async ({
  page,
}) => {
  await page.getByTestId("community-invite-dialog-trigger").click();

  await page.getByTestId("member-pubkey-input").fill(DIRECT_ADD_NPUB);
  await page.getByTestId(`member-search-result-${DIRECT_ADD_HEX}`).click();
  const memberRole = page.getByTestId("member-role");
  await expect(memberRole).toHaveText("Member");
  await memberRole.click();
  await expect(page.getByRole("menuitemradio", { name: "Admin" })).toHaveCount(
    0,
  );
  await page.keyboard.press("Escape");
});

test("owner direct-add replaces the prior recipient and grants one admin", async ({
  page,
}) => {
  await page.getByTestId("community-invite-dialog-trigger").click();
  await page.getByTestId("member-pubkey-input").fill(DIRECT_ADD_NPUB);
  await page.getByTestId(`member-search-result-${DIRECT_ADD_HEX}`).click();
  await page.getByTestId("member-pubkey-input").fill(SECOND_DIRECT_ADD_NPUB);
  await page
    .getByTestId(`member-search-result-${SECOND_DIRECT_ADD_HEX}`)
    .click();
  await page.getByTestId("member-role").click();
  await page.getByRole("menuitemradio", { name: "Admin" }).click();
  await page.getByTestId("confirm-add-member").click();
  const confirmation = page.getByTestId("grant-admin-confirmation");
  await expect(confirmation).toBeVisible();
  await confirmation
    .getByRole("button", { name: "Grant admin access" })
    .click();

  await expect
    .poll(async () =>
      page.evaluate(
        ({ targetPubkey, role }) =>
          (window.__BUZZ_E2E_COMMAND_PAYLOADS__ ?? []).some((entry) => {
            if (entry.command !== "plugin:websocket|send") return false;
            const wireMessage = (
              entry.payload as {
                message?: { data?: unknown };
              }
            )?.message?.data;
            if (typeof wireMessage !== "string") return false;
            const message = JSON.parse(wireMessage) as unknown[];
            if (message[0] !== "EVENT") return false;
            const event = message[1] as
              | { kind?: number; tags?: string[][] }
              | undefined;
            return (
              event?.kind === 9030 &&
              event.tags?.some(
                (tag) => tag[0] === "p" && tag[1] === targetPubkey,
              ) &&
              event.tags.some((tag) => tag[0] === "role" && tag[1] === role)
            );
          }),
        {
          targetPubkey: SECOND_DIRECT_ADD_HEX,
          role: "admin",
        },
      ),
    )
    .toBe(true);

  const firstRecipientWasPublished = await page.evaluate(
    (targetPubkey) =>
      (window.__BUZZ_E2E_COMMAND_PAYLOADS__ ?? []).some((entry) => {
        if (entry.command !== "plugin:websocket|send") return false;
        const wireMessage = (entry.payload as { message?: { data?: unknown } })
          ?.message?.data;
        if (typeof wireMessage !== "string") return false;
        const message = JSON.parse(wireMessage) as unknown[];
        const event = message[1] as
          | { kind?: number; tags?: string[][] }
          | undefined;
        return (
          message[0] === "EVENT" &&
          event?.kind === 9030 &&
          event.tags?.some((tag) => tag[0] === "p" && tag[1] === targetPubkey)
        );
      }),
    DIRECT_ADD_HEX,
  );
  expect(firstRecipientWasPublished).toBe(false);
});

test("malformed npub cannot be selected or admitted", async ({ page }) => {
  await page.getByTestId("community-invite-dialog-trigger").click();
  await page.getByTestId("member-pubkey-input").fill("npub1not-a-valid-key");
  await expect(page.getByTestId("confirm-add-member")).toHaveCount(0);
  await expect(page.getByText(/No people found/)).toBeVisible();

  await page.getByTestId("member-pubkey-input").fill("0".repeat(64));
  await expect(page.getByTestId("confirm-add-member")).toHaveCount(0);
  await expect(page.getByText(/No people found/)).toBeVisible();
});

test("owner explicitly confirms promotion before an admin event is signed", async ({
  page,
}) => {
  const bobRow = page
    .locator('[data-testid^="relay-member-row-"]')
    .filter({ has: page.getByText("bob", { exact: true }) });
  await bobRow.getByRole("button", { name: /Actions for/ }).click();
  await page.getByRole("menuitem", { name: "Make admin" }).click();
  await expect(page.getByTestId("grant-admin-confirmation")).toBeVisible();

  const before = await page.evaluate(
    () =>
      (window.__BUZZ_E2E_COMMAND_PAYLOADS__ ?? []).filter((entry) => {
        const wire = (entry.payload as { message?: { data?: unknown } })
          ?.message?.data;
        return (
          entry.command === "plugin:websocket|send" &&
          typeof wire === "string" &&
          (JSON.parse(wire) as unknown[])[0] === "EVENT" &&
          ((JSON.parse(wire) as unknown[])[1] as { kind?: number })?.kind ===
            9032
        );
      }).length,
  );
  expect(before).toBe(0);

  await page
    .getByTestId("grant-admin-confirmation")
    .getByRole("button", { name: "Grant admin access" })
    .click();
  await expect
    .poll(() =>
      page.evaluate(() =>
        (window.__BUZZ_E2E_COMMAND_PAYLOADS__ ?? []).some((entry) => {
          const wire = (entry.payload as { message?: { data?: unknown } })
            ?.message?.data;
          return (
            entry.command === "plugin:websocket|send" &&
            typeof wire === "string" &&
            ((JSON.parse(wire) as unknown[])[1] as { kind?: number })?.kind ===
              9032
          );
        }),
      ),
    )
    .toBe(true);
});

test("revokes a pending invite after explicit confirmation", async ({
  page,
}) => {
  let deletePath: string | null = null;
  await page.route("**/api/invites/*", async (route) => {
    deletePath = new URL(route.request().url()).pathname;
    pendingInviteVisible = false;
    await route.fulfill({
      contentType: "application/json",
      json: { status: "revoked" },
      status: 200,
    });
  });

  await page
    .getByTestId("revoke-invite-123e4567-e89b-12d3-a456-426614174000")
    .click();
  const confirmation = page.getByTestId("revoke-invite-confirmation");
  await expect(confirmation).toBeVisible();
  await confirmation.getByRole("button", { name: "Revoke invite" }).click();
  await expect(
    page.getByTestId("pending-invite-123e4567-e89b-12d3-a456-426614174000"),
  ).toHaveCount(0);
  expect(deletePath).toBe("/api/invites/123e4567-e89b-12d3-a456-426614174000");
});
