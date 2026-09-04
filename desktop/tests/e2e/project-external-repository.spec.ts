import { expect, test } from "@playwright/test";

import { installMockBridge, TEST_IDENTITIES } from "../helpers/bridge";

async function enableProjectsFeature(page: import("@playwright/test").Page) {
  await page.addInitScript(() => {
    window.localStorage.setItem(
      "buzz-feature-overrides-v1",
      JSON.stringify({ projects: true }),
    );
  });
}

async function seedExternalNemoProject(page: import("@playwright/test").Page) {
  const owner = TEST_IDENTITIES.alice.pubkey;
  const repositoryAddress = `30617:${owner}:nemo`;
  await page.addInitScript(
    ({ address, repositoryOwner }) => {
      const now = Math.floor(Date.now() / 1_000);
      window.__BUZZ_E2E_EXTRA_PROJECT_EVENTS__ = [
        {
          id: "ca".repeat(32),
          kind: 30617,
          pubkey: repositoryOwner,
          created_at: now,
          content: "External GitHub retry fixture.",
          tags: [
            ["d", "nemo"],
            ["name", "Nemo"],
            ["clone", "https://github.com/mysteropodes/nemo.git"],
            ["web", "https://github.com/mysteropodes/nemo"],
            ["default-branch", "main"],
          ],
        },
        {
          id: "cb".repeat(32),
          kind: 30621,
          pubkey: repositoryOwner,
          created_at: now,
          content: "",
          tags: [
            ["d", "nemo"],
            ["name", "Nemo"],
            ["a", address],
            ["buzz-channel", "cf63feec-21bb-5bf0-a2f8-0e4c3de8ec73"],
          ],
        },
      ];
    },
    { address: repositoryAddress, repositoryOwner: owner },
  );
}

test("a remote-only GitHub repository exposes a working retry", async ({
  page,
}) => {
  await enableProjectsFeature(page);
  await seedExternalNemoProject(page);
  await installMockBridge(page, {
    projectRepoSnapshotError: "network: GitHub snapshot unavailable",
  });
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await page.getByTestId("open-projects-view").click();
  await page.getByTestId("projects-section-projects").click();
  await page
    .locator(
      '[data-testid="project-card-nemo"], [data-testid="project-row-nemo"]',
    )
    .first()
    .click();
  await page.getByTestId("project-home-context-repo-nemo").click();

  const unavailable = page.getByTestId("project-repository-unavailable");
  await expect(unavailable).toBeVisible();
  await expect(page.getByText("Code hosted on github.com")).toHaveCount(0);
  const remoteSnapshotsBeforeRetry = await page.evaluate(
    () =>
      (window.__BUZZ_E2E_COMMAND_PAYLOADS__ ?? []).filter(
        ({ command }) => command === "get_project_repo_snapshot",
      ).length,
  );
  await page.evaluate(() => {
    const testWindow = window as typeof window & {
      __BUZZ_E2E__?: { mock?: { projectRepoSnapshotError?: string } };
    };
    if (testWindow.__BUZZ_E2E__?.mock) {
      delete testWindow.__BUZZ_E2E__.mock.projectRepoSnapshotError;
    }
  });
  await unavailable.getByRole("button", { name: "Retry" }).click();

  await expect(page.getByText("Remote state refreshed.")).toBeVisible();
  await expect(unavailable).toHaveCount(0);
  await expect
    .poll(() =>
      page.evaluate(
        () =>
          (window.__BUZZ_E2E_COMMAND_PAYLOADS__ ?? []).filter(
            ({ command }) => command === "get_project_repo_snapshot",
          ).length,
      ),
    )
    .toBeGreaterThan(remoteSnapshotsBeforeRetry);
  await page.getByRole("tab", { name: "Files" }).click();
  await expect(page.getByText("desktop", { exact: true })).toBeVisible();
  await page.getByTestId("project-repository-branch-trigger").click();
  await expect(page.getByTestId("project-create-branch")).toHaveCount(0);
  await expect(page.getByTestId("project-delete-branch")).toHaveCount(0);
});
