import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";

const BASE_URL = "http://192.168.178.197:8012/orders";

test.describe("orders detail loop repro", () => {
  test("repeatedly opens order details and previews", async ({ page }) => {
    test.setTimeout(300_000);

    const consoleMessages: string[] = [];
    const pageErrors: string[] = [];
    const responses: string[] = [];
    page.on("console", (message) => {
      if (message.type() === "warning" || message.type() === "error") {
        consoleMessages.push(`${message.type()}: ${message.text()}`);
      }
    });
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });
    page.on("response", (response) => {
      const url = response.url();
      if (url.includes("/api/orders/") || url.includes("/invoice/") || url.includes("disposition=inline")) {
        responses.push(`${response.status()} ${response.request().method()} ${url}`);
      }
    });

    await page.goto(BASE_URL, { waitUntil: "networkidle" });

    const samples: Array<Record<string, unknown>> = [];
    for (let index = 0; index < 12; index += 1) {
      const rows = page.locator("tr[data-react-orders-row='true']");
      const row = rows.nth(index % 8);
      await row.locator("td").first().click();
      await page.waitForSelector("#detailsModal.active", { timeout: 10000 });
      await page.waitForTimeout(600);

      const previewButton = page.locator("#ordersDetailsContent [data-action='preview-document']").first();
      const previewVisible = await previewButton.isVisible().catch(() => false);
      if (previewVisible) {
        await previewButton.click();
        await page.waitForTimeout(1000);
        const closePreviewButton = page.locator("#closePreviewBtn");
        if (await closePreviewButton.isVisible().catch(() => false)) {
          await closePreviewButton.click();
          await page.waitForTimeout(300);
        }
      }

      const detailState = await page.evaluate((loopIndex) => {
        const modal = document.getElementById("detailsModal");
        const content = document.getElementById("ordersDetailsContent");
        const previewFrameCount = document.querySelectorAll("#previewBody iframe").length;
        const previewImageCount = document.querySelectorAll("#previewBody img").length;
        const detailImageCount = document.querySelectorAll("#ordersDetailsContent img").length;
        return {
          loop: loopIndex,
          modalActive: modal?.className || "",
          detailTextLength: content?.innerText.length || 0,
          previewFrameCount,
          previewImageCount,
          detailImageCount,
          bodyTextLength: document.body.innerText.length,
        };
      }, index);
      samples.push(detailState);

      await page.locator("#closeModalBtn").click();
      await page.waitForTimeout(400);
    }

    const finalState = await page.evaluate(() => ({
      url: window.location.href,
      title: document.title,
      bodyTextLength: document.body.innerText.length,
      bodySnippet: document.body.innerText.slice(0, 400),
      previewFrameCount: document.querySelectorAll("#previewBody iframe").length,
      previewImageCount: document.querySelectorAll("#previewBody img").length,
      detailsOpen: Boolean(document.querySelector("#detailsModal.active")),
    }));

    const result = { samples, consoleMessages, pageErrors, responses, finalState };
    const outputPath = test.info().outputPath("orders-detail-loop-repro.json");
    await fs.writeFile(outputPath, JSON.stringify(result, null, 2), "utf8");
    console.log(`ORDERS_DETAIL_LOOP_RESULT=${JSON.stringify(result)}`);

    expect(result).toBeTruthy();
  });
});
