import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";

const BASE_URL = "http://192.168.178.197:8012/orders";

test.describe("orders polling repro", () => {
  test("forces polling on and observes orders over time", async ({ page }) => {
    test.setTimeout(600_000);

    await page.addInitScript(() => {
      window.localStorage.setItem("dash-combined.polling", JSON.stringify({ enabled: true, intervalSec: 5 }));
    });

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
      if (url.includes("/api/orders") || url.includes("/api/sync/changestamp")) {
        responses.push(`${response.status()} ${response.request().method()} ${url}`);
      }
    });

    await page.goto(BASE_URL, { waitUntil: "networkidle" });

    const samples: Array<Record<string, unknown>> = [];
    for (let tick = 0; tick < 24; tick += 1) {
      const sample = await page.evaluate((tickIndex) => {
        return {
          tick: tickIndex,
          path: window.location.pathname + window.location.search,
          title: document.title,
          bodyTextLength: document.body.innerText.length,
          bodyHasOrdersText: document.body.innerText.includes("Kombinierte Orders"),
          visibleInputs: Array.from(document.querySelectorAll(".purchase-input")).filter((node) => node instanceof HTMLElement && node.getBoundingClientRect().height > 0).length,
          loadingTextVisible: document.body.innerText.includes("Orders werden geladen"),
          statusText: document.getElementById("statusBox")?.textContent?.trim() || "",
        };
      }, tick);
      samples.push(sample);

      const blankLike = sample.bodyTextLength === 0 || !sample.bodyHasOrdersText || String(sample.title || "").trim() === "";
      if (blankLike) {
        break;
      }
      await page.waitForTimeout(15000);
    }

    const finalState = await page.evaluate(() => ({
      url: window.location.href,
      title: document.title,
      bodyTextLength: document.body.innerText.length,
      bodySnippet: document.body.innerText.slice(0, 400),
    }));

    const result = { samples, consoleMessages, pageErrors, responses, finalState };
    const outputPath = test.info().outputPath("orders-polling-repro.json");
    await fs.writeFile(outputPath, JSON.stringify(result, null, 2), "utf8");
    console.log(`ORDERS_POLLING_RESULT=${JSON.stringify(result)}`);

    expect(result).toBeTruthy();
  });
});
