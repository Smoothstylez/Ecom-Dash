import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";

const BASE_URL = "http://192.168.178.197:8012/orders";

test.describe("orders long-stay repro", () => {
  test("stays on orders and records degradation over time", async ({ page }) => {
    test.setTimeout(1_800_000);

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
      if (response.url().includes("/api/orders") || response.url().includes("/api/sync/changestamp")) {
        responses.push(`${response.status()} ${response.request().method()} ${response.url()}`);
      }
    });

    await page.goto(BASE_URL, { waitUntil: "networkidle" });

    const samples: Array<Record<string, unknown>> = [];

    let failureState: Record<string, unknown> | null = null;

    for (let minute = 0; minute < 120; minute += 1) {
      const sample = await page.evaluate(async (minuteIndex) => {
        const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));
        const start = performance.now();
        const container = document.querySelector(".main-content-wrapper") instanceof HTMLElement
          ? document.querySelector(".main-content-wrapper") as HTMLElement
          : (document.scrollingElement || document.documentElement) as HTMLElement;
        const frameGaps: number[] = [];
        let running = true;
        let last = performance.now();
        const raf = () => {
          if (!running) return;
          requestAnimationFrame((now) => {
            frameGaps.push(now - last);
            last = now;
            raf();
          });
        };
        raf();
        const maxScrollTop = Math.max(0, container.scrollHeight - container.clientHeight);
        const step = Math.max(180, Math.round(container.clientHeight * 0.55));
        for (let top = 0; top <= maxScrollTop; top += step) {
          container.scrollTo({ top: Math.min(top, maxScrollTop), behavior: "auto" });
          await sleep(40);
        }
        for (let top = maxScrollTop; top >= 0; top -= step) {
          container.scrollTo({ top: Math.max(top, 0), behavior: "auto" });
          await sleep(40);
        }
        await sleep(120);
        running = false;
        const filtered = frameGaps.filter((value) => Number.isFinite(value) && value > 0);
        const visibleInputs = Array.from(document.querySelectorAll(".purchase-input")).filter((node) => node instanceof HTMLElement && node.getBoundingClientRect().height > 0).length;
        return {
          minute: minuteIndex,
          path: window.location.pathname + window.location.search,
          durationMs: Number((performance.now() - start).toFixed(1)),
          avgFrameGapMs: filtered.length ? Number((filtered.reduce((sum, value) => sum + value, 0) / filtered.length).toFixed(2)) : null,
          maxFrameGapMs: filtered.length ? Number(Math.max(...filtered).toFixed(2)) : null,
          over50msFrames: filtered.filter((value) => value > 50).length,
          over100msFrames: filtered.filter((value) => value > 100).length,
          bodyTextLength: document.body.innerText.length,
          visibleInputs,
          title: document.title,
          bodySnippet: document.body.innerText.slice(0, 200),
          bodyHasOrdersText: document.body.innerText.includes("Kombinierte Orders"),
        };
      }, minute);

      samples.push(sample);

      const blankLike = sample.bodyTextLength === 0
        || !sample.bodyHasOrdersText
        || String(sample.title || "").trim() === "";

      if (blankLike) {
        failureState = {
          minute,
          reason: "orders-content-missing-or-blank",
          sample,
        };
        break;
      }

      try {
        await page.waitForTimeout(15000);
      } catch (error) {
        failureState = {
          minute,
          reason: "wait-failed",
          error: error instanceof Error ? error.message : String(error),
        };
        break;
      }
    }

    const finalState = await page.evaluate(() => ({
      url: window.location.href,
      title: document.title,
      bodyTextLength: document.body.innerText.length,
      bodySnippet: document.body.innerText.slice(0, 500),
    }));

    const result = {
      samples,
      failureState,
      consoleMessages,
      pageErrors,
      responses,
      finalState,
    };

    const outputPath = test.info().outputPath("orders-long-stay-repro.json");
    await fs.writeFile(outputPath, JSON.stringify(result, null, 2), "utf8");
    console.log(`ORDERS_LONG_STAY_RESULT=${JSON.stringify(result)}`);

    expect(result).toBeTruthy();
  });
});
