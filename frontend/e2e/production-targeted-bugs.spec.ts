import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";

const BASE_URL = "http://192.168.178.197:8012";

async function scrollContainer(page: import("@playwright/test").Page, rounds: number) {
  return page.evaluate(async (count) => {
    const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));
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
    const step = Math.max(200, Math.round(container.clientHeight * 0.65));
    for (let round = 0; round < count; round += 1) {
      for (let top = 0; top <= maxScrollTop; top += step) {
        container.scrollTo({ top: Math.min(top, maxScrollTop), behavior: "auto" });
        await sleep(35);
      }
      for (let top = maxScrollTop; top >= 0; top -= step) {
        container.scrollTo({ top: Math.max(top, 0), behavior: "auto" });
        await sleep(35);
      }
    }
    await sleep(150);
    running = false;
    const filtered = frameGaps.filter((value) => Number.isFinite(value) && value > 0);
    return {
      path: window.location.pathname + window.location.search,
      avgFrameGapMs: filtered.length ? Number((filtered.reduce((sum, value) => sum + value, 0) / filtered.length).toFixed(2)) : null,
      maxFrameGapMs: filtered.length ? Number(Math.max(...filtered).toFixed(2)) : null,
      over50msFrames: filtered.filter((value) => value > 50).length,
      over100msFrames: filtered.filter((value) => value > 100).length,
      scrollHeight: container.scrollHeight,
      clientHeight: container.clientHeight,
      bodyTextLength: document.body.innerText.length,
    };
  }, rounds);
}

test.describe("production targeted bugs", () => {
  test("captures live bug candidates on production", async ({ page }) => {
    test.setTimeout(180_000);

    const pageErrors: string[] = [];
    const consoleMessages: string[] = [];
    const requests: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error" || message.type() === "warning") {
        consoleMessages.push(`${message.type()}: ${message.text()}`);
      }
    });
    page.on("response", (response) => {
      if (response.url().includes("/api/")) {
        requests.push(`${response.status()} ${response.request().method()} ${response.url()}`);
      }
    });

    await page.goto(BASE_URL, { waitUntil: "networkidle" });

    await page.getByRole("tab", { name: "Buchungen", exact: true }).click();
    await page.waitForTimeout(500);
    const bookingsPaths: Array<{ clicked: string; path: string }> = [];
    for (const name of ["Transaktionen", "Bestellungen", "Templates", "Konten", "Belege"]) {
      await page.getByRole("button", { name, exact: true }).click();
      await page.waitForTimeout(250);
      bookingsPaths.push({ clicked: name, path: new URL(page.url()).pathname + new URL(page.url()).search });
    }

    await page.getByRole("tab", { name: "Google Ads", exact: true }).click();
    await page.waitForTimeout(600);
    const googleAdsMetric = await scrollContainer(page, 10);

    await page.getByRole("tab", { name: "Kunden", exact: true }).click();
    await page.waitForTimeout(800);
    const customerButtons = page.locator("button");
    const buttonTexts = (await customerButtons.allTextContents()).map((value) => value.replace(/\s+/g, " ").trim()).filter(Boolean);
    const globeToggle = buttonTexts.find((value) => /globe|globus/i.test(value));
    let customerGlobeState: Record<string, unknown> = { available: false };
    if (globeToggle) {
      await page.getByRole("button", { name: globeToggle, exact: true }).click();
      await page.waitForTimeout(1500);
      customerGlobeState = await page.evaluate(() => {
        const globeView = document.getElementById("customerGeoGlobeView");
        return {
          available: true,
          path: window.location.pathname + window.location.search,
          text: globeView?.textContent?.replace(/\s+/g, " ").trim() || "",
          canvasCount: globeView?.querySelectorAll("canvas").length || 0,
          iframeCount: globeView?.querySelectorAll("iframe").length || 0,
          bodyTextLength: document.body.innerText.length,
        };
      });
    }

    await page.getByRole("tab", { name: "Orders", exact: true }).click();
    await page.waitForTimeout(700);
    const ordersInputLag = await page.evaluate(async () => {
      const originalFetch = window.fetch.bind(window);
      const blocked: Array<{ url: string; method: string }> = [];
      window.fetch = async (input, init) => {
        const request = input instanceof Request ? input : null;
        const url = typeof input === "string" ? input : request ? request.url : String(input);
        const method = String((init && init.method) || (request && request.method) || "GET").toUpperCase();
        if (method === "PATCH" && /\/api\/orders\/[^/]+\/[^/]+\/purchase(?:\?|$)/.test(url)) {
          blocked.push({ url, method });
          return new Response(JSON.stringify({ blocked: true }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        return originalFetch(input, init);
      };

      const input = document.querySelector(".purchase-input") as HTMLInputElement | null;
      if (!input) {
        return { available: false, blockedWrites: blocked };
      }
      const logs: Array<{ type: string; t: number; key?: string }> = [];
      input.addEventListener("keydown", (event) => {
        logs.push({ type: "keydown", t: performance.now(), key: event.key });
      }, { capture: true });
      input.addEventListener("input", () => {
        logs.push({ type: "input", t: performance.now() });
      }, { capture: true });

      input.focus();
      input.select();
      const value = "12.34";
      for (const character of value) {
        document.execCommand("insertText", false, character);
        await new Promise((resolve) => window.setTimeout(resolve, 120));
      }
      input.blur();
      await new Promise((resolve) => window.setTimeout(resolve, 50));
      const keydowns = logs.filter((entry) => entry.type === "keydown" && entry.key && entry.key.length === 1);
      const inputs = logs.filter((entry) => entry.type === "input");
      const pairs = Math.min(keydowns.length, inputs.length);
      const lags: number[] = [];
      for (let index = 0; index < pairs; index += 1) {
        lags.push(Number((inputs[index].t - keydowns[index].t).toFixed(2)));
      }
      return {
        available: true,
        avgLagMs: lags.length ? Number((lags.reduce((sum, item) => sum + item, 0) / lags.length).toFixed(2)) : null,
        maxLagMs: lags.length ? Math.max(...lags) : null,
        blockedWrites: blocked,
      };
    });

    const finalState = await page.evaluate(() => ({
      url: window.location.href,
      title: document.title,
      bodySnippet: document.body.innerText.slice(0, 400),
    }));

    const result = {
      bookingsPaths,
      googleAdsMetric,
      customerGlobeState,
      ordersInputLag,
      pageErrors,
      consoleMessages,
      requests,
      finalState,
    };

    const outputPath = test.info().outputPath("production-targeted-bugs.json");
    await fs.writeFile(outputPath, JSON.stringify(result, null, 2), "utf8");
    console.log(`PRODUCTION_TARGETED_RESULT=${JSON.stringify(result)}`);

    expect(result).toBeTruthy();
  });
});
