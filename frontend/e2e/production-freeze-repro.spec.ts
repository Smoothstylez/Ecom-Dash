import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";

const BASE_URL = "http://192.168.178.197:8012";

type ScrollMetric = {
  path: string;
  scrollHeight: number;
  clientHeight: number;
  avgFrameGapMs: number | null;
  maxFrameGapMs: number | null;
  over50msFrames: number;
  over100msFrames: number;
  longTasks: Array<{ startTime: number; duration: number }>;
  bodyTextLength: number;
};

async function measureScroll(page: import("@playwright/test").Page): Promise<ScrollMetric> {
  return page.evaluate(async () => {
    const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));
    const container = document.querySelector(".main-content-wrapper") instanceof HTMLElement
      ? document.querySelector(".main-content-wrapper") as HTMLElement
      : (document.scrollingElement || document.documentElement) as HTMLElement;
    const frameGaps: number[] = [];
    const longTasks: Array<{ startTime: number; duration: number }> = [];
    let observer: PerformanceObserver | null = null;
    if ("PerformanceObserver" in window) {
      try {
        observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            longTasks.push({
              startTime: Number(entry.startTime.toFixed(1)),
              duration: Number(entry.duration.toFixed(1)),
            });
          }
        });
        observer.observe({ entryTypes: ["longtask"] });
      } catch {
        observer = null;
      }
    }

    let running = true;
    let last = performance.now();
    const raf = () => {
      if (!running) {
        return;
      }
      requestAnimationFrame((now) => {
        frameGaps.push(now - last);
        last = now;
        raf();
      });
    };
    raf();

    const maxScrollTop = Math.max(0, container.scrollHeight - container.clientHeight);
    const step = Math.max(200, Math.round(container.clientHeight * 0.65));
    for (let target = 0; target <= maxScrollTop; target += step) {
      container.scrollTo({ top: Math.min(target, maxScrollTop), behavior: "auto" });
      await sleep(50);
    }
    for (let target = maxScrollTop; target >= 0; target -= step) {
      container.scrollTo({ top: Math.max(target, 0), behavior: "auto" });
      await sleep(50);
    }
    await sleep(150);
    running = false;
    observer?.disconnect();

    const filtered = frameGaps.filter((value) => Number.isFinite(value) && value > 0);
    return {
      path: window.location.pathname + window.location.search,
      scrollHeight: container.scrollHeight,
      clientHeight: container.clientHeight,
      avgFrameGapMs: filtered.length ? Number((filtered.reduce((sum, value) => sum + value, 0) / filtered.length).toFixed(2)) : null,
      maxFrameGapMs: filtered.length ? Number(Math.max(...filtered).toFixed(2)) : null,
      over50msFrames: filtered.filter((value) => value > 50).length,
      over100msFrames: filtered.filter((value) => value > 100).length,
      longTasks,
      bodyTextLength: document.body.innerText.length,
    };
  });
}

test.describe("production freeze repro", () => {
  test("stress navigation and detect freeze candidate", async ({ page }) => {
    test.setTimeout(180_000);

    const consoleErrors: string[] = [];
    const pageErrors: string[] = [];
    const failedRequests: string[] = [];
    const routeMetrics: Array<{ label: string; loadMs: number; path: string }> = [];
    const scrollMetrics: Array<{ label: string; metric: ScrollMetric }> = [];

    page.on("console", (message) => {
      if (message.type() === "error" || message.type() === "warning") {
        consoleErrors.push(`${message.type()}: ${message.text()}`);
      }
    });
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });
    page.on("requestfailed", (request) => {
      failedRequests.push(`${request.method()} ${request.url()} :: ${request.failure()?.errorText || "failed"}`);
    });

    await page.goto(BASE_URL, { waitUntil: "networkidle" });

    const topTabs = [
      { name: "Analytics", expectedPath: "/analytics" },
      { name: "Orders", expectedPath: "/orders" },
      { name: "Kunden", expectedPath: "/customers" },
      { name: "Buchungen", expectedPath: "/bookings/full" },
      { name: "Google Ads", expectedPath: "/google-ads" },
      { name: "eBay", expectedPath: "/ebay" },
    ];

    for (let cycle = 0; cycle < 4; cycle += 1) {
      for (const tab of topTabs) {
        const startedAt = Date.now();
        await page.getByRole("tab", { name: tab.name, exact: true }).click();
        await page.waitForTimeout(500);
        const currentPath = new URL(page.url()).pathname;
        routeMetrics.push({
          label: `cycle-${cycle}-${tab.name.toLowerCase()}`,
          loadMs: Date.now() - startedAt,
          path: new URL(page.url()).pathname + new URL(page.url()).search,
        });

        if (tab.name === "Buchungen") {
          const subtabs = ["Transaktionen", "Bestellungen", "Templates", "Konten", "Belege"];
          for (const subtab of subtabs) {
            await page.getByRole("button", { name: subtab, exact: true }).click();
            await page.waitForTimeout(300);
            scrollMetrics.push({
              label: `cycle-${cycle}-bookings-${subtab.toLowerCase()}`,
              metric: await measureScroll(page),
            });
          }
          continue;
        }

        if (currentPath.startsWith(tab.expectedPath)) {
          scrollMetrics.push({
            label: `cycle-${cycle}-${tab.name.toLowerCase()}`,
            metric: await measureScroll(page),
          });
        }
      }
    }

    const finalState = await page.evaluate(() => ({
      url: window.location.href,
      title: document.title,
      bodyTextLength: document.body.innerText.length,
      bodySnippet: document.body.innerText.slice(0, 300),
    }));

    const result = {
      routeMetrics,
      scrollMetrics,
      consoleErrors,
      pageErrors,
      failedRequests,
      finalState,
    };

    const outputPath = test.info().outputPath("production-freeze-repro.json");
    await fs.writeFile(outputPath, JSON.stringify(result, null, 2), "utf8");
    console.log(`PRODUCTION_REPRO_RESULT=${JSON.stringify(result)}`);

    expect(result).toBeTruthy();
  });
});
