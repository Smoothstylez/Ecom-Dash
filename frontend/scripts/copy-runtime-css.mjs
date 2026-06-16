import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const frontendDir = resolve(scriptDir, "..");
const distDir = resolve(frontendDir, "dist");
const sourceDir = resolve(frontendDir, "..", "ecommerce-dashboard", "app", "static", "css");
const targetDir = resolve(frontendDir, "dist", "static", "css");
const packagedFrontendDir = resolve(frontendDir, "..", "ecommerce-dashboard", "frontend_dist");

if (!existsSync(sourceDir)) {
  throw new Error(`Runtime CSS source directory not found: ${sourceDir}`);
}

mkdirSync(targetDir, { recursive: true });
cpSync(sourceDir, targetDir, { recursive: true, force: true });

rmSync(packagedFrontendDir, { recursive: true, force: true });
mkdirSync(packagedFrontendDir, { recursive: true });
cpSync(distDir, packagedFrontendDir, { recursive: true, force: true });
