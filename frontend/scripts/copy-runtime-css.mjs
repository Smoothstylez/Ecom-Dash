import { cpSync, existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const frontendDir = resolve(scriptDir, "..");
const sourceDir = resolve(frontendDir, "..", "ecommerce-dashboard", "app", "static", "css");
const targetDir = resolve(frontendDir, "dist", "static", "css");

if (!existsSync(sourceDir)) {
  throw new Error(`Runtime CSS source directory not found: ${sourceDir}`);
}

mkdirSync(targetDir, { recursive: true });
cpSync(sourceDir, targetDir, { recursive: true, force: true });
