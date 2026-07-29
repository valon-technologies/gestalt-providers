import { spawnSync } from "node:child_process";
import { readdirSync, statSync } from "node:fs";
import { join } from "node:path";

function findTestFiles(dir) {
  const files = [];
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      files.push(...findTestFiles(path));
      continue;
    }
    if (name.endsWith(".test.ts")) {
      files.push(path);
    }
  }
  return files;
}

const tests = findTestFiles("src/lib");
if (tests.length === 0) {
  console.error("No lib unit tests found under src/lib");
  process.exit(1);
}

const result = spawnSync(
  process.execPath,
  ["--import", "tsx", "--test", ...tests],
  { stdio: "inherit" },
);

process.exit(result.status ?? 1);
