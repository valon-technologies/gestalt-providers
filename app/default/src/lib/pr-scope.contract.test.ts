/**
 * PR scope contract — forbidden committed paths.
 *
 * Manifest: docs/agent/pr-scope-manifest.json
 *
 * Agents: after editing app/default/scripts/ or adding demo drivers, validate:
 *
 *   cd app/default && bun test src/lib/pr-scope.contract.test.ts
 */

import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "../../../..");
const manifest = JSON.parse(
  readFileSync(join(repoRoot, "docs/agent/pr-scope-manifest.json"), "utf8"),
);

function globToRegExp(glob: string): RegExp {
  const escaped = glob
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*\*/g, "<<<GLOBSTAR>>>")
    .replace(/\*/g, "[^/]*")
    .replace(/<<<GLOBSTAR>>>/g, ".*");
  return new RegExp(`^${escaped}$`);
}

function matchesForbidden(path: string, glob: string): boolean {
  return globToRegExp(glob).test(path.replace(/\\/g, "/"));
}

function gitTrackedFiles(): string[] {
  return execSync("git ls-files", { cwd: repoRoot, encoding: "utf8" })
    .trim()
    .split("\n")
    .filter(Boolean);
}

test("pr-scope manifest lists at least one forbidden path rule", () => {
  expect(manifest.forbiddenCommittedPaths?.length).toBeGreaterThan(0);
});

test("git index satisfies pr-scope manifest", () => {
  const violations: string[] = [];
  const rules = manifest.forbiddenCommittedPaths as Array<{
    glob: string;
    reason: string;
  }>;

  for (const file of gitTrackedFiles()) {
    for (const rule of rules) {
      if (matchesForbidden(file, rule.glob)) {
        violations.push(`${file}: ${rule.reason}`);
      }
    }
  }

  expect(violations, violations.join("\n")).toEqual([]);
});

test("ephemeral demo driver path stays gitignored", () => {
  const preferred = (manifest.ephemeralDemoDriver as { preferredPath: string })
    .preferredPath;
  const gitignore = readFileSync(
    join(repoRoot, "app/default/.gitignore"),
    "utf8",
  );
  expect(gitignore).toContain(".local/");
  expect(preferred).toMatch(/\.local\//);
});
