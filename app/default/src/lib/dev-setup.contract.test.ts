import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "../..");
const repoRoot = join(appRoot, "../..");
const packageJson = JSON.parse(
  readFileSync(join(appRoot, "package.json"), "utf8"),
);
const profile = JSON.parse(
  readFileSync(
    join(repoRoot, ".engineering-playbook/local-dev.json"),
    "utf8",
  ),
);

describe("worktree development contract", () => {
  it("keeps the package development command frontend-only", () => {
    expect(packageJson.scripts.dev).toBe("vite");
  });

  it("starts an isolated Gestaltd backend before the frontend", () => {
    expect(profile).toMatchObject({
      schema_version: 1,
      port_range: [8200, 8399],
      services: [
        {
          id: "backend",
          cwd: ".",
          command: [
            "gestaltd",
            "serve",
            "--port",
            "{port}",
            "app/default",
          ],
          env: {
            GESTALT_PROVIDERS_DIR: "",
            TMPDIR: "{repo_root}/.local",
          },
          ready_timeout_seconds: 300,
        },
        {
          id: "frontend",
          cwd: "app/default",
          command: [
            "bun",
            "run",
            "dev",
            "--host",
            "{host}",
            "--port",
            "{port}",
            "--strictPort",
          ],
          env: {
            GESTALT_API_PROXY_TARGET:
              "http://{host}:{service_backend_port}",
            VITE_GESTALT_PUBLIC_ORIGIN:
              "http://{host}:{service_backend_port}",
          },
          health_url: "http://{host}:{port}/",
          ready_timeout_seconds: 60,
        },
      ],
    });
  });
});
