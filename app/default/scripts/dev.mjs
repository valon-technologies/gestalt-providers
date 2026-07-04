import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const catalogPath = process.env.GESTALT_APP_WRITE_CATALOG;
if (catalogPath) {
  writeFileSync(
    catalogPath,
    `name: home
operations:
  - id: static
    method: GET
    transport: app
`,
    "utf8",
  );
  process.exit(0);
}

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const viteBin = path.join(projectDir, "node_modules", "vite", "bin", "vite.js");
const child = spawn(process.execPath, [viteBin], {
  cwd: projectDir,
  stdio: "inherit",
  env: process.env,
});
child.on("exit", (code, signal) => {
  process.exit(code ?? (signal ? 1 : 0));
});
