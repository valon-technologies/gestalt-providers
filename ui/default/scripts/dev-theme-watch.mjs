// Mirrors GESTALT_THEME_FILE into .dev/theme.css so the @theme.css import
// hot-reloads while the source lives in another checkout (see THEMING.md).
// Polls a single file: cheap, atomic-rename-proof, and keeps the dev
// server's watch scope inside the project.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const source = process.env.GESTALT_THEME_FILE;
if (!source) {
  console.error(
    "dev-theme-watch: set GESTALT_THEME_FILE, e.g. in .env.local — loaded via npm run dev:theme (see THEMING.md)",
  );
  process.exit(1);
}
const resolved = path.resolve(source);
const mirror = path.join(projectDir, ".dev", "theme.css");

fs.mkdirSync(path.dirname(mirror), { recursive: true });

function sync() {
  try {
    // copyFileSync writes *through* an existing symlink; a mirror symlink
    // pointing outside the project root re-creates the Turbopack out-of-root
    // OOM (ISS-20260612-003). Remove before every copy.
    fs.rmSync(mirror, { force: true });
    fs.copyFileSync(resolved, mirror);
    console.log(`dev-theme-watch: synced ${new Date().toLocaleTimeString()}`);
  } catch (error) {
    console.error(`dev-theme-watch: ${error.message}`);
  }
}

sync();
fs.watchFile(resolved, { interval: 300 }, (curr, prev) => {
  if (curr.mtimeMs !== prev.mtimeMs) {
    sync();
  }
});
console.log(`dev-theme-watch: watching ${resolved}`);
