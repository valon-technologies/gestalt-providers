import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { gestalt } from "@valon-technologies/gestalt/vite";
import { defineConfig } from "vite";

const projectDir = path.dirname(fileURLToPath(import.meta.url));

const devThemeSource =
  process.env.NODE_ENV !== "production" && process.env.GESTALT_THEME_FILE
    ? path.resolve(process.env.GESTALT_THEME_FILE)
    : null;
const devThemeMirror = path.join(projectDir, ".dev", "theme.css");
if (devThemeSource) {
  fs.mkdirSync(path.dirname(devThemeMirror), { recursive: true });
  fs.rmSync(devThemeMirror, { force: true });
  fs.copyFileSync(devThemeSource, devThemeMirror);
}

const themeTarget = devThemeSource
  ? path.resolve(devThemeMirror)
  : path.resolve(projectDir, "src/theme.stub.css");

export default defineConfig({
  plugins: [react(), tailwindcss(), gestalt()],
  resolve: {
    alias: {
      "@": path.resolve(projectDir, "src"),
      "@theme.css": themeTarget,
    },
  },
});
