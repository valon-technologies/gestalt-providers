/**
 * Oxlint owns lint for the Gestalt home console (app/default).
 *
 * Home-shell invariants live in ./oxlint-plugin-home.mjs. Fleet toolshed
 * rules (oxlint-gestalt) are intentionally not extended — that package is
 * private to toolshed and encodes a different token/overwrite model.
 *
 * Categories stay off by default (toolshed pattern): ship only intentional
 * rules so lint is fail-closed for conventions we care about, not a pile of
 * stylistic warnings inherited from ESLint migrations.
 */
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "oxlint";

const root = dirname(fileURLToPath(import.meta.url));
const homePlugin = join(root, "oxlint-plugin-home.mjs");

export default defineConfig({
  ignorePatterns: [
    "dist/**",
    "out/**",
    "node_modules/**",
    ".dev/**",
    ".local/**",
    "public/admin/echarts.min.js",
  ],
  jsPlugins: [homePlugin],
  categories: {
    correctness: "off",
    nursery: "off",
    pedantic: "off",
    perf: "off",
    restriction: "off",
    style: "off",
    suspicious: "off",
  },
  overrides: [
    {
      files: ["src/components/ui/**/*.{ts,tsx}"],
      rules: {
        "home/no-brand-text-on-selected": "error",
      },
    },
  ],
});
