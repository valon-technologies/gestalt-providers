import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const themeCss = fs.readFileSync(path.join(projectDir, "shared", "theme.css"), "utf8");
const globalsCss = fs.readFileSync(path.join(projectDir, "src", "globals.css"), "utf8");
const mainSource = fs.readFileSync(path.join(projectDir, "src", "main.tsx"), "utf8");
const routerSource = fs.readFileSync(path.join(projectDir, "src", "router.tsx"), "utf8");
const mountSource = fs.readFileSync(path.join(projectDir, "src", "lib", "mount.ts"), "utf8");
const viteSource = fs.readFileSync(path.join(projectDir, "vite.config.ts"), "utf8");
const indexHtml = fs.readFileSync(path.join(projectDir, "index.html"), "utf8");

const requiredThemeTokens = [
  "background",
  "foreground",
  "card",
  "card-foreground",
  "popover",
  "popover-foreground",
  "primary",
  "primary-foreground",
  "secondary",
  "secondary-foreground",
  "muted",
  "muted-foreground",
  "accent",
  "accent-foreground",
  "destructive",
  "destructive-foreground",
  "border",
  "input",
  "ring",
  "success",
  "success-foreground",
  "warning",
  "warning-foreground",
  "info",
  "info-foreground",
  "radius",
  "ui-font-sans",
  "ui-font-display",
  "ui-font-mono",
  "heading-weight",
];
const geometryAndTypeTokens = new Set([
  "radius",
  "ui-font-sans",
  "ui-font-display",
  "ui-font-mono",
  "heading-weight",
]);

function declarationsFor(selector) {
  const match = themeCss.match(new RegExp(`${selector}\\s*\\{([\\s\\S]*?)\\n\\}`, "m"));
  assert(match, `missing ${selector} default block`);
  return match[1];
}

const lightDefaults = declarationsFor(":where\\(:root\\)");
const darkDefaults = declarationsFor(":where\\(.dark\\)");
for (const token of requiredThemeTokens) {
  const declaration = new RegExp(`--${token}:`);
  assert(declaration.test(lightDefaults), `light default is missing --${token}`);
  assert(
    declaration.test(darkDefaults) || geometryAndTypeTokens.has(token),
    `dark default is missing --${token}`,
  );
  if (!geometryAndTypeTokens.has(token)) {
    assert(
      globalsCss.includes(`--color-${token}: var(--${token});`),
      `Tailwind bridge is missing --${token}`,
    );
  }
}

assert(globalsCss.includes("--font-sans: var(--ui-font-sans);"), "Tailwind bridge is missing ui-font-sans");
assert(globalsCss.includes("--font-display: var(--ui-font-display);"), "Tailwind bridge is missing ui-font-display");
assert(globalsCss.includes("--font-mono: var(--ui-font-mono);"), "Tailwind bridge is missing ui-font-mono");
assert(globalsCss.includes("font-weight: var(--heading-weight);"), "base styles do not consume heading-weight");

function walk(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name);
    return entry.isDirectory() ? walk(entryPath) : [entryPath];
  });
}

const rawPaletteUtility = /(?:^|[^a-z-])(?:bg|text|border|ring|decoration|fill|stroke)-(?:slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose|base|gold|grove|ember|paper)-\d+/m;
const rawNeutralUtility = /(?:^|[^a-z-])(?:bg|text|border|ring|fill|stroke)-(?:white|black)(?:\b|\/)/m;
const legacyInkUtility = /(?<![\w-])(?:text-secondary|text-muted|text-faint|border-alpha(?:-strong)?|bg-alpha-(?:5|10)|divide-alpha)(?![\w-])/m;
const privateThemeToken = /--(?:tenant|valon)-[a-z0-9-]+/i;
for (const file of walk(path.join(projectDir, "src")).filter((entry) => /\.(?:ts|tsx|css)$/.test(entry))) {
  const source = fs.readFileSync(file, "utf8");
  const relativeFile = path.relative(projectDir, file);
  assert(!rawPaletteUtility.test(source), `${relativeFile} uses a raw palette utility`);
  assert(!rawNeutralUtility.test(source), `${relativeFile} uses a raw neutral utility`);
  assert(!legacyInkUtility.test(source), `${relativeFile} uses a legacy ink utility`);
  assert(!privateThemeToken.test(source), `${relativeFile} exposes a private theme token`);
}

assert(!mainSource.includes("@theme.css"), "main.tsx must not import a second theme stylesheet");
assert(!viteSource.includes("GESTALT_THEME_FILE"), "Vite must not read tenant theme files");
assert(!viteSource.includes('"@theme.css"'), "Vite must not alias a tenant stylesheet");
assert(viteSource.includes('base: "./"'), "Vite must emit mount-relative asset URLs");
assert(viteSource.includes('"/theme.css"'), "Vite must proxy the runtime stylesheet endpoint");
assert(viteSource.includes('"/theme/"'), "Vite must proxy runtime theme assets");
assert(routerSource.includes("basepath: appBasepath"), "router must use the runtime mount path");
assert(mountSource.includes("import.meta.env.BASE_URL"), "runtime mount detection must honor the native UI development base path");
assert.equal(
  (indexHtml.match(/<link\s+rel="stylesheet"\s+href="theme\.css"\s*\/>/g) ?? []).length,
  1,
  "index.html must have one relative runtime theme link",
);

console.log("theme contract verified");
