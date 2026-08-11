/**
 * Typeset wiring contract — reading rhythm must stay a first-class module.
 *
 * Failure mode this guards: Vite serves globals without typeset-reading (e.g.
 * after a stash removed the file mid-session). Docs then render with every
 * block margin at 0 and look "collapsed," even though React markup is fine.
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const HERE = dirname(fileURLToPath(import.meta.url));
const APP_SRC = join(HERE, "..");
const MAIN = readFileSync(join(APP_SRC, "main.tsx"), "utf8");
const GLOBALS = readFileSync(join(APP_SRC, "globals.css"), "utf8");
const TYPESET = readFileSync(join(APP_SRC, "styles/typeset-reading.css"), "utf8");

test("main.tsx imports typeset-reading after globals.css", () => {
  const globalsAt = MAIN.indexOf('import "./globals.css"');
  const typesetAt = MAIN.indexOf('import "./styles/typeset-reading.css"');
  expect(globalsAt).toBeGreaterThanOrEqual(0);
  expect(typesetAt).toBeGreaterThan(globalsAt);
});

test("globals.css does not CSS-import typeset-reading (Vite graph owns it)", () => {
  expect(GLOBALS).not.toMatch(/@import\s+["'].*typeset-reading\.css["']/);
});

test("typeset-reading defines docs flow tokens and chrome-in-flow gaps", () => {
  expect(TYPESET).toMatch(/\.typeset-docs\s*\{[^}]*--typeset-flow:\s*1\.5em/s);
  expect(TYPESET).toMatch(
    /\.typeset\s*>\s*:where\(\.not-typeset,\s*\[data-not-typeset\],\s*\[data-typeset-chrome\]\)/,
  );
  expect(TYPESET).toContain("margin-block-start: var(--typeset-flow)");
});

test("typeset-reading paragraphs use text-wrap pretty to avoid orphans", () => {
  expect(TYPESET).toMatch(/&:where\(p\)\s*\{[^}]*text-wrap:\s*pretty/s);
});

test("typeset-reading stays tenant-neutral (no org brand filename/comments)", () => {
  expect(TYPESET).not.toMatch(/valon/i);
  expect(TYPESET).not.toMatch(/Melange|Season Serif/i);
});
