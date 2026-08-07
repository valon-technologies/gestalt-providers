import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DIR = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(DIR, "code-block.tsx"), "utf8");

describe("CodeBlock chrome contract", () => {
  test("exposes header | inset chrome with inset overlay copy", () => {
    expect(SOURCE).toContain('export type CodeBlockChrome = "header" | "inset"');
    expect(SOURCE).toContain('chrome = "header"');
    expect(SOURCE).toContain('if (inset)');
    expect(SOURCE).toContain('data-slot="code-block-inset"');
    expect(SOURCE).toContain("CodeBlockInsetCopy");
    expect(SOURCE).toContain("[&_pre]:pe-10");
  });

  test("InstallCommand uses inset CodeBlock instead of a parallel flex row", () => {
    expect(SOURCE).toContain(
      '<CodeBlock chrome="inset" language="cli" code={command} variant={variant} />',
    );
    expect(SOURCE).not.toContain("flex h-10 items-center justify-between gap-2 px-3");
  });
});
