import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "copyable-code.tsx"), "utf8");

describe("CopyableCode", () => {
  test("never puts the clipboard payload in title, aria-label, or tooltip", () => {
    expect(SOURCE).not.toContain("isTruncated");
    expect(SOURCE).not.toContain("title={");
    expect(SOURCE).not.toContain("aria-label={value}");
    expect(SOURCE).not.toContain("aria-label={display}");
    expect(SOURCE).not.toContain("`Copy ${value}`");
    expect(SOURCE).not.toContain("tooltip={value}");
    expect(SOURCE).toContain("tooltip={tooltip}");
    expect(SOURCE).toContain(
      "<code className={copyableCodeTextVariants()}>{display}</code>",
    );
    expect(SOURCE).toContain("maskCopyableValue(value)");
    expect(SOURCE).toContain("maskSecretsInText");
    expect(SOURCE).toContain('data-slot="copyable-code-reveal"');
    expect(SOURCE).toContain('lg: "text-base"');
  });

  test("copy action fills the trailing cell", () => {
    expect(SOURCE).toContain("items-stretch self-stretch");
    expect(SOURCE).toContain('density="chip"');
  });
});
