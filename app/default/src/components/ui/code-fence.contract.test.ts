import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  codeFenceShellClass,
  codeFenceShellVariants,
} from "@/components/ui/code-fence";

const HERE = dirname(fileURLToPath(import.meta.url));
const GLOBALS = readFileSync(join(HERE, "../../globals.css"), "utf8");

describe("code-fence surface contract", () => {
  test("fence shell shares Card / Alert canvas rounded-lg", () => {
    const alert = readFileSync(join(HERE, "alert.tsx"), "utf8");
    const card = readFileSync(join(HERE, "card.tsx"), "utf8");
    expect(alert).toContain("rounded-lg");
    expect(card).toContain("[--radius-in-panel:var(--radius-nested)]");
    expect(alert).toContain("[--radius-in-panel:var(--radius-nested)]");
    expect(codeFenceShellVariants()).toContain(
      "rounded-[var(--radius-in-panel,_var(--radius-lg))]",
    );
    expect(codeFenceShellClass).toBe(codeFenceShellVariants({ variant: "outline" }));
    expect(codeFenceShellVariants()).not.toContain("rounded-md");
    expect(codeFenceShellVariants()).not.toContain("rounded-xl");
    expect(codeFenceShellVariants()).not.toContain("rounded-lg");
  });

  test("nested fence radius is inherited from padded panel variants", () => {
    const fence = readFileSync(join(HERE, "code-fence.tsx"), "utf8");
    const descriptionList = readFileSync(
      join(HERE, "description-list.tsx"),
      "utf8",
    );
    const dialog = readFileSync(join(HERE, "dialog.tsx"), "utf8");
    const alertDialog = readFileSync(join(HERE, "alert-dialog.tsx"), "utf8");
    expect(GLOBALS).toContain("--radius-nested: var(--radius-sm)");
    expect(GLOBALS).not.toContain("--radius-in-panel: var(--radius-nested)");
    expect(fence).toContain('data-slot="code-fence"');
    expect(descriptionList).toContain('cardVariants({ variant: "outline" })');
    expect(dialog).toContain("rounded-xl");
    expect(alertDialog).toMatch(
      /function AlertDialogContent[\s\S]*?className=\{cn\(\s*"[^"]*rounded-xl/,
    );
  });
});
