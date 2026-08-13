import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "agent-console.tsx"),
  "utf8",
);

/**
 * Prompt height is owned by one wrap recipe shared by the invisible measure and
 * the absolute typed line. The measure must also reserve the block cursor’s 1ch
 * advance — otherwise wrap at `w-[32rem]` can grow past the reserved height.
 */
describe("AgentConsoleInput measure contract", () => {
  test("measure and typed line share AGENT_CONSOLE_PROMPT_WRAP", () => {
    expect(SOURCE).toContain(
      'const AGENT_CONSOLE_PROMPT_WRAP =\n  "break-words whitespace-pre-wrap [overflow-wrap:anywhere]"',
    );
    expect(SOURCE).toContain('className={cn("invisible", AGENT_CONSOLE_PROMPT_WRAP)}');
    expect(SOURCE).toContain(
      "className={cn(AGENT_CONSOLE_PROMPT_WRAP, measureText ? \"absolute inset-0\" : null)}",
    );
  });

  test("measure reserves cursor advance matching AgentConsoleCursor width", () => {
    expect(SOURCE).toContain(
      'const AGENT_CONSOLE_CURSOR_ADVANCE = "inline-block h-[1.1em] w-[1ch] align-baseline"',
    );
    expect(SOURCE).toContain("<span className={AGENT_CONSOLE_CURSOR_ADVANCE} />");
    expect(SOURCE).toContain('w-[1ch]');
  });

  test("does not export an unused AgentConsoleSkin data shape", () => {
    expect(SOURCE).not.toContain("export type AgentConsoleSkin");
  });
});
