import {
  CodeBlock,
  type CodeBlockProps,
} from "@/components/ui/code-block";

/** Docs-owned mapping from highlight grammar to chrome header labels. */
const DEFAULT_HEADER_BY_LANGUAGE: Record<string, string> = {
  bash: "Terminal",
  shell: "Terminal",
  shellscript: "Terminal",
  sh: "Terminal",
};

const LANGUAGE_ALIASES: Record<string, string> = {
  shellscript: "bash",
  shell: "bash",
  sh: "bash",
};

export type DocsShellCodeProps = {
  code: string;
  language?: string;
  /** Overrides the header when grammar id is not reader-friendly. */
  filename?: string;
  variant?: CodeBlockProps["variant"];
};

/**
 * Docs shell snippet — registry CodeBlock with docs-owned header labels.
 * `language` selects highlighting; `filename` (or the default map) owns the header.
 */
export function DocsShellCode({
  code,
  language = "bash",
  filename,
  variant = "outline",
}: DocsShellCodeProps) {
  const normalized = LANGUAGE_ALIASES[language] ?? language;
  const header = filename ?? DEFAULT_HEADER_BY_LANGUAGE[language];

  return (
    <CodeBlock
      code={code}
      language={normalized}
      filename={header}
      variant={variant}
    />
  );
}
