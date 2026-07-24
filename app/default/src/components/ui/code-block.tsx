"use client";

import * as React from "react";

import { FileCode2 } from "lucide-react";
import { all, createLowlight } from "lowlight";

import { CopyIconButton } from "@/components/ui/copy-button";
import {
  CodeFenceHeader,
  CodeFenceShell,
  codeFenceHighlightClass,
  codeFencePreClass,
  codeFenceShellVariants,
  codeLineEmphasisRowClass,
  type CodeFenceShellProps,
} from "@/components/ui/code-fence";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { TooltipProvider } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

// Display CodeBlock for install snippets / docs / AI messages — not the Plate
// editor fence. Highlighting uses the same lowlight → hljs class pipeline as
// markdown-editor, styled by valon-typeset's `.typeset-code-hljs`. Surface
// paint comes from `code-fence` (shared with Plate code-block-node). Chrome
// (filename, copy, line numbers, tabs) is modeled on shadcnspace's CodeBlock.

const lowlight = createLowlight(all);

type CodeFenceVariant = NonNullable<CodeFenceShellProps["variant"]>;

type HastElement = {
  type: "element";
  tagName: string;
  properties?: { className?: Array<string | number> | string };
  children: HastNode[];
};
type HastText = { type: "text"; value: string };
type HastRoot = { type: "root"; children: HastNode[] };
type HastNode = HastRoot | HastElement | HastText | { type: string };

const LANGUAGE_ALIASES: Record<string, string> = {
  js: "javascript",
  jsx: "javascript",
  ts: "typescript",
  tsx: "tsx",
  md: "markdown",
  sh: "bash",
  shell: "bash",
  yml: "yaml",
  plaintext: "plaintext",
  text: "plaintext",
  plain: "plaintext",
};

function resolveLanguage(language: string): string {
  const normalized = language.trim().toLowerCase();
  const aliased = LANGUAGE_ALIASES[normalized] ?? normalized;
  return lowlight.registered(aliased) ? aliased : "plaintext";
}

/** Canonical line endings for display + copy — one chokepoint for CRLF/CR. */
function normalizeCodeNewlines(code: string): string {
  return code.replace(/\r\n|\r/g, "\n");
}

function classNameFromProperties(
  properties: HastElement["properties"] | undefined,
): string | undefined {
  const value = properties?.className;
  if (Array.isArray(value)) return value.filter(Boolean).join(" ") || undefined;
  if (typeof value === "string") return value || undefined;
  return undefined;
}

/** Split a highlighted HAST tree into one React node per source line. */
function hastToHighlightedLines(tree: HastRoot): React.ReactNode[] {
  const lines: React.ReactNode[][] = [[]];

  const pushText = (text: string, className?: string) => {
    const parts = text.split("\n");
    parts.forEach((part, partIndex) => {
      if (partIndex > 0) lines.push([]);
      if (!part) return;
      const current = lines[lines.length - 1]!;
      current.push(
        className ? (
          <span key={`${lines.length}-${current.length}`} className={className}>
            {part}
          </span>
        ) : (
          <React.Fragment key={`${lines.length}-${current.length}`}>
            {part}
          </React.Fragment>
        ),
      );
    });
  };

  const walk = (node: HastNode, inheritedClass?: string) => {
    if (node.type === "text") {
      pushText((node as HastText).value, inheritedClass);
      return;
    }
    if (node.type === "root") {
      for (const child of (node as HastRoot).children) {
        walk(child, inheritedClass);
      }
      return;
    }
    if (node.type !== "element") return;
    const element = node as HastElement;
    const className =
      classNameFromProperties(element.properties) ?? inheritedClass;
    if (element.children.length === 0) return;
    for (const child of element.children) walk(child, className);
  };

  walk(tree);
  return lines.map((nodes, index) => (
    <React.Fragment key={index}>{nodes}</React.Fragment>
  ));
}

function highlightCodeToLines(
  code: string,
  language: string,
): React.ReactNode[] {
  const normalized = normalizeCodeNewlines(code);
  const lang = resolveLanguage(language);
  try {
    return hastToHighlightedLines(
      lowlight.highlight(lang, normalized) as HastRoot,
    );
  } catch {
    return normalized.split("\n").map((line, index) => (
      <React.Fragment key={index}>{line}</React.Fragment>
    ));
  }
}

type CodeBodyProps = {
  code: string;
  language: string;
  showLineNumbers?: boolean;
  scrollable?: boolean;
  maxHeight?: number;
  highlightLines?: number[];
  className?: string;
};

/**
 * Each highlighted line is already one CSS grid row. Do not append a trailing
 * `\n` inside `whitespace-pre` — that doubles line height. Keep a zero-width
 * space so empty rows still form a line box.
 */
function lineRowContent(line: React.ReactNode): React.ReactNode {
  return (
    <>
      {line}
      {"\u200b"}
    </>
  );
}

function CodeBody({
  code,
  language,
  showLineNumbers = false,
  scrollable = false,
  maxHeight = 400,
  highlightLines,
  className,
}: CodeBodyProps) {
  const lines = React.useMemo(
    () => highlightCodeToLines(code, language),
    [code, language],
  );
  const highlighted = React.useMemo(
    () => new Set(highlightLines ?? []),
    [highlightLines],
  );

  return (
    <div
      className={cn(
        "overflow-x-auto",
        scrollable && "overflow-y-auto",
        className,
      )}
      style={scrollable ? { maxHeight } : undefined}
    >
      <pre className={codeFencePreClass}>
        <code
          className={cn(
            codeFenceHighlightClass,
            "grid min-w-full text-[length:inherit] leading-[inherit]",
            showLineNumbers &&
              "grid-cols-[auto_1fr] gap-x-4 [counter-reset:line]",
          )}
        >
          {lines.map((line, index) => {
            const lineNumber = index + 1;
            const isHighlighted = highlighted.has(lineNumber);
            return (
              <React.Fragment key={lineNumber}>
                {showLineNumbers ? (
                  <span
                    aria-hidden
                    className={cn(
                      "select-none text-right text-xs leading-[inherit] text-muted-foreground/55 [counter-increment:line] before:content-[counter(line)]",
                      isHighlighted && "text-muted-foreground",
                    )}
                  />
                ) : null}
                <span
                  className={cn(
                    "min-w-0 whitespace-pre",
                    isHighlighted && codeLineEmphasisRowClass(showLineNumbers),
                  )}
                >
                  {lineRowContent(line)}
                </span>
              </React.Fragment>
            );
          })}
        </code>
      </pre>
    </div>
  );
}

export type CodeBlockPaneIdentity = {
  /** Caller-owned SoT when the list is dynamic; wins over derived identity. */
  id?: string;
  label: string;
  language: string;
  code: string;
};

type CodeBlockPaneRecord = {
  label: string;
  language: string;
  code: string;
  callerId?: string;
  id: string;
};

type CodeBlockPaneIdState = {
  records: CodeBlockPaneRecord[];
  seq: number;
};

const EMPTY_PANE_ID_STATE: CodeBlockPaneIdState = { records: [], seq: 0 };

/**
 * Reconcile pane Tabs values across list edits.
 *
 * - Caller `id` is used as-is (occurrence-suffixed when duplicated in-frame).
 * - Derived panes reuse a prior id by exact (label, language, code) match, else
 *   soft (label, language) match so streaming edits keep selection.
 * - New panes allocate a monotonic id — never list-index suffixes that swap
 *   on reorder.
 */
function reconcileCodeBlockPaneIds(
  state: CodeBlockPaneIdState,
  panes: readonly CodeBlockPaneIdentity[],
): { ids: string[]; state: CodeBlockPaneIdState } {
  const used = new Set<number>();
  const issuedCaller = new Map<string, number>();
  const ids: string[] = [];
  const nextRecords: CodeBlockPaneRecord[] = [];
  let seq = state.seq;

  for (const pane of panes) {
    let id: string;
    if (pane.id) {
      const occurrence = issuedCaller.get(pane.id) ?? 0;
      issuedCaller.set(pane.id, occurrence + 1);
      id = occurrence === 0 ? pane.id : `${pane.id}\u001f${occurrence}`;
    } else {
      let idx = state.records.findIndex(
        (record, index) =>
          !used.has(index) &&
          record.callerId === undefined &&
          record.label === pane.label &&
          record.language === pane.language &&
          record.code === pane.code,
      );
      if (idx < 0) {
        idx = state.records.findIndex(
          (record, index) =>
            !used.has(index) &&
            record.callerId === undefined &&
            record.label === pane.label &&
            record.language === pane.language,
        );
      }
      if (idx >= 0) {
        used.add(idx);
        id = state.records[idx]!.id;
      } else {
        id = `${pane.label}\u001f${pane.language}\u001f#${seq}`;
        seq += 1;
      }
    }
    ids.push(id);
    nextRecords.push({
      label: pane.label,
      language: pane.language,
      code: pane.code,
      callerId: pane.id,
      id,
    });
  }

  return { ids, state: { records: nextRecords, seq } };
}

/** Assign pane ids from an empty registry (tests / one-shot lists). */
function assignCodeBlockPaneIds(
  panes: readonly CodeBlockPaneIdentity[],
): string[] {
  return reconcileCodeBlockPaneIds(EMPTY_PANE_ID_STATE, panes).ids;
}

/**
 * Stable Tabs value helper for a single pane. Prefer `assignCodeBlockPaneIds`
 * / `useCodeBlockPaneIds` for lists — those own uniqueness and reconciliation.
 */
function codeBlockPaneId(
  pane: CodeBlockPaneIdentity,
  occurrence = 0,
): string {
  const base = pane.id ?? `${pane.label}\u001f${pane.language}`;
  return occurrence === 0 ? base : `${base}\u001f${occurrence}`;
}

/** Persist pane ids across renders so reorder/stream keep the right selection. */
function useCodeBlockPaneIds(panes: readonly CodeBlockPaneIdentity[]) {
  const stateRef = React.useRef<CodeBlockPaneIdState>(EMPTY_PANE_ID_STATE);
  const { ids, state } = reconcileCodeBlockPaneIds(stateRef.current, panes);
  stateRef.current = state;
  return ids;
}

/** Clamp controlled tab value onto the current pane id list. */
function resolveActivePaneId(
  active: string,
  paneIds: readonly string[],
): string {
  if (paneIds.includes(active)) return active;
  return paneIds[0] ?? "";
}

/**
 * Selection owned by pane identity. When the pane list changes and the
 * current id is gone, fall back to the first pane (setState-during-render).
 */
function useActivePaneId(paneIds: readonly string[]) {
  const fallback = paneIds[0] ?? "";
  const [active, setActive] = React.useState(fallback);
  const resolved = resolveActivePaneId(active, paneIds);
  if (active !== resolved) {
    setActive(resolved);
  }
  return [resolved, setActive] as const;
}

/**
 * Keep every pane mounted (forceMount) so highlight isn't re-paid on switch.
 * Width sizes to max(panes) via measured minWidth; height follows the active
 * pane only — inactive panes are out of flow (`absolute`), never grid-stacked,
 * so a taller inactive file cannot inflate the fence.
 * `inert` keeps inactive copy controls out of the focus order while mounted.
 */
const codeBlockTabPanelClass =
  "mt-0 outline-none data-[state=inactive]:pointer-events-none data-[state=inactive]:invisible data-[state=inactive]:absolute data-[state=inactive]:top-0 data-[state=inactive]:left-0";

function CodeBlockTabPanelStack({
  children,
}: {
  children: React.ReactNode;
}) {
  const ref = React.useRef<HTMLDivElement>(null);
  const [minWidth, setMinWidth] = React.useState<number | undefined>(undefined);

  React.useLayoutEffect(() => {
    const root = ref.current;
    if (!root) return;

    const measure = () => {
      let max = 0;
      for (const child of Array.from(root.children)) {
        max = Math.max(max, (child as HTMLElement).scrollWidth);
      }
      setMinWidth((prev) => (prev === max ? prev : max || undefined));
    };

    measure();
    const ro = new ResizeObserver(measure);
    for (const child of Array.from(root.children)) {
      ro.observe(child);
    }
    return () => ro.disconnect();
  }, [children]);

  return (
    <div
      ref={ref}
      className="relative"
      style={minWidth != null ? { minWidth } : undefined}
    >
      {children}
    </div>
  );
}

function CodeBlockShell({
  className,
  variant,
  children,
}: {
  className?: string;
  variant?: CodeFenceVariant;
  children: React.ReactNode;
}) {
  return (
    <TooltipProvider delayDuration={0}>
      <CodeFenceShell
        data-slot="code-block"
        variant={variant}
        className={cn("w-full", className)}
      >
        {children}
      </CodeFenceShell>
    </TooltipProvider>
  );
}

function CodeBlockHeader({
  label,
  code,
  leading,
}: {
  label: React.ReactNode;
  code: string;
  leading?: React.ReactNode;
}) {
  return (
    <CodeFenceHeader data-slot="code-block-header">
      <div className="flex min-w-0 items-center gap-2 text-muted-foreground">
        {leading ?? <FileCode2 className="size-3.5 shrink-0" aria-hidden />}
        <span className="truncate font-mono text-xs">{label}</span>
      </div>
      <CopyIconButton value={() => normalizeCodeNewlines(code)} />
    </CodeFenceHeader>
  );
}

export type CodeBlockProps = {
  code: string;
  language?: string;
  filename?: string;
  showLineNumbers?: boolean;
  scrollable?: boolean;
  maxHeight?: number;
  /** 1-based line numbers to emphasize. */
  highlightLines?: number[];
  /** `outline` = border, transparent fill (default). `solid` = muted fill. */
  variant?: CodeFenceVariant;
  className?: string;
};

function CodeBlock({
  code,
  language = "tsx",
  filename,
  showLineNumbers = false,
  scrollable = false,
  maxHeight = 400,
  highlightLines,
  variant,
  className,
}: CodeBlockProps) {
  return (
    <CodeBlockShell className={className} variant={variant}>
      <CodeBlockHeader label={filename ?? language} code={code} />
      <CodeBody
        code={code}
        language={language}
        showLineNumbers={showLineNumbers}
        scrollable={scrollable}
        maxHeight={maxHeight}
        highlightLines={highlightLines}
      />
    </CodeBlockShell>
  );
}

export type CodeBlockFile = {
  /** Stable selection id across list edits; defaults to content-derived identity. */
  id?: string;
  filename: string;
  code: string;
  language?: string;
};

export type MultiFileCodeBlockProps = {
  files: CodeBlockFile[];
  showLineNumbers?: boolean;
  scrollable?: boolean;
  maxHeight?: number;
  variant?: CodeFenceVariant;
  className?: string;
};

function MultiFileCodeBlock({
  files,
  showLineNumbers = false,
  scrollable = false,
  maxHeight = 400,
  variant,
  className,
}: MultiFileCodeBlockProps) {
  const drafts = files.map((entry) => ({
    id: entry.id,
    label: entry.filename,
    code: entry.code,
    language: entry.language ?? "tsx",
  }));
  const paneIds = useCodeBlockPaneIds(drafts);
  const panes = drafts.map((entry, index) => ({
    ...entry,
    id: paneIds[index]!,
  }));
  const [active, setActive] = useActivePaneId(paneIds);
  const file = panes.find((pane) => pane.id === active) ?? panes[0];

  if (!file) return null;

  return (
    <CodeBlockShell className={className} variant={variant}>
      <Tabs
        value={active}
        onValueChange={setActive}
        className="w-full gap-0"
      >
        <div className="flex items-center gap-1 border-b border-border pr-1">
          <TabsList
            size="default"
            aria-label="Files"
            className="min-w-0 flex-1 justify-start overflow-x-auto rounded-none border-0"
          >
            {panes.map((pane) => (
              <TabsTrigger
                key={pane.id}
                value={pane.id}
                className="flex-none shrink-0 font-mono text-xs"
              >
                {pane.label}
              </TabsTrigger>
            ))}
          </TabsList>
          <CopyIconButton value={() => normalizeCodeNewlines(file.code)} />
        </div>
        <CodeBlockTabPanelStack>
          {panes.map((pane) => (
            <TabsContent
              key={pane.id}
              value={pane.id}
              forceMount
              inert={pane.id !== active ? true : undefined}
              aria-hidden={pane.id !== active}
              tabIndex={pane.id === active ? 0 : -1}
              className={codeBlockTabPanelClass}
            >
              <CodeBody
                code={pane.code}
                language={pane.language}
                showLineNumbers={showLineNumbers}
                scrollable={scrollable}
                maxHeight={maxHeight}
              />
            </TabsContent>
          ))}
        </CodeBlockTabPanelStack>
      </Tabs>
    </CodeBlockShell>
  );
}

export type CodeBlockLanguageTab = {
  /** Stable selection id across list edits; defaults to content-derived identity. */
  id?: string;
  label: string;
  filename: string;
  code: string;
  language?: string;
};

export type LanguageTabsCodeBlockProps = {
  tabs: CodeBlockLanguageTab[];
  showLineNumbers?: boolean;
  scrollable?: boolean;
  maxHeight?: number;
  variant?: CodeFenceVariant;
  className?: string;
};

function LanguageTabsCodeBlock({
  tabs,
  showLineNumbers = false,
  scrollable = false,
  maxHeight = 400,
  variant,
  className,
}: LanguageTabsCodeBlockProps) {
  const drafts = tabs.map((entry) => ({
    id: entry.id,
    label: entry.label,
    filename: entry.filename,
    code: entry.code,
    language: entry.language ?? "tsx",
  }));
  const paneIds = useCodeBlockPaneIds(drafts);
  const panes = drafts.map((entry, index) => ({
    ...entry,
    id: paneIds[index]!,
  }));
  const [active, setActive] = useActivePaneId(paneIds);

  if (panes.length === 0) return null;

  return (
    <CodeBlockShell className={className} variant={variant}>
      <Tabs
        value={active}
        onValueChange={setActive}
        className="w-full gap-0"
      >
        <TabsList
          size="default"
          aria-label="Languages"
          className="w-full justify-start overflow-x-auto rounded-none border-border"
        >
          {panes.map((pane) => (
            <TabsTrigger
              key={pane.id}
              value={pane.id}
              className="flex-none shrink-0"
            >
              {pane.label}
            </TabsTrigger>
          ))}
        </TabsList>
        <CodeBlockTabPanelStack>
          {panes.map((pane) => (
            <TabsContent
              key={pane.id}
              value={pane.id}
              forceMount
              inert={pane.id !== active ? true : undefined}
              aria-hidden={pane.id !== active}
              tabIndex={pane.id === active ? 0 : -1}
              className={codeBlockTabPanelClass}
            >
              <CodeBlockHeader label={pane.filename} code={pane.code} />
              <CodeBody
                code={pane.code}
                language={pane.language}
                showLineNumbers={showLineNumbers}
                scrollable={scrollable}
                maxHeight={maxHeight}
              />
            </TabsContent>
          ))}
        </CodeBlockTabPanelStack>
      </Tabs>
    </CodeBlockShell>
  );
}

const PACKAGE_MANAGERS = ["pnpm", "npm", "yarn", "bun"] as const;
type PackageManager = (typeof PACKAGE_MANAGERS)[number];

const PACKAGE_MANAGER_OPTIONS: SegmentedControlOption<PackageManager>[] =
  PACKAGE_MANAGERS.map((value) => ({ value, label: value }));

function buildInstallCommand(pm: PackageManager, registryUrl: string): string {
  switch (pm) {
    case "npm":
      return `npx shadcn@latest add ${registryUrl}`;
    case "yarn":
      return `yarn dlx shadcn@latest add ${registryUrl}`;
    case "bun":
      return `bunx --bun shadcn@latest add ${registryUrl}`;
    default:
      return `pnpm dlx shadcn@latest add ${registryUrl}`;
  }
}

export type InstallCommandProps = {
  registryUrl: string;
  variant?: CodeFenceVariant;
  className?: string;
};

function InstallCommand({ registryUrl, variant, className }: InstallCommandProps) {
  const [pm, setPm] = React.useState<PackageManager>("pnpm");
  const command = buildInstallCommand(pm, registryUrl);

  return (
    <TooltipProvider delayDuration={0}>
      <div data-slot="install-command" className={cn("space-y-2", className)}>
        <SegmentedControl
          size="sm"
          label="Package manager"
          value={pm}
          onValueChange={setPm}
          options={PACKAGE_MANAGER_OPTIONS}
          showLabels
        />
        <div
          className={cn(
            codeFenceShellVariants({ variant }),
            "flex h-10 items-center justify-between gap-2 px-3",
          )}
        >
          <code className="grow truncate font-mono text-sm">{command}</code>
          <CopyIconButton value={() => normalizeCodeNewlines(command)} />
        </div>
      </div>
    </TooltipProvider>
  );
}

export {
  CodeBlock,
  MultiFileCodeBlock,
  LanguageTabsCodeBlock,
  InstallCommand,
  // Exported for tests / custom chrome.
  highlightCodeToLines,
  normalizeCodeNewlines,
  resolveLanguage,
  codeBlockPaneId,
  assignCodeBlockPaneIds,
  reconcileCodeBlockPaneIds,
  resolveActivePaneId,
  lineRowContent,
};
