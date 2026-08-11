
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";

import { FileCode2 } from "lucide-react";
import { all, createLowlight } from "lowlight";

import { CopyIconButton } from "@/components/ui/copy-button";
import {
  CodeFenceHeader,
  CodeFenceShell,
  codeFenceHighlightClass,
  codeFencePreClass,
  codeLineEmphasisRowClassName,
  codeLineRowBleedClass,
  type CodeFenceShellProps,
} from "@/components/ui/code-fence";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { TooltipProvider } from "@/components/ui/tooltip";
import { cn } from "@/lib/cn";
import cliLanguage from "@/components/ui/code-block-cli-language";

// Display CodeBlock for install snippets / docs / AI messages — not the Plate
// editor fence. Highlighting uses the same lowlight → hljs class pipeline as
// markdown-editor, styled by typeset's `.typeset-code-hljs`. Surface
// paint comes from `code-fence` (shared with Plate code-block-node). Chrome
// (filename, copy, line numbers, tabs) is modeled on shadcnspace's CodeBlock.
// CLI command lines use the registered `cli` grammar (not highlight.js bash).

const lowlight = createLowlight(all);
lowlight.register("cli", cliLanguage);

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
  // Display snippets tagged sh/shell are almost always command lines, not
  // bash scripts — route them to the CLI grammar. Keep `bash` as real bash.
  sh: "cli",
  shell: "cli",
  "console-command": "cli",
  "shell-command": "cli",
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
  const lineNoCh = String(Math.max(lines.length, 1)).length;

  return (
    <div
      className={cn(
        "overflow-x-auto",
        scrollable && "overflow-y-auto",
        className,
      )}
      style={scrollable ? { maxHeight } : undefined}
    >
      <pre className={cn(codeFencePreClass, "overflow-x-visible")}>
        <code
          className={cn(
            codeFenceHighlightClass,
            "block min-w-full",
            showLineNumbers && "[counter-reset:line]",
          )}
          style={
            showLineNumbers
              ? ({ "--code-line-no-ch": `${lineNoCh}ch` } as React.CSSProperties)
              : undefined
          }
        >
          {lines.map((line, index) => {
            const lineNumber = index + 1;
            const isHighlighted = highlighted.has(lineNumber);
            return (
              <span
                key={lineNumber}
                className={cn(
                  // One row owns gutter + code. Bleed is on every row so
                  // gutters stay column-aligned; wash/edge are highlight-only
                  // (inset shadow — no border box shift).
                  "flex w-max min-w-full items-baseline",
                  codeLineRowBleedClass,
                  isHighlighted && codeLineEmphasisRowClassName,
                )}
              >
                {showLineNumbers ? (
                  <span
                    aria-hidden
                    className={cn(
                      "w-[var(--code-line-no-ch)] shrink-0 select-none pr-4 text-right text-xs leading-[inherit] text-muted-foreground/55 tabular-nums [counter-increment:line] before:content-[counter(line)]",
                      isHighlighted && "text-muted-foreground",
                    )}
                  />
                ) : null}
                <span className="whitespace-pre">{lineRowContent(line)}</span>
              </span>
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
      {/*
        Keep CodeFenceShell's data-slot="code-fence" so typeset-reading fence
        rules apply. Do not pass data-slot here — it would overwrite.
      */}
      <CodeFenceShell variant={variant} className={cn("w-full", className)}>
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
  /** Filename chrome only — omit for copy-only header (language is not a label). */
  label?: React.ReactNode;
  code: string;
  leading?: React.ReactNode;
}) {
  const hasLabel =
    label != null && !(typeof label === "string" && label.trim() === "");

  return (
    <CodeFenceHeader data-slot="code-block-header">
      <div className="flex min-w-0 items-center gap-2 text-muted-foreground">
        {hasLabel ? (
          <>
            {leading ?? <FileCode2 className="size-3.5 shrink-0" aria-hidden />}
            <span className="truncate font-mono text-xs">{label}</span>
          </>
        ) : null}
      </div>
      <CopyIconButton value={() => normalizeCodeNewlines(code)} />
    </CodeFenceHeader>
  );
}

/** Snippet chrome layout — header row vs inset copy (docs / blog style). */
export type CodeBlockChrome = "header" | "inset";

export type CodeBlockProps = {
  code: string;
  language?: string;
  filename?: string;
  /**
   * Chrome layout. `header` = optional filename row + copy (default).
   * `inset` = no header; copy overlays the code body (Vercel-style docs fence).
   */
  chrome?: CodeBlockChrome;
  /** Idle copy tooltip / aria-label (defaults to "Copy"). */
  copyLabel?: string;
  showLineNumbers?: boolean;
  scrollable?: boolean;
  maxHeight?: number;
  /** 1-based line numbers to emphasize. */
  highlightLines?: number[];
  /** `outline` = border, transparent fill (default). `solid` = muted fill. */
  variant?: CodeFenceVariant;
  className?: string;
};

/** True when the fence is one visual row — tighten body so inset copy insets match. */
function isSingleLineCode(code: string): boolean {
  return !normalizeCodeNewlines(code).includes("\n");
}

/**
 * Inset copy sits outside the horizontal scrollport (always visible on the end).
 * Use padding-token top/end — never `%` of the wrapper — so classic scrollbars
 * cannot shift the control off the code row (InstallCommand long URLs).
 */
function CodeBlockInsetCopy({
  code,
  copyLabel,
}: {
  code: string;
  copyLabel?: string;
}) {
  return (
    <div className="absolute end-1.5 top-1.5 z-10">
      <CopyIconButton
        value={() => normalizeCodeNewlines(code)}
        tooltip={copyLabel}
      />
    </div>
  );
}

function CodeBlock({
  code,
  language = "tsx",
  filename,
  chrome = "header",
  copyLabel,
  showLineNumbers = false,
  scrollable = false,
  maxHeight = 400,
  highlightLines,
  variant,
  className,
}: CodeBlockProps) {
  const inset = chrome === "inset";
  const singleLineInset = inset && isSingleLineCode(code);
  const body = (
    <CodeBody
      code={code}
      language={language}
      showLineNumbers={showLineNumbers}
      scrollable={scrollable}
      maxHeight={maxHeight}
      highlightLines={highlightLines}
      className={cn(
        // End padding clears the overlaid copy control.
        inset && "[&_pre]:pe-10",
        // Single-line: py + line box sized to icon-xs so top/end/bottom inset match
        // end-1.5/top-1.5 without %-centering against a scrollbar-inflated wrapper.
        singleLineInset &&
          "[&_pre]:py-1.5 [&_pre]:leading-[length:var(--size-control-xs)]",
      )}
    />
  );

  if (inset) {
    return (
      <CodeBlockShell className={className} variant={variant}>
        {/*
          Focus ring on inset copy can paint past the fence edge; keep a hair of
          room so overflow clip on the shell does not truncate it.
        */}
        <div data-slot="code-block-inset" className="relative p-px">
          <CodeBlockInsetCopy code={code} copyLabel={copyLabel} />
          {body}
        </div>
      </CodeBlockShell>
    );
  }

  return (
    <CodeBlockShell className={className} variant={variant}>
      <CodeBlockHeader label={filename} code={code} />
      {body}
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

  // PM picker + inset fence — copy inset/margins owned by chrome="inset", not a
  // parallel single-line flex row (that drifted to asymmetric pe vs py).
  return (
    <div data-slot="install-command" className={cn("space-y-2", className)}>
      <SegmentedControl
        size="sm"
        label="Package manager"
        value={pm}
        onValueChange={setPm}
        options={PACKAGE_MANAGER_OPTIONS}
        showLabels
      />
      <CodeBlock chrome="inset" language="cli" code={command} variant={variant} />
    </div>
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
