import { useMemo, useState, type ReactNode } from "react";
import { Search } from "lucide-react";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Input } from "@/components/ui/input";
import {
  codeFencePreClass,
  CodeFenceShell,
} from "@/components/ui/code-fence";
import { cn } from "@/lib/cn";
import type { WorkflowLogGroup } from "./workflow-run-graph";
import { formatDuration } from "./workflow-run-graph";
import { WorkflowStatusIcon } from "./workflow-status-icon";

/**
 * GHA-style collapsible stdout log viewer.
 * Ready for a future streaming backend: pass `groups` with line arrays
 * (and later append lines as the stream grows).
 */
export function WorkflowStepLogViewer({
  groups,
  emptyMessage = "No log output for this step yet.",
}: {
  groups: WorkflowLogGroup[];
  emptyMessage?: string;
}) {
  const [query, setQuery] = useState("");
  const needle = query.trim().toLowerCase();

  const filtered = useMemo(() => {
    if (!needle) return groups;
    return groups
      .map((group) => ({
        ...group,
        lines: group.lines.filter((line) =>
          line.text.toLowerCase().includes(needle),
        ),
      }))
      .filter((group) => group.lines.length > 0 || group.name.toLowerCase().includes(needle));
  }, [groups, needle]);

  const defaultOpen = useMemo(() => {
    return filtered
      .filter((group) => !group.defaultCollapsed)
      .map((group) => group.id);
  }, [filtered]);

  if (groups.length === 0) {
    return (
      <p className="text-sm text-muted-foreground" data-testid="workflow-step-logs-empty">
        {emptyMessage}
      </p>
    );
  }

  return (
    <div className="space-y-3" data-testid="workflow-step-log-viewer">
      <div className="relative max-w-md">
        <Search
          className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground"
          aria-hidden
        />
        <Input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search logs"
          className="pl-8"
          aria-label="Search logs"
        />
      </div>

      <CodeFenceShell variant="outline" className="bg-card">
        <Accordion
          type="multiple"
          defaultValue={defaultOpen}
          className="w-full"
        >
          {filtered.map((group) => (
            <AccordionItem key={group.id} value={group.id}>
              <AccordionTrigger className="px-3 py-2.5 hover:no-underline">
                <span className="flex min-w-0 flex-1 items-center gap-2 pr-2">
                  <WorkflowStatusIcon status={group.status || "succeeded"} />
                  <span className="truncate text-sm font-medium">
                    {group.name}
                  </span>
                  <span className="ml-auto shrink-0 text-xs font-normal text-muted-foreground tabular-nums">
                    {formatDuration(group.durationMs)}
                  </span>
                </span>
              </AccordionTrigger>
              <AccordionContent className="px-0 pb-0">
                <LogBody group={group} highlight={needle} />
              </AccordionContent>
            </AccordionItem>
          ))}
        </Accordion>
      </CodeFenceShell>

      {filtered.length === 0 ? (
        <p className="text-sm text-muted-foreground">No log lines match this search.</p>
      ) : null}
    </div>
  );
}

function LogBody({
  group,
  highlight,
}: {
  group: WorkflowLogGroup;
  highlight: string;
}) {
  if (group.lines.length === 0) {
    return (
      <p className="px-3 py-2 text-xs text-muted-foreground">No output.</p>
    );
  }

  return (
    <pre
      className={cn(
        codeFencePreClass,
        "max-h-[28rem] overflow-auto border-t border-border bg-muted/40 py-2 text-xs leading-5",
      )}
    >
      {group.lines.map((line, index) => {
        const number = line.number ?? index + 1;
        return (
          <div
            key={`${group.id}-${number}-${index}`}
            className={cn(
              "grid grid-cols-[3rem_minmax(0,1fr)] gap-x-3 px-3 hover:bg-accent/40",
              line.level === "error" && "text-destructive",
              line.level === "warning" && "text-warning-foreground",
            )}
          >
            <span className="select-none text-right text-muted-foreground/70 tabular-nums">
              {number}
            </span>
            <code className="whitespace-pre-wrap break-all text-foreground">
              {highlight ? highlightMatch(line.text, highlight) : line.text}
            </code>
          </div>
        );
      })}
    </pre>
  );
}

function highlightMatch(text: string, needle: string): ReactNode {
  const lower = text.toLowerCase();
  const start = lower.indexOf(needle);
  if (start < 0) return text;
  const end = start + needle.length;
  return (
    <>
      {text.slice(0, start)}
      <mark className="rounded-sm bg-accent-vivid/30 text-foreground">
        {text.slice(start, end)}
      </mark>
      {text.slice(end)}
    </>
  );
}
