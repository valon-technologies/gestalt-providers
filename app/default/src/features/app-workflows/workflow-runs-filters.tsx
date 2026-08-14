import { useDeferredValue, useEffect, useId, useMemo, useRef, useState } from "react";
import { PlusCircle, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Switch } from "@/components/ui/switch";
import { WorkflowStatusIcon } from "@/features/app-workflows/workflow-status-icon";
import {
  shortDefinitionId,
} from "@/features/app-workflows/workflow-format";
import {
  WORKFLOW_RUNS_GROUP_BY_LABEL,
  mergeWorkflowDefinitionIds,
} from "@/features/app-workflows/workflow-runs-group";
import {
  WORKFLOW_RUNS_LIST_STATUSES,
  type WorkflowRunsListQuery,
  type WorkflowRunsListStatus,
  workflowRunsListQueryIsActive,
  workflowRunsStatusFilterScope,
} from "@/features/app-workflows/workflow-runs-list-query";

const STATUS_LABELS: Record<WorkflowRunsListStatus, string> = {
  pending: "Pending",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  canceled: "Canceled",
};

const MAX_STATUS_BADGES = 2;

export function WorkflowRunsFilters({
  query,
  definitionOptions,
  disabled,
  onChange,
  onClear,
}: {
  query: WorkflowRunsListQuery;
  definitionOptions: string[];
  disabled?: boolean;
  onChange: (next: WorkflowRunsListQuery) => void;
  onClear: () => void;
}) {
  const [searchDraft, setSearchDraft] = useState(query.q);
  const deferredSearch = useDeferredValue(searchDraft);
  const [statusOpen, setStatusOpen] = useState(false);
  const [definitionOpen, setDefinitionOpen] = useState(false);
  const groupById = useId();
  const queryRef = useRef(query);
  const onChangeRef = useRef(onChange);
  const statusScope = workflowRunsStatusFilterScope(query);
  const active = workflowRunsListQueryIsActive(query);
  const selectedStatuses = query.statuses;

  queryRef.current = query;
  onChangeRef.current = onChange;

  useEffect(() => {
    setSearchDraft(query.q);
  }, [query.q]);

  useEffect(() => {
    if (deferredSearch === queryRef.current.q) return;
    onChangeRef.current({ ...queryRef.current, q: deferredSearch });
  }, [deferredSearch]);

  function toggleStatus(status: WorkflowRunsListStatus) {
    const next = new Set(selectedStatuses);
    if (next.has(status)) next.delete(status);
    else next.add(status);
    onChange({
      ...query,
      statuses: WORKFLOW_RUNS_LIST_STATUSES.filter((value) => next.has(value)),
    });
  }

  const definitionLabel = query.definitionId
    ? shortDefinitionId(query.definitionId)
    : "Definition";

  return (
    <div className="space-y-3" data-testid="workflow-runs-filters">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
        <Input
          type="search"
          value={searchDraft}
          onChange={(event) => setSearchDraft(event.target.value)}
          placeholder="Run ID, definition, event…"
          disabled={disabled}
          aria-label="Search runs"
          data-testid="workflow-runs-search"
          className="lg:max-w-sm [&::-webkit-search-cancel-button]:hidden"
        />
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">
          <Popover open={statusOpen} onOpenChange={setStatusOpen}>
            <PopoverTrigger asChild>
              <Button
                type="button"
                variant="outline"
                className="border-dashed"
                disabled={disabled}
                aria-label={
                  selectedStatuses.length > 0
                    ? `Filter by status, ${selectedStatuses.length} selected: ${selectedStatuses
                        .map((status) => STATUS_LABELS[status])
                        .join(", ")}`
                    : "Filter by status"
                }
                data-testid="workflow-runs-status-filter"
              >
                <PlusCircle className="size-3.5" aria-hidden />
                Status
                {selectedStatuses.length > 0 ? (
                  <>
                    <span aria-hidden className="mx-0.5 h-4 w-px bg-border" />
                    <Badge variant="secondary" size="sm" className="lg:hidden">
                      {selectedStatuses.length}
                    </Badge>
                    <span className="hidden gap-1 lg:flex">
                      {selectedStatuses.length > MAX_STATUS_BADGES ? (
                        <Badge variant="secondary" size="sm">
                          {selectedStatuses.length} selected
                        </Badge>
                      ) : (
                        selectedStatuses.map((status) => (
                          <Badge
                            key={status}
                            variant="secondary"
                            size="sm"
                            className="gap-1"
                          >
                            <WorkflowStatusIcon status={status} size="sm" />
                            {STATUS_LABELS[status]}
                          </Badge>
                        ))
                      )}
                    </span>
                  </>
                ) : null}
              </Button>
            </PopoverTrigger>
            <PopoverContent
              className="flex w-56 flex-col overflow-hidden p-0"
              align="start"
            >
              <Command className="min-h-0 flex-1">
                <CommandInput placeholder="Status" aria-label="Search statuses" />
                <CommandList className="min-h-0 max-h-[300px] flex-1">
                  <CommandEmpty>No statuses.</CommandEmpty>
                  <CommandGroup>
                    {WORKFLOW_RUNS_LIST_STATUSES.map((status) => {
                      const isSelected = selectedStatuses.includes(status);
                      return (
                        <CommandItem
                          key={status}
                          value={`${STATUS_LABELS[status]} ${status}`}
                          onSelect={() => toggleStatus(status)}
                          data-testid={`workflow-runs-status-${status}`}
                        >
                          <Checkbox
                            checked={isSelected}
                            tabIndex={-1}
                            aria-hidden
                            className="pointer-events-none mr-2"
                          />
                          <WorkflowStatusIcon status={status} size="sm" />
                          <span className="truncate">
                            {STATUS_LABELS[status]}
                          </span>
                        </CommandItem>
                      );
                    })}
                  </CommandGroup>
                </CommandList>
                {selectedStatuses.length > 0 ? (
                  <div className="shrink-0 border-t border-border p-1">
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="w-full justify-center"
                      onClick={() => onChange({ ...query, statuses: [] })}
                    >
                      Clear filter
                    </Button>
                  </div>
                ) : null}
              </Command>
            </PopoverContent>
          </Popover>

          <Popover open={definitionOpen} onOpenChange={setDefinitionOpen}>
            <PopoverTrigger asChild>
              <Button
                type="button"
                variant="outline"
                className="border-dashed"
                disabled={disabled || definitionOptions.length === 0}
                aria-label={
                  query.definitionId
                    ? `Filter by definition, selected: ${query.definitionId}`
                    : "Filter by definition"
                }
                data-testid="workflow-runs-definition-filter"
              >
                <PlusCircle className="size-3.5" aria-hidden />
                {query.definitionId ? (
                  <>
                    Definition
                    <span aria-hidden className="mx-0.5 h-4 w-px bg-border" />
                    <Badge variant="secondary" size="sm">
                      <code className="text-xs">{definitionLabel}</code>
                    </Badge>
                  </>
                ) : (
                  "Definition"
                )}
              </Button>
            </PopoverTrigger>
            <PopoverContent
              className="flex w-[min(28rem,calc(100vw-2rem))] flex-col overflow-hidden p-0"
              align="start"
            >
              <Command className="min-h-0 flex-1">
                <CommandInput
                  placeholder="Definition"
                  aria-label="Search definitions"
                />
                <CommandList className="min-h-0 max-h-[300px] flex-1">
                  <CommandEmpty>No definitions.</CommandEmpty>
                  <CommandGroup>
                    {definitionOptions.map((definitionId) => {
                      const selected = query.definitionId === definitionId;
                      return (
                        <CommandItem
                          key={definitionId}
                          value={definitionId}
                          onSelect={() => {
                            onChange({
                              ...query,
                              definitionId: selected ? undefined : definitionId,
                            });
                            setDefinitionOpen(false);
                          }}
                        >
                          <Checkbox
                            checked={selected}
                            tabIndex={-1}
                            aria-hidden
                            className="pointer-events-none mr-2"
                          />
                          <span className="whitespace-normal break-all font-mono text-xs">
                            {definitionId}
                          </span>
                        </CommandItem>
                      );
                    })}
                  </CommandGroup>
                </CommandList>
              </Command>
              {query.definitionId ? (
                <div className="border-t border-border p-1">
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="w-full justify-center"
                    onClick={() => {
                      onChange({ ...query, definitionId: undefined });
                      setDefinitionOpen(false);
                    }}
                  >
                    Clear definition filter
                  </Button>
                </div>
              ) : null}
            </PopoverContent>
          </Popover>

          {active ? (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={onClear}
              disabled={disabled}
              data-testid="workflow-runs-clear-filters"
            >
              <X className="size-3.5" aria-hidden />
              Clear filters
            </Button>
          ) : null}

          <div className="flex items-center gap-2 lg:ml-auto">
            <Label htmlFor={groupById} variant="inline">
              {WORKFLOW_RUNS_GROUP_BY_LABEL}
            </Label>
            <Switch
              id={groupById}
              checked={query.groupBy === "definition"}
              onCheckedChange={(checked) =>
                onChange({
                  ...query,
                  groupBy: checked ? "definition" : "none",
                })
              }
              data-testid="workflow-runs-group-by-definition"
            />
          </div>
        </div>
      </div>

      {statusScope === "loaded-only" ? (
        <p className="text-xs text-muted-foreground">
          Status filters apply to runs loaded so far. Choose one status to
          filter all runs, or load more runs.
        </p>
      ) : null}
      {statusScope === "server" ? (
        <p className="text-xs text-muted-foreground">
          Showing {STATUS_LABELS[query.statuses[0] ?? "pending"].toLowerCase()}{" "}
          runs.
        </p>
      ) : null}
    </div>
  );
}

export function useWorkflowDefinitionFilterOptions(
  apiDefinitionIds: string[],
  runDefinitionIds: string[],
): string[] {
  return useMemo(
    () =>
      mergeWorkflowDefinitionIds(apiDefinitionIds, runDefinitionIds).sort(
        (a, b) => a.localeCompare(b),
      ),
    [apiDefinitionIds, runDefinitionIds],
  );
}
