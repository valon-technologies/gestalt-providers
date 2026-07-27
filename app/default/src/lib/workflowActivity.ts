import type { WorkflowRun, WorkflowTarget } from "@/lib/api";

export type WorkflowDefinitionActivity = {
  definitionId: string;
  runCount: number;
  scheduleCount: number;
  eventCount: number;
  manualCount: number;
  otherCount: number;
  lastStatus?: string;
  lastCreatedAt?: string;
  subjects: string[];
  activationIds: string[];
  eventSources: string[];
  eventTypes: string[];
};

/** Aggregate definition / activation signals visible from recent runs. */
export function summarizeWorkflowDefinitionsFromRuns(
  runs: WorkflowRun[],
): WorkflowDefinitionActivity[] {
  const byId = new Map<string, WorkflowDefinitionActivity>();

  for (const run of runs) {
    const definitionId = run.definitionId?.trim() || "(untitled definition)";
    let entry = byId.get(definitionId);
    if (!entry) {
      entry = {
        definitionId,
        runCount: 0,
        scheduleCount: 0,
        eventCount: 0,
        manualCount: 0,
        otherCount: 0,
        subjects: [],
        activationIds: [],
        eventSources: [],
        eventTypes: [],
      };
      byId.set(definitionId, entry);
    }

    entry.runCount += 1;
    const kind = (run.trigger?.kind || "").toLowerCase();
    if (kind === "schedule" || kind === "cron") {
      entry.scheduleCount += 1;
    } else if (kind === "event") {
      entry.eventCount += 1;
    } else if (kind === "manual" || kind === "") {
      entry.manualCount += 1;
    } else {
      entry.otherCount += 1;
    }

    if (
      !entry.lastCreatedAt ||
      (run.createdAt && run.createdAt > entry.lastCreatedAt)
    ) {
      entry.lastCreatedAt = run.createdAt;
      entry.lastStatus = run.status;
    }

    const subject = run.createdBy?.subjectId?.trim();
    if (subject && !entry.subjects.includes(subject)) {
      entry.subjects.push(subject);
    }
    const activationId = run.trigger?.activationId?.trim();
    if (activationId && !entry.activationIds.includes(activationId)) {
      entry.activationIds.push(activationId);
    }
    const eventSource = run.trigger?.event?.source?.trim();
    if (eventSource && !entry.eventSources.includes(eventSource)) {
      entry.eventSources.push(eventSource);
    }
    const eventType = run.trigger?.event?.type?.trim();
    if (eventType && !entry.eventTypes.includes(eventType)) {
      entry.eventTypes.push(eventType);
    }
  }

  return [...byId.values()].sort((left, right) => {
    const leftAt = left.lastCreatedAt || "";
    const rightAt = right.lastCreatedAt || "";
    return rightAt.localeCompare(leftAt);
  });
}

export function collectAutomationSubjects(runs: WorkflowRun[]): string[] {
  const subjects = new Set<string>();
  for (const run of runs) {
    const subject = run.createdBy?.subjectId?.trim();
    if (subject) subjects.add(subject);
  }
  return [...subjects].sort();
}

/** Apps referenced by steps on a workflow target. */
export function workflowTargetAppNames(target: WorkflowTarget): string[] {
  const names = new Set<string>();
  for (const step of target.steps) {
    const name = step.app?.name?.trim();
    if (name) names.add(name);
  }
  return [...names];
}

/**
 * Best-effort ownership signal when list responses omit hydrated targets.
 * Definition IDs are conventionally `app_<appName>_…`.
 */
export function workflowRunDefinitionApp(run: WorkflowRun): string | null {
  const definitionId = run.definitionId?.trim();
  if (!definitionId?.startsWith("app_")) return null;
  const rest = definitionId.slice("app_".length);
  if (!rest) return null;
  const underscore = rest.indexOf("_");
  return underscore === -1 ? rest : rest.slice(0, underscore);
}

export function workflowRunMatchesApp(run: WorkflowRun, appName: string): boolean {
  const needle = appName.trim();
  if (!needle) return false;
  if (workflowTargetAppNames(run.target).includes(needle)) return true;
  return workflowRunDefinitionApp(run) === needle;
}
