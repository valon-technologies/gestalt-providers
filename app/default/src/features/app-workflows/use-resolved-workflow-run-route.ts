import { useEffect, useMemo } from "react";
import { useNavigate, useRouterState } from "@tanstack/react-router";
import {
  useWorkflowRunSummaries,
  useWorkflowRunsQuery,
} from "@/lib/queries";
import {
  decodeTemporalRunHandle,
  rememberWorkflowRunPublicId,
  resolveWorkflowRunPublicId,
  workflowRunPathId,
} from "@/features/app-workflows/workflow-format";

/**
 * Rewrite a legacy full-handle run URL to the short path form, keeping any
 * job/step suffix. Returns null when the pathname should stay as-is.
 */
export function rewriteShortWorkflowRunPath(opts: {
  pathname: string;
  app: string;
  routeRunId: string;
  publicRunId: string;
}): string | null {
  const short = workflowRunPathId(opts.publicRunId);
  if (!short || short === opts.routeRunId) return null;
  if (
    !(
      opts.routeRunId === opts.publicRunId ||
      Boolean(decodeTemporalRunHandle(opts.routeRunId))
    )
  ) {
    return null;
  }
  const marker = `/apps/${opts.app}/admin/workflows/runs/`;
  const idx = opts.pathname.indexOf(marker);
  if (idx < 0) return null;
  const rest = opts.pathname.slice(idx + marker.length);
  const slash = rest.indexOf("/");
  const currentSeg = slash >= 0 ? rest.slice(0, slash) : rest;
  const after = slash >= 0 ? rest.slice(slash) : "";
  let decoded = currentSeg;
  try {
    decoded = decodeURIComponent(currentSeg);
  } catch {
    // keep raw segment
  }
  if (decoded !== opts.routeRunId && currentSeg !== opts.routeRunId) {
    return null;
  }
  const next = `${marker}${encodeURIComponent(short)}${after}`;
  return next !== opts.pathname ? next : null;
}

/**
 * Resolve a route `$runId` (short or full handle) to the public API id, using
 * the shared run index (any ListRuns page) plus session memory. GetRun still
 * needs the full handle.
 */
export function useResolvedWorkflowRunRoute(app: string, routeRunId: string) {
  const navigate = useNavigate();
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const knownRuns = useWorkflowRunSummaries(app);

  const resolvedId = useMemo(
    () => resolveWorkflowRunPublicId(app, routeRunId, knownRuns),
    [app, knownRuns, routeRunId],
  );
  const needsDiscovery =
    Boolean(routeRunId.trim()) &&
    !decodeTemporalRunHandle(resolvedId) &&
    resolvedId === routeRunId;
  useWorkflowRunsQuery(app, { enabled: needsDiscovery });

  const listRun = useMemo(
    () =>
      knownRuns.find(
        (run) =>
          run.id === resolvedId ||
          run.id === routeRunId ||
          workflowRunPathId(run.id) === routeRunId,
      ),
    [knownRuns, resolvedId, routeRunId],
  );

  const publicRunId = useMemo(() => {
    if (decodeTemporalRunHandle(resolvedId)) return resolvedId;
    if (listRun?.id) return listRun.id;
    return null;
  }, [listRun?.id, resolvedId]);

  useEffect(() => {
    if (listRun?.id) rememberWorkflowRunPublicId(app, listRun.id);
  }, [app, listRun?.id]);

  useEffect(() => {
    if (!publicRunId) return;
    rememberWorkflowRunPublicId(app, publicRunId);
    const next = rewriteShortWorkflowRunPath({
      pathname,
      app,
      routeRunId,
      publicRunId,
    });
    if (next) void navigate({ to: next, replace: true });
  }, [app, navigate, pathname, publicRunId, routeRunId]);

  return {
    publicRunId,
    listRun: listRun ?? undefined,
    pathRunId: publicRunId ? workflowRunPathId(publicRunId) : routeRunId,
  };
}
