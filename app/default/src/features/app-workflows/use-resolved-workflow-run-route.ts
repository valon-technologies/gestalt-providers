import { useEffect, useMemo } from "react";
import { useNavigate, useRouterState } from "@tanstack/react-router";
import {
  useWorkflowRunSummaries,
  useWorkflowRunsQuery,
} from "@/lib/queries";
import { userFacingError } from "@/lib/user-facing-error";
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
 * Public id to pass to GetRun. Prefer the run index / Temporal handle.
 * After discovery is exhausted, fall back to the route segment so non-Temporal
 * ids (and bookmarked full handles) still load instead of hanging on page 1.
 */
export function publicWorkflowRunIdForGetRun(opts: {
  routeRunId: string;
  resolvedId: string;
  listRunId?: string;
  discoveryExhausted: boolean;
}): string | null {
  if (decodeTemporalRunHandle(opts.resolvedId)) return opts.resolvedId;
  if (opts.listRunId) return opts.listRunId;
  if (decodeTemporalRunHandle(opts.routeRunId)) return opts.routeRunId;
  const trimmed = opts.routeRunId.trim();
  if (!trimmed || !opts.discoveryExhausted) return null;
  return trimmed;
}

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
  const needsDiscovery =
    Boolean(routeRunId.trim()) &&
    !decodeTemporalRunHandle(resolvedId) &&
    !listRun &&
    resolvedId === routeRunId;
  const discovery = useWorkflowRunsQuery(app, { enabled: needsDiscovery });
  useEffect(() => {
    if (!needsDiscovery) return;
    if (
      !discovery.hasNextPage ||
      discovery.isFetchingNextPage ||
      discovery.isPending
    ) {
      return;
    }
    void discovery.fetchNextPage();
  }, [
    discovery.fetchNextPage,
    discovery.hasNextPage,
    discovery.isFetchingNextPage,
    discovery.isPending,
    discovery.dataUpdatedAt,
    needsDiscovery,
  ]);
  const discoveryExhausted =
    !needsDiscovery ||
    (discovery.isFetched &&
      !discovery.hasNextPage &&
      !discovery.isFetchingNextPage &&
      !discovery.isPending);

  const publicRunId = useMemo(
    () =>
      publicWorkflowRunIdForGetRun({
        routeRunId,
        resolvedId,
        listRunId: listRun?.id,
        discoveryExhausted,
      }),
    [discoveryExhausted, listRun?.id, resolvedId, routeRunId],
  );

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
    discoveryPending:
      needsDiscovery &&
      (discovery.isPending ||
        discovery.isFetching ||
        discovery.isFetchingNextPage),
    discoveryError:
      needsDiscovery && discovery.error
        ? userFacingError(
            discovery.error,
            "Couldn't find this workflow run. Try again.",
          )
        : null,
    retryDiscovery: () => {
      void discovery.refetch();
    },
  };
}
