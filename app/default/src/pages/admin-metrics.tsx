import { RefreshCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  ADMIN_METRICS_EMPTY,
  ADMIN_METRICS_LAST_REFRESHED,
  ADMIN_METRICS_LOAD_ERROR,
  ADMIN_METRICS_LOADING,
  ADMIN_METRICS_PAGE_DESCRIPTION,
  ADMIN_METRICS_PAGE_TITLE,
  ADMIN_METRICS_REFRESH,
  ADMIN_METRICS_UNAVAILABLE,
} from "@/features/admin-access/admin-access-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { isAPIErrorStatus } from "@/lib/api";
import { useAdminMetricsQuery } from "@/lib/queries";

export default function AdminMetricsPage() {
  useDocumentTitle(ADMIN_METRICS_PAGE_TITLE);
  const metricsQuery = useAdminMetricsQuery();
  const unavailable = isAPIErrorStatus(metricsQuery.error, 503);
  const status = metricsQuery.isPending
    ? ADMIN_METRICS_LOADING
    : metricsQuery.isError
      ? unavailable
        ? ADMIN_METRICS_UNAVAILABLE
        : ADMIN_METRICS_LOAD_ERROR
      : metricsQuery.dataUpdatedAt
        ? ADMIN_METRICS_LAST_REFRESHED(
            new Date(metricsQuery.dataUpdatedAt).toLocaleTimeString(),
          )
        : ADMIN_METRICS_EMPTY;

  return (
    <div className="space-y-6">
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{ADMIN_METRICS_PAGE_TITLE}</PageHeaderTitle>
          <PageHeaderDescription>
            {ADMIN_METRICS_PAGE_DESCRIPTION}
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => void metricsQuery.refetch()}
            disabled={metricsQuery.isFetching}
          >
            <RefreshCcw
              className={
                metricsQuery.isFetching
                  ? "animate-spin motion-reduce:animate-none"
                  : undefined
              }
            />
            {ADMIN_METRICS_REFRESH}
          </Button>
        </PageHeaderActions>
      </PageHeader>
      <div className="overflow-hidden rounded-lg border border-border bg-card text-card-foreground">
        <div className="border-b border-border px-4 py-3">
          <span className="text-sm text-muted-foreground">{status}</span>
        </div>
        <pre className="max-h-[calc(100vh-220px)] min-h-[480px] overflow-auto whitespace-pre-wrap p-4 font-mono text-sm text-foreground">
          {metricsQuery.isError
            ? ""
            : metricsQuery.data ||
              (metricsQuery.isFetching ? "" : ADMIN_METRICS_EMPTY)}
        </pre>
      </div>
    </div>
  );
}
