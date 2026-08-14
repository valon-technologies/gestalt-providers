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
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { SpinnerIcon } from "@/components/icons";
import {
  Stat,
  StatGroup,
  StatLabel,
  StatValue,
} from "@/components/ui/stat";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";
import {
  appMetricsIsEmpty,
  formatAverageDuration,
  formatMetricCount,
} from "@/features/app-workspace/app-metrics";
import {
  APP_METRICS_EMPTY,
  APP_METRICS_ERRORS_LABEL,
  APP_METRICS_FORBIDDEN,
  APP_METRICS_LATENCY_LABEL,
  APP_METRICS_LOAD_ERROR,
  APP_METRICS_OPERATION_COLUMN,
  APP_METRICS_OPERATIONS_TITLE,
  APP_METRICS_PAGE_DESCRIPTION,
  APP_METRICS_PAGE_TITLE,
  APP_METRICS_REFRESH,
  APP_METRICS_REQUESTS_LABEL,
  APP_METRICS_UNAVAILABLE,
} from "@/features/app-workspace/app-metrics-copy";
import { isAPIErrorStatus } from "@/lib/api";
import { useAppAdminMetricsQuery } from "@/lib/queries";

export default function AppAdminMetricsPage() {
  const { app } = useAppWorkspace();
  const metricsQuery = useAppAdminMetricsQuery(app);
  const forbidden =
    metricsQuery.isError && isAPIErrorStatus(metricsQuery.error, 403);
  const unavailable =
    metricsQuery.isError && isAPIErrorStatus(metricsQuery.error, 503);
  const loadError =
    metricsQuery.isError && !forbidden && !unavailable
      ? APP_METRICS_LOAD_ERROR
      : null;
  const metrics = metricsQuery.data;
  const empty = Boolean(metrics && appMetricsIsEmpty(metrics));

  return (
    <section aria-label={APP_METRICS_PAGE_TITLE}>
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{APP_METRICS_PAGE_TITLE}</PageHeaderTitle>
          <PageHeaderDescription>
            {APP_METRICS_PAGE_DESCRIPTION}
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
            {APP_METRICS_REFRESH}
          </Button>
        </PageHeaderActions>
      </PageHeader>

      {metricsQuery.isPending ? (
        <p className="mt-5 flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading metrics…
        </p>
      ) : null}

      {forbidden ? (
        <p className="mt-5 text-sm text-muted-foreground">{APP_METRICS_FORBIDDEN}</p>
      ) : null}

      {unavailable ? (
        <p className="mt-5 text-sm text-muted-foreground">{APP_METRICS_UNAVAILABLE}</p>
      ) : null}

      {loadError ? (
        <p className="mt-5 text-sm text-destructive">{loadError}</p>
      ) : null}

      {metrics && !forbidden && !unavailable ? (
        empty ? (
          <p className="mt-5 text-sm text-muted-foreground">{APP_METRICS_EMPTY}</p>
        ) : (
          <div className="mt-6 space-y-8">
            <StatGroup>
              <Stat variant="plain">
                <StatLabel>{APP_METRICS_REQUESTS_LABEL}</StatLabel>
                <StatValue>{formatMetricCount(metrics.requests)}</StatValue>
              </Stat>
              <Stat variant="plain">
                <StatLabel>{APP_METRICS_ERRORS_LABEL}</StatLabel>
                <StatValue>{formatMetricCount(metrics.errors)}</StatValue>
              </Stat>
              <Stat variant="plain">
                <StatLabel>{APP_METRICS_LATENCY_LABEL}</StatLabel>
                <StatValue>
                  {formatAverageDuration(
                    metrics.durationSecondsSum,
                    metrics.durationSecondsCount,
                  )}
                </StatValue>
              </Stat>
            </StatGroup>

            <section aria-labelledby="app-metrics-operations">
              <SectionHeader>
                <SectionHeaderContent>
                  <SectionHeaderTitle id="app-metrics-operations">
                    {APP_METRICS_OPERATIONS_TITLE}
                  </SectionHeaderTitle>
                </SectionHeaderContent>
              </SectionHeader>
              {metrics.operations.length === 0 ? (
                <p className="mt-3 text-sm text-muted-foreground">
                  {APP_METRICS_EMPTY}
                </p>
              ) : (
                <div className="mt-3 overflow-hidden rounded-lg border border-border bg-card">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>{APP_METRICS_OPERATION_COLUMN}</TableHead>
                        <TableHead align="end">{APP_METRICS_REQUESTS_LABEL}</TableHead>
                        <TableHead align="end">{APP_METRICS_ERRORS_LABEL}</TableHead>
                        <TableHead align="end">{APP_METRICS_LATENCY_LABEL}</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {metrics.operations.map((row) => (
                        <TableRow key={row.operation}>
                          <TableCell>
                            <span className="font-mono text-sm">{row.operation}</span>
                          </TableCell>
                          <TableCell numeric align="end">
                            {formatMetricCount(row.requests)}
                          </TableCell>
                          <TableCell numeric align="end">
                            {formatMetricCount(row.errors)}
                          </TableCell>
                          <TableCell numeric align="end">
                            {formatAverageDuration(
                              row.durationSecondsSum,
                              row.durationSecondsCount,
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </section>
          </div>
        )
      ) : null}
    </section>
  );
}
