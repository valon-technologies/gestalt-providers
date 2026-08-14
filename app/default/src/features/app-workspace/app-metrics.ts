export function averageDurationMs(
  durationSecondsSum: number,
  durationSecondsCount: number,
): number | null {
  if (durationSecondsCount <= 0) return null;
  return (durationSecondsSum / durationSecondsCount) * 1000;
}

export function formatAverageDuration(
  durationSecondsSum: number,
  durationSecondsCount: number,
): string {
  const ms = averageDurationMs(durationSecondsSum, durationSecondsCount);
  if (ms === null) return "No data";
  if (ms < 10) return `${ms.toFixed(2)} ms`;
  if (ms < 1000) return `${ms.toFixed(1)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export function formatMetricCount(value: number): string {
  return Math.round(value).toLocaleString();
}

export function appMetricsIsEmpty(metrics: {
  requests: number;
  operations: unknown[];
}): boolean {
  return metrics.requests <= 0 && metrics.operations.length === 0;
}
