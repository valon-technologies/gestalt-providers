/** True when a 200 body is a metrics scrape, not an HTML fallback. */
export function isAdminMetricsScrapeText(
  contentType: string,
  body: string,
): boolean {
  if (/\btext\/html\b/i.test(contentType)) return false;
  const trimmed = body.trimStart();
  return !/^<!doctype html/i.test(trimmed) && !/^<html[\s>]/i.test(trimmed);
}
