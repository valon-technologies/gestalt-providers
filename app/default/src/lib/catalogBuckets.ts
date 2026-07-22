import type { Integration } from "@/lib/api";
import { connectionSetupBucket } from "@/lib/catalogFilters";

/**
 * Catalog browse taxonomy for /apps — ChatGPT Plugins–style layout
 * (Installed first, then discovery categories), with Valon-relevant buckets.
 *
 * No team/category on `/api/v1/apps` yet; this curated map is the interim SoT
 * until category lands on the manifest → API.
 */

export type CatalogBucketId =
  | "productivity"
  | "communication"
  | "developer-tools"
  | "business-operations"
  | "data-analytics"
  | "finance"
  | "people"
  | "customer"
  | "security"
  | "other";

export type CatalogBucket = {
  id: CatalogBucketId;
  label: string;
  description: string;
};

/** Discovery order after Installed — omit empty sections in the UI. */
export const CATALOG_BUCKETS: readonly CatalogBucket[] = [
  {
    id: "productivity",
    label: "Productivity",
    description: "Docs, tasks, design, and day-to-day work surfaces.",
  },
  {
    id: "communication",
    label: "Communication",
    description: "Chat, email, calendar, and meetings.",
  },
  {
    id: "developer-tools",
    label: "Developer tools",
    description: "Source control, CI, infra, flags, and on-call.",
  },
  {
    id: "business-operations",
    label: "Business & operations",
    description: "Valon workspace apps and internal ops tooling.",
  },
  {
    id: "data-analytics",
    label: "Data & analytics",
    description: "Warehouses, BI, pipelines, and reporting.",
  },
  {
    id: "finance",
    label: "Finance",
    description: "Spend, treasury, and money movement.",
  },
  {
    id: "people",
    label: "People",
    description: "Hiring, HR, learning, and talent.",
  },
  {
    id: "customer",
    label: "Customer",
    description: "Support, success, and go-to-market systems.",
  },
  {
    id: "security",
    label: "Security",
    description: "Compliance, trust, and access controls.",
  },
  {
    id: "other",
    label: "Other",
    description: "Everything else in this workspace.",
  },
] as const;

/**
 * Explicit app → discovery bucket. Prefer listing every known app so browse
 * order stays intentional.
 */
const APP_BUCKET: Readonly<Record<string, CatalogBucketId>> = {
  // Productivity
  confluence: "productivity",
  excalidraw: "productivity",
  figma: "productivity",
  google_docs: "productivity",
  google_drive: "productivity",
  google_sheets: "productivity",
  google_slides: "productivity",
  jira: "productivity",
  linear: "productivity",
  notion: "productivity",
  sharepoint: "productivity",

  // Communication
  gmail: "communication",
  google_calendar: "communication",
  granola: "communication",
  slack: "communication",
  slack_v2: "communication",
  teams: "communication",

  // Developer tools
  aiSpendTracker: "developer-tools",
  ciCd: "developer-tools",
  ciWorkqueue: "developer-tools",
  datadog: "developer-tools",
  delta: "developer-tools",
  deploymentViewer: "developer-tools",
  deployos: "developer-tools",
  entityDiff: "developer-tools",
  gcp_batch: "developer-tools",
  github: "developer-tools",
  gitlab: "developer-tools",
  incident_io: "developer-tools",
  launchdarkly: "developer-tools",
  modelProviderBillingMetrics: "developer-tools",
  oncall: "developer-tools",
  pagerduty: "developer-tools",
  tokenPile: "developer-tools",
  trafficCop: "developer-tools",
  trunk: "developer-tools",
  valkey: "developer-tools",
  vercel: "developer-tools",

  // Business & operations (Valon internal + ops)
  gIssues: "business-operations",
  glinks: "business-operations",
  helloWorld: "business-operations",
  jarvis: "business-operations",
  paConfigurationRegistry: "business-operations",
  registry: "business-operations",
  valonProfile: "business-operations",
  vmStyleGuide: "business-operations",
  workplaceHub: "business-operations",

  // Data & analytics
  bigquery: "data-analytics",
  clickhouse: "data-analytics",
  copilotReports: "data-analytics",
  dataSchemaExplorer: "data-analytics",
  dbt_cloud: "data-analytics",
  gcs: "data-analytics",
  hex: "data-analytics",
  loanPopulationDashboard: "data-analytics",
  looker: "data-analytics",
  planetscale: "data-analytics",
  sdtPipeline: "data-analytics",
  standardReporting: "data-analytics",
  vdsForge: "data-analytics",

  // Finance
  modern_treasury: "finance",
  ramp: "finance",

  // People
  ashby: "people",
  gong: "people",
  itAccountOnboarding: "people",
  rippling: "people",
  talentTeam: "people",
  trainingCurriculum: "people",
  valonLearn: "people",
  valonSats: "people",

  // Customer
  customerRoadmapReview: "customer",
  dealHub: "customer",
  frontPorch: "customer",
  frontPorchRestApi: "customer",
  intercom: "customer",
  nice_incontact: "customer",
  zendesk: "customer",

  // Security
  vanta: "security",

  // Other
  extend: "other",
};

export function catalogBucketIdFor(
  integration: Integration,
): CatalogBucketId {
  const mapped = APP_BUCKET[integration.name];
  if (mapped) return mapped;
  // Mounted product UI without an explicit row → business & operations.
  if (integration.mountedPath?.trim()) return "business-operations";
  return "other";
}

/** Connected / ready apps — shown in the Installed section first. */
export function isCatalogInstalled(integration: Integration): boolean {
  return connectionSetupBucket(integration) === "ready";
}

export type CatalogBucketSection = {
  bucket: CatalogBucket;
  integrations: Integration[];
};

export type CatalogBrowseLayout = {
  installed: Integration[];
  sections: CatalogBucketSection[];
};

/**
 * Partition a pre-filtered/sorted list into Installed + discovery sections.
 * Installed apps are omitted from category grids so they aren’t shown twice.
 */
export function groupCatalogForBrowse(
  integrations: Integration[],
): CatalogBrowseLayout {
  const installed: Integration[] = [];
  const discovery: Integration[] = [];
  for (const integration of integrations) {
    if (isCatalogInstalled(integration)) {
      installed.push(integration);
    } else {
      discovery.push(integration);
    }
  }

  const byId = new Map<CatalogBucketId, Integration[]>();
  for (const bucket of CATALOG_BUCKETS) {
    byId.set(bucket.id, []);
  }
  for (const integration of discovery) {
    byId.get(catalogBucketIdFor(integration))!.push(integration);
  }

  const sections = CATALOG_BUCKETS.flatMap((bucket) => {
    const apps = byId.get(bucket.id) ?? [];
    if (apps.length === 0) return [];
    return [{ bucket, integrations: apps }];
  });

  return { installed, sections };
}

/** @deprecated Prefer groupCatalogForBrowse — kept for call sites mid-migration. */
export function groupCatalogByBucket(
  integrations: Integration[],
): CatalogBucketSection[] {
  return groupCatalogForBrowse(integrations).sections;
}
