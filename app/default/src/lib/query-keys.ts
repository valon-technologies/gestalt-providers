export const queryKeys = {
  auth: {
    root: ["auth"] as const,
    session: () => ["auth", "session"] as const,
    info: () => ["auth", "info"] as const,
  },
  integrations: {
    root: ["integrations"] as const,
    directory: () => ["integrations", "directory"] as const,
    connections: () => ["integrations", "connections"] as const,
    operations: (appName: string) =>
      ["integrations", appName, "operations"] as const,
    access: (appName: string) => ["integrations", appName, "access"] as const,
  },
  tokens: {
    root: ["tokens"] as const,
    list: () => ["tokens", "list"] as const,
  },
  appAdmin: {
    root: ["app-admin"] as const,
    registry: (app: string) => ["app-admin", app, "registry"] as const,
    history: (app: string) => ["app-admin", app, "history"] as const,
    members: (app: string) => ["app-admin", app, "members"] as const,
    identities: (app: string) => ["app-admin", app, "identities"] as const,
    metrics: (app: string) => ["app-admin", app, "metrics"] as const,
  },
  admin: {
    root: ["admin"] as const,
    access: () => ["admin", "access"] as const,
    platformAdmins: () => ["admin", "platform-admins"] as const,
    metrics: () => ["admin", "metrics"] as const,
    versions: () => ["admin", "versions"] as const,
    version: (app: string) => ["admin", "versions", app] as const,
  },
  authorization: {
    root: ["authorization"] as const,
    resourceTypes: () => ["authorization", "resource-types"] as const,
  },
  workflows: {
    root: ["workflows"] as const,
    /** Prefix for all run-list queries (with or without status/definition). */
    list: (app: string) => ["workflows", app, "list"] as const,
    /** App-scoped run index populated by any ListRuns page (flat or grouped). */
    runSummaries: (app: string) =>
      ["workflows", app, "run-summaries"] as const,
    listPage: (
      app: string,
      status: string,
      definitionId = "all",
      pageSize: number | "default" = "default",
    ) => ["workflows", app, "list", status, definitionId, pageSize] as const,
    detail: (app: string, runId: string) =>
      ["workflows", app, "detail", runId] as const,
    events: (app: string, runId: string) =>
      ["workflows", app, "detail", runId, "events"] as const,
    output: (app: string, runId: string) =>
      ["workflows", app, "detail", runId, "output"] as const,
    stepLogs: (app: string, runId: string, jobId: string, stepId: string) =>
      ["workflows", app, "detail", runId, "jobs", jobId, "steps", stepId, "logs"] as const,
    definitions: (app: string) =>
      ["workflows", app, "definitions"] as const,
    definition: (app: string, definitionId: string) =>
      ["workflows", app, "definitions", definitionId] as const,
  },
} as const;
