export const queryKeys = {
  auth: {
    root: ["auth"] as const,
    session: () => ["auth", "session"] as const,
    info: () => ["auth", "info"] as const,
  },
  integrations: {
    root: ["integrations"] as const,
    list: () => ["integrations", "list"] as const,
    operations: (appName: string) =>
      ["integrations", appName, "operations"] as const,
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
    list: (app: string) => ["workflows", app, "list"] as const,
    detail: (app: string, runId: string) =>
      ["workflows", app, "detail", runId] as const,
    stepLogs: (app: string, runId: string, jobId: string, stepId: string) =>
      ["workflows", app, "detail", runId, "jobs", jobId, "steps", stepId, "logs"] as const,
    definitions: (app: string) =>
      ["workflows", app, "definitions"] as const,
    definition: (app: string, definitionId: string) =>
      ["workflows", app, "definitions", definitionId] as const,
  },
} as const;
