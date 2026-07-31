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
  managedIdentities: {
    root: ["managed-identities"] as const,
    list: () => ["managed-identities", "list"] as const,
    detail: (id: string) => ["managed-identities", "detail", id] as const,
    members: (id: string) => ["managed-identities", id, "members"] as const,
    grants: (id: string) => ["managed-identities", id, "grants"] as const,
    integrations: (id: string) =>
      ["managed-identities", id, "integrations"] as const,
  },
  appAdmin: {
    root: ["app-admin"] as const,
    registry: (app: string) => ["app-admin", app, "registry"] as const,
    history: (app: string) => ["app-admin", app, "history"] as const,
    members: (app: string) => ["app-admin", app, "members"] as const,
  },
  workflows: {
    root: ["workflows"] as const,
    list: (app: string) => ["workflows", app, "list"] as const,
    detail: (app: string, runId: string) =>
      ["workflows", app, "detail", runId] as const,
  },
} as const;
