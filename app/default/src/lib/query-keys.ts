export const queryKeys = {
  auth: {
    root: ["auth"] as const,
    session: () => ["auth", "session"] as const,
    info: () => ["auth", "info"] as const,
  },
  integrations: {
    root: ["integrations"] as const,
    list: () => ["integrations", "list"] as const,
  },
  tokens: {
    root: ["tokens"] as const,
    list: () => ["tokens", "list"] as const,
  },
  workflows: {
    root: ["workflows"] as const,
    list: () => ["workflows", "list"] as const,
    detail: (id: string) => ["workflows", "detail", id] as const,
  },
  agents: {
    root: ["agents"] as const,
    providers: () => ["agents", "providers"] as const,
    sessions: (opts?: { view?: string; limit?: number }) =>
      ["agents", "sessions", opts ?? {}] as const,
    session: (id: string, provider: string) =>
      ["agents", "session", id, provider] as const,
    turns: (sessionId: string, provider: string, status?: string) =>
      ["agents", "turns", sessionId, provider, status ?? "all"] as const,
    turn: (id: string, provider: string) =>
      ["agents", "turn", id, provider] as const,
    interactions: (turnId: string, provider: string) =>
      ["agents", "interactions", turnId, provider] as const,
  },
  managedIdentities: {
    root: ["managed-identities"] as const,
    list: () => ["managed-identities", "list"] as const,
    detail: (id: string) => ["managed-identities", "detail", id] as const,
    members: (id: string) => ["managed-identities", id, "members"] as const,
    grants: (id: string) => ["managed-identities", id, "grants"] as const,
    tokens: (id: string) => ["managed-identities", id, "tokens"] as const,
    integrations: (id: string) =>
      ["managed-identities", id, "integrations"] as const,
  },
  appAdmin: {
    root: ["app-admin"] as const,
    registry: (app: string) => ["app-admin", app, "registry"] as const,
    history: (app: string) => ["app-admin", app, "history"] as const,
  },
} as const;
