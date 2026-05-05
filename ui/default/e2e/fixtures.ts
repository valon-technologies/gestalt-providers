import { test as base, expect, type Page, type Route } from "@playwright/test";
import type {
  AgentInteraction,
  AgentProvider,
  AgentRun,
  AgentSession,
  AgentTurn,
  AgentTurnEvent,
  APIToken,
  Integration,
  IntegrationOperation,
  ManagedIdentity,
  WorkflowEventTrigger,
  WorkflowRun,
  WorkflowSchedule,
} from "../src/lib/api";

type MockAgentRunsOptions = {
  onCreate?: (
    body: Partial<AgentRun> & Record<string, unknown>,
  ) => AgentRun | { status: number; json: unknown };
  onCancel?: (
    run: AgentRun,
    body: { reason?: string } | null,
  ) => { status: number; json: unknown } | undefined;
};

type MockAgentRunsController = {
  setRuns: (runs: AgentRun[]) => void;
  getRuns: () => AgentRun[];
};

type MockAgentSessionsData = {
  providers?: AgentProvider[];
  sessions: AgentSession[];
  turns: Record<string, AgentTurn[]>;
  events?: Record<string, AgentTurnEvent[]>;
  interactions?: Record<string, AgentInteraction[]>;
};

type MockAgentSessionsOptions = {
  onCreateSession?: (
    body: Record<string, unknown>,
  ) => AgentSession | { status: number; json: unknown };
  onCreateTurn?: (
    session: AgentSession,
    body: Record<string, unknown>,
  ) => AgentTurn | { status: number; json: unknown };
  onCancelTurn?: (
    turn: AgentTurn,
    body: { reason?: string } | null,
  ) => { status: number; json: unknown } | undefined;
  onResolveInteraction?: (
    interaction: AgentInteraction,
    resolution: Record<string, unknown>,
  ) => { status: number; json: unknown } | undefined;
  streamErrorByTurn?: Record<string, unknown>;
};

type MockAgentSessionsController = {
  setSessions: (sessions: AgentSession[]) => void;
  setTurns: (sessionID: string, turns: AgentTurn[]) => void;
  setEvents: (turnID: string, events: AgentTurnEvent[]) => void;
  setInteractions: (turnID: string, interactions: AgentInteraction[]) => void;
  getSessions: () => AgentSession[];
  getTurns: (sessionID: string) => AgentTurn[];
};

type MockWorkflowRunsOptions = {
  onCancel?: (
    run: WorkflowRun,
    body: { reason?: string } | null,
  ) => { status: number; json: unknown } | undefined;
};

type MockWorkflowRunsController = {
  setRuns: (runs: WorkflowRun[]) => void;
  getRuns: () => WorkflowRun[];
};

type MockWorkflowSchedulesOptions = {
  onCreate?: (
    body: Partial<WorkflowSchedule> & Record<string, unknown>,
  ) => WorkflowSchedule | { status: number; json: unknown };
  onUpdate?: (
    current: WorkflowSchedule,
    body: Partial<WorkflowSchedule> & Record<string, unknown>,
  ) => WorkflowSchedule | { status: number; json: unknown };
};

type MockWorkflowSchedulesController = {
  setSchedules: (schedules: WorkflowSchedule[]) => void;
  getSchedules: () => WorkflowSchedule[];
};

type MockWorkflowEventTriggersOptions = {
  onCreate?: (
    body: Partial<WorkflowEventTrigger> & Record<string, unknown>,
  ) => WorkflowEventTrigger | { status: number; json: unknown };
  onUpdate?: (
    current: WorkflowEventTrigger,
    body: Partial<WorkflowEventTrigger> & Record<string, unknown>,
  ) => WorkflowEventTrigger | { status: number; json: unknown };
};

type MockWorkflowEventTriggersController = {
  setTriggers: (triggers: WorkflowEventTrigger[]) => void;
  getTriggers: () => WorkflowEventTrigger[];
};

export async function mockIntegrations(
  page: Page,
  integrations: Integration[],
  opts?: { onDisconnect?: (name: string, url: URL) => void },
) {
  await page.route("**/api/v1/integrations", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: integrations });
    } else {
      route.fallback();
    }
  });

  await page.route("**/api/v1/integrations/*", (route: Route, request) => {
    if (request.method() === "DELETE") {
      const url = new URL(request.url());
      const name = url.pathname.split("/").pop() || "";
      opts?.onDisconnect?.(name, url);
      route.fulfill({ json: { status: "disconnected" } });
    } else {
      route.fallback();
    }
  });
}

export async function mockIntegrationOperations(
  page: Page,
  operationsByIntegrationName: Record<string, IntegrationOperation[]>,
) {
  await page.route("**/api/v1/integrations/*/operations", async (route: Route, request) => {
    if (request.method() !== "GET") {
      await route.fallback();
      return;
    }

    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const integration = parts[parts.length - 2] || "";
    await route.fulfill({ json: operationsByIntegrationName[integration] || [] });
  });
}

export async function mockManualConnect(
  page: Page,
  opts?: { onConnect?: (integration: string, credential: string) => void },
) {
  await page.route(
    "**/api/v1/auth/connect-manual",
    async (route: Route, request) => {
      if (request.method() === "POST") {
        const body = request.postDataJSON() as {
          integration: string;
          credential: string;
          returnPath?: string;
        };
        opts?.onConnect?.(body.integration, body.credential);
        await route.fulfill({ json: { status: "connected" } });
      } else {
        await route.fallback();
      }
    },
  );
}

export async function mockManagedIdentities(
  page: Page,
  identities: ManagedIdentity[],
) {
  await page.route("**/api/v1/authorization/subjects", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: identities });
    } else {
      route.fallback();
    }
  });
}

export async function mockAuthInfo(
  page: Page,
  info: {
    provider: string;
    displayName: string;
    loginSupported?: boolean;
    features?: { agent?: boolean };
  },
) {
  await page.route("**/api/v1/auth/info", (route: Route) => {
    route.fulfill({ json: { loginSupported: true, ...info } });
  });
}

export async function mockTokens(page: Page, tokens: APIToken[]) {
  await page.route("**/api/v1/tokens", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: tokens });
    } else {
      route.fallback();
    }
  });
}

export async function mockAgentSessions(
  page: Page,
  data: MockAgentSessionsData,
  opts?: MockAgentSessionsOptions,
): Promise<MockAgentSessionsController> {
  let currentSessions = data.sessions.map((session) => structuredClone(session));
  let turnsBySession = cloneRecordArray(data.turns);
  let eventsByTurn = cloneRecordArray(data.events ?? {});
  let interactionsByTurn = cloneRecordArray(data.interactions ?? {});
  const providers =
    data.providers ??
    ([
      {
        name: "simple",
        default: true,
        capabilities: {
          streamingText: true,
          toolCalls: true,
          interactions: true,
          supportedToolSources: ["mcp_catalog"],
        },
      },
    ] satisfies AgentProvider[]);

  function sessionsSorted() {
    return currentSessions.slice().sort((left, right) => {
      const leftTime = Date.parse(left.lastTurnAt || left.updatedAt || left.createdAt || "");
      const rightTime = Date.parse(right.lastTurnAt || right.updatedAt || right.createdAt || "");
      return (Number.isNaN(rightTime) ? 0 : rightTime) - (Number.isNaN(leftTime) ? 0 : leftTime);
    });
  }

  function sessionByID(id: string) {
    return currentSessions.find((session) => session.id === id);
  }

  function turnByID(id: string) {
    for (const turns of Object.values(turnsBySession)) {
      const found = turns.find((turn) => turn.id === id);
      if (found) return found;
    }
    return undefined;
  }

  await page.route("**/api/v1/agent/providers", async (route: Route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({ json: { providers } });
      return;
    }
    await route.fallback();
  });

  await page.route(/\/api\/v1\/agent\/sessions(?:\?.*)?$/, async (route: Route, request) => {
    if (request.method() === "GET") {
      const url = new URL(request.url());
      const provider = url.searchParams.get("provider") || "";
      const state = url.searchParams.get("state") || "";
      const filtered = sessionsSorted().filter((session) => {
        if (provider && session.provider !== provider) return false;
        if (state && session.state !== state) return false;
        return true;
      });
      await route.fulfill({ json: filtered });
      return;
    }

    if (request.method() !== "POST") {
      await route.fallback();
      return;
    }

    const body = (request.postDataJSON() as Record<string, unknown>) ?? {};
    const created =
      opts?.onCreateSession?.(body) ??
      ({
        id: `agent_session_${currentSessions.length + 1}`,
        provider: typeof body.provider === "string" && body.provider ? body.provider : "simple",
        model: typeof body.model === "string" ? body.model : undefined,
        clientRef: typeof body.clientRef === "string" ? body.clientRef : undefined,
        state: "active",
        createdAt: "2026-04-23T00:00:00Z",
        updatedAt: "2026-04-23T00:00:00Z",
      } satisfies AgentSession);

    if ("status" in created) {
      await route.fulfill({ status: created.status, json: created.json });
      return;
    }

    currentSessions = [structuredClone(created), ...currentSessions];
    turnsBySession[created.id] = turnsBySession[created.id] ?? [];
    await route.fulfill({ status: 201, json: created });
  });

  await page.route(
    /\/api\/v1\/agent\/sessions\/[^/?]+(?:\/turns)?(?:\?.*)?$/,
    async (route: Route, request) => {
      const url = new URL(request.url());
      const parts = url.pathname.split("/");
      const sessionIndex = parts.indexOf("sessions");
      const sessionID = sessionIndex >= 0 ? parts[sessionIndex + 1] : "";
      const session = sessionByID(sessionID);
      if (!session) {
        await route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }

      if (parts[sessionIndex + 2] !== "turns") {
        if (request.method() === "GET") {
          await route.fulfill({ json: session });
          return;
        }
        if (request.method() === "PATCH") {
          const body = (request.postDataJSON() as Partial<AgentSession>) ?? {};
          Object.assign(session, body, { updatedAt: "2026-04-23T01:00:00Z" });
          await route.fulfill({ json: session });
          return;
        }
        await route.fallback();
        return;
      }

      if (request.method() === "GET") {
        const status = url.searchParams.get("status") || "";
        const limit = Number(url.searchParams.get("limit") || "0");
        let turns = (turnsBySession[sessionID] ?? []).filter((turn) => {
          if (status && turn.status !== status) return false;
          return true;
        });
        if (limit > 0) {
          turns = turns.slice(0, limit);
        }
        await route.fulfill({ json: turns });
        return;
      }

      if (request.method() !== "POST") {
        await route.fallback();
        return;
      }

      const body = (request.postDataJSON() as Record<string, unknown>) ?? {};
      const created =
        opts?.onCreateTurn?.(structuredClone(session), body) ??
        ({
          id: `agent_turn_${Object.values(turnsBySession).flat().length + 1}`,
          sessionId: session.id,
          provider: session.provider,
          model: typeof body.model === "string" ? body.model : session.model,
          status: "running",
          messages: Array.isArray(body.messages)
            ? (body.messages as AgentTurn["messages"])
            : [],
          createdAt: "2026-04-23T00:00:00Z",
          startedAt: "2026-04-23T00:00:00Z",
          executionRef: `agent_turn_${Object.values(turnsBySession).flat().length + 1}`,
        } satisfies AgentTurn);

      if (typeof (created as { status?: unknown }).status === "number") {
        const override = created as { status: number; json: unknown };
        await route.fulfill({ status: override.status, json: override.json });
        return;
      }

      turnsBySession[sessionID] = [structuredClone(created), ...(turnsBySession[sessionID] ?? [])];
      session.lastTurnAt = created.createdAt;
      await route.fulfill({ status: 201, json: created });
    },
  );

  await page.route(/\/api\/v1\/agent\/turns\/[^/?]+(?:\/.*)?(?:\?.*)?$/, async (route: Route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const turnIndex = parts.indexOf("turns");
    const turnID = turnIndex >= 0 ? parts[turnIndex + 1] : "";
    const tail = parts.slice(turnIndex + 2);
    const turn = turnByID(turnID);
    if (!turn) {
      await route.fulfill({ status: 404, json: { error: "not found" } });
      return;
    }

    if (tail.length === 0) {
      if (request.method() === "GET") {
        await route.fulfill({ json: turn });
        return;
      }
      await route.fallback();
      return;
    }

    if (tail[0] === "cancel") {
      if (request.method() !== "POST") {
        await route.fallback();
        return;
      }
      const body = (request.postDataJSON() as { reason?: string } | null) ?? null;
      const override = opts?.onCancelTurn?.(structuredClone(turn), body);
      if (override) {
        await route.fulfill({ status: override.status, json: override.json });
        return;
      }
      if (!["pending", "running", "waiting_for_input"].includes(turn.status || "")) {
        await route.fulfill({ status: 412, json: { error: "agent turn is no longer active" } });
        return;
      }
      turn.status = "canceled";
      turn.statusMessage = body?.reason;
      turn.completedAt = "2026-04-23T00:05:00Z";
      await route.fulfill({ json: turn });
      return;
    }

    if (tail[0] === "events" && tail[1] === "stream") {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      const after = Number(url.searchParams.get("after") || "0");
      const limit = Number(url.searchParams.get("limit") || "100");
      const pageEvents = (eventsByTurn[turnID] ?? [])
        .filter((event) => event.seq > after)
        .slice(0, limit);
      const frames = [": heartbeat\n\n"];
      for (const event of pageEvents) {
        frames.push(`data: ${JSON.stringify(event)}\n\n`);
      }
      if (opts?.streamErrorByTurn?.[turnID]) {
        frames.push(`event: error\ndata: ${JSON.stringify(opts.streamErrorByTurn[turnID])}\n\n`);
      }
      await route.fulfill({
        status: 200,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
        },
        body: frames.join(""),
      });
      return;
    }

    if (tail[0] === "events") {
      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }
      const after = Number(url.searchParams.get("after") || "0");
      const limit = Number(url.searchParams.get("limit") || "100");
      const pageEvents = (eventsByTurn[turnID] ?? [])
        .filter((event) => event.seq > after)
        .slice(0, limit);
      await route.fulfill({ json: pageEvents });
      return;
    }

    if (tail[0] === "interactions" && tail.length === 1) {
      if (request.method() === "GET") {
        await route.fulfill({ json: interactionsByTurn[turnID] ?? [] });
        return;
      }
      await route.fallback();
      return;
    }

    if (tail[0] === "interactions" && tail[2] === "resolve") {
      if (request.method() !== "POST") {
        await route.fallback();
        return;
      }
      const interactionID = tail[1];
      const interaction = (interactionsByTurn[turnID] ?? []).find((item) => item.id === interactionID);
      if (!interaction) {
        await route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }
      const body = (request.postDataJSON() as { resolution?: Record<string, unknown> } | null) ?? null;
      const resolution = body?.resolution ?? {};
      const override = opts?.onResolveInteraction?.(structuredClone(interaction), resolution);
      if (override) {
        await route.fulfill({ status: override.status, json: override.json });
        return;
      }
      interaction.state = "resolved";
      interaction.resolution = resolution;
      interaction.resolvedAt = "2026-04-23T00:06:00Z";
      await route.fulfill({ json: interaction });
      return;
    }

    await route.fallback();
  });

  return {
    setSessions(nextSessions) {
      currentSessions = nextSessions.map((session) => structuredClone(session));
    },
    setTurns(sessionID, turns) {
      turnsBySession[sessionID] = turns.map((turn) => structuredClone(turn));
    },
    setEvents(turnID, events) {
      eventsByTurn[turnID] = events.map((event) => structuredClone(event));
    },
    setInteractions(turnID, interactions) {
      interactionsByTurn[turnID] = interactions.map((interaction) => structuredClone(interaction));
    },
    getSessions() {
      return currentSessions.map((session) => structuredClone(session));
    },
    getTurns(sessionID) {
      return (turnsBySession[sessionID] ?? []).map((turn) => structuredClone(turn));
    },
  };
}

export async function mockAgentRuns(
  page: Page,
  runs: AgentRun[],
  opts?: MockAgentRunsOptions,
): Promise<MockAgentRunsController> {
  let currentRuns = runs.map((run) => structuredClone(run));
  const createdSessions = new Map<
    string,
    {
      id: string;
      provider: string;
      model?: string;
      clientRef?: string;
      state: string;
      createdAt: string;
      updatedAt: string;
      lastTurnAt?: string;
    }
  >();
  const sessionIDsByRunID = new Map<string, string>();

  await page.route("**/api/v1/agent/providers", async (route: Route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({
        json: {
          providers: [
            {
              name: "simple",
              default: true,
              capabilities: {
                streamingText: true,
                toolCalls: true,
                interactions: true,
                supportedToolSources: ["mcp_catalog"],
              },
            },
          ],
        },
      });
      return;
    }
    await route.fallback();
  });

  function runSessionID(run: AgentRun): string {
    if (run.sessionId) return run.sessionId;
    if (run.sessionRef) return run.sessionRef;
    const existing = sessionIDsByRunID.get(run.id);
    if (existing) return existing;
    const next = `agent_session_${sessionIDsByRunID.size + 1}`;
    sessionIDsByRunID.set(run.id, next);
    return next;
  }

  function runWithSession(run: AgentRun): AgentRun {
    const sessionId = runSessionID(run);
    return {
      ...run,
      sessionId,
      sessionRef: run.sessionRef || sessionId,
    };
  }

  function sessionsForRuns() {
    const sessions = new Map(createdSessions);
    currentRuns.forEach((run) => {
      const value = runWithSession(run);
      const sessionID = value.sessionId || runSessionID(value);
      const existing = sessions.get(sessionID);
      sessions.set(sessionID, {
        id: sessionID,
        provider: value.provider,
        model: value.model,
        clientRef: value.sessionRef,
        state: existing?.state || "active",
        createdAt: existing?.createdAt || value.createdAt || "2026-04-23T00:00:00Z",
        updatedAt: existing?.updatedAt || value.createdAt || "2026-04-23T00:00:00Z",
        lastTurnAt: value.createdAt || existing?.lastTurnAt,
      });
    });
    return [...sessions.values()].sort((left, right) => {
      const leftTime = Date.parse(left.lastTurnAt || left.createdAt || "");
      const rightTime = Date.parse(right.lastTurnAt || right.createdAt || "");
      return (Number.isNaN(rightTime) ? 0 : rightTime) - (Number.isNaN(leftTime) ? 0 : leftTime);
    });
  }

  function sessionByID(id: string) {
    return sessionsForRuns().find((session) => session.id === id);
  }

  await page.route(/\/api\/v1\/agent\/sessions(?:\?.*)?$/, async (route: Route, request) => {
    if (request.method() === "GET") {
      const url = new URL(request.url());
      const provider = url.searchParams.get("provider") || "";
      const filtered = sessionsForRuns().filter((session) => {
        if (provider && session.provider !== provider) return false;
        return true;
      });
      await route.fulfill({ json: filtered });
      return;
    }

    if (request.method() !== "POST") {
      await route.fallback();
      return;
    }

    const body = (request.postDataJSON() as Record<string, unknown>) ?? {};
    const id = `agent_session_${createdSessions.size + currentRuns.length + 1}`;
    const session = {
      id,
      provider: typeof body.provider === "string" ? body.provider : "simple",
      model: typeof body.model === "string" ? body.model : undefined,
      clientRef: typeof body.clientRef === "string" ? body.clientRef : undefined,
      state: "active",
      createdAt: "2026-04-23T00:00:00Z",
      updatedAt: "2026-04-23T00:00:00Z",
    };
    createdSessions.set(id, session);
    await route.fulfill({ status: 201, json: session });
  });

  await page.route(
    /\/api\/v1\/agent\/sessions\/[^/?]+(?:\/turns)?(?:\?.*)?$/,
    async (route: Route, request) => {
      const url = new URL(request.url());
      const parts = url.pathname.split("/");
      const sessionIndex = parts.indexOf("sessions");
      const sessionID = sessionIndex >= 0 ? parts[sessionIndex + 1] : "";
      const session = sessionByID(sessionID);
      if (!session) {
        await route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }

      if (parts[sessionIndex + 2] !== "turns") {
        if (request.method() === "GET") {
          await route.fulfill({ json: session });
          return;
        }
        await route.fallback();
        return;
      }

      if (request.method() === "GET") {
        const status = url.searchParams.get("status") || "";
        const filtered = currentRuns
          .map((run) => runWithSession(run))
          .filter((run) => {
            if (run.sessionId !== sessionID) return false;
            if (status && run.status !== status) return false;
            return true;
          });
        await route.fulfill({ json: filtered });
        return;
      }

      if (request.method() !== "POST") {
        await route.fallback();
        return;
      }

      const body = (request.postDataJSON() as Partial<AgentRun> & Record<string, unknown>) ?? {};
      const createBody = {
        ...body,
        provider: session.provider,
        model: typeof body.model === "string" ? body.model : session.model,
        sessionId: session.id,
        sessionRef: session.clientRef || session.id,
      };
      const created =
        opts?.onCreate?.(createBody) ??
        ({
          id: `agent_run_${currentRuns.length + 1}`,
          sessionId: session.id,
          sessionRef: session.clientRef || session.id,
          provider: session.provider,
          model: typeof body.model === "string" ? body.model : session.model || "fast",
          status: "running",
          messages: Array.isArray(body.messages)
            ? (body.messages as AgentRun["messages"])
            : [],
          createdAt: "2026-04-23T00:00:00Z",
          startedAt: "2026-04-23T00:00:00Z",
          executionRef: `agent_run_${currentRuns.length + 1}`,
        } satisfies AgentRun);

      if (typeof (created as { status?: unknown }).status === "number") {
        const override = created as { status: number; json: unknown };
        await route.fulfill({ status: override.status, json: override.json });
        return;
      }

      const normalized = runWithSession({
        ...created,
        sessionId: created.sessionId || session.id,
        sessionRef: created.sessionRef || session.clientRef || session.id,
      });
      currentRuns = [structuredClone(normalized), ...currentRuns];
      await route.fulfill({ status: 201, json: normalized });
    },
  );

  await page.route(
    /\/api\/v1\/agent\/turns\/[^/?]+(?:\/cancel)?(?:\?.*)?$/,
    async (route: Route, request) => {
      const url = new URL(request.url());
      const parts = url.pathname.split("/");
      const id = parts[parts.length - 2] === "turns"
        ? parts[parts.length - 1]
        : parts[parts.length - 2];

      if (request.method() === "POST" && parts[parts.length - 1] === "cancel") {
        const run = currentRuns.find((item) => item.id === id);
        if (!run) {
          await route.fulfill({ status: 404, json: { error: "not found" } });
          return;
        }
        const body = (request.postDataJSON() as { reason?: string } | null) ?? null;
        const override = opts?.onCancel?.(structuredClone(run), body);
        if (override) {
          await route.fulfill({ status: override.status, json: override.json });
          return;
        }
        if (
          run.status !== "pending" &&
          run.status !== "running" &&
          run.status !== "waiting_for_input"
        ) {
          await route.fulfill({
            status: 412,
            json: { error: "agent run is no longer active" },
          });
          return;
        }
        run.status = "canceled";
        run.completedAt = new Date().toISOString();
        if (body?.reason) {
          run.statusMessage = body.reason;
        }
        await route.fulfill({ json: runWithSession(run) });
        return;
      }

      if (request.method() !== "GET") {
        await route.fallback();
        return;
      }

      const run = currentRuns.find((item) => item.id === id);
      if (!run) {
        await route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }
      await route.fulfill({ json: runWithSession(run) });
    },
  );

  return {
    setRuns(nextRuns) {
      currentRuns = nextRuns.map((run) => structuredClone(run));
    },
    getRuns() {
      return currentRuns.map((run) => structuredClone(run));
    },
  };
}

export async function mockAgentNotConfigured(page: Page) {
  await page.route(/\/api\/v1\/agent\/sessions(?:\?.*)?$/, async (route: Route) => {
    await route.fulfill({
      status: 412,
      json: { error: "agent is not configured" },
    });
  });
  await page.route(
    /\/api\/v1\/agent\/sessions\/[^/?]+(?:\/turns)?(?:\?.*)?$/,
    async (route: Route) => {
      await route.fulfill({
        status: 412,
        json: { error: "agent is not configured" },
      });
    },
  );
  await page.route(
    /\/api\/v1\/agent\/turns\/[^/?]+(?:\/cancel)?(?:\?.*)?$/,
    async (route: Route) => {
      await route.fulfill({
        status: 412,
        json: { error: "agent is not configured" },
      });
    },
  );
}

export async function mockWorkflowRuns(
  page: Page,
  runs: WorkflowRun[],
  opts?: MockWorkflowRunsOptions,
): Promise<MockWorkflowRunsController> {
  let currentRuns = runs.map((run) => structuredClone(run));

  await page.route("**/api/v1/workflow/runs", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: currentRuns });
    } else {
      route.fallback();
    }
  });

  await page.route("**/api/v1/workflow/runs/**", (route: Route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const id = parts[parts.length - 2] === "runs"
      ? parts[parts.length - 1]
      : parts[parts.length - 2];

    if (request.method() === "POST" && parts[parts.length - 1] === "cancel") {
      const run = currentRuns.find((item) => item.id === id);
      if (!run) {
        route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }
      const body = (request.postDataJSON() as { reason?: string } | null) ?? null;
      const override = opts?.onCancel?.(structuredClone(run), body);
      if (override) {
        route.fulfill({ status: override.status, json: override.json });
        return;
      }
      if (run.status !== "pending") {
        route.fulfill({
          status: 412,
          json: { error: "workflow run cannot be canceled once it has started" },
        });
        return;
      }
      run.status = "canceled";
      run.completedAt = new Date().toISOString();
      if (body?.reason) {
        run.statusMessage = body.reason;
      }
      route.fulfill({ json: run });
      return;
    }

    if (request.method() !== "GET") {
      route.fallback();
      return;
    }
    const run = currentRuns.find((item) => item.id === id);
    if (!run) {
      route.fulfill({ status: 404, json: { error: "not found" } });
      return;
    }
    route.fulfill({ json: run });
  });

  return {
    setRuns(nextRuns) {
      currentRuns = nextRuns.map((run) => structuredClone(run));
    },
    getRuns() {
      return currentRuns.map((run) => structuredClone(run));
    },
  };
}

export async function mockWorkflowSchedules(
  page: Page,
  schedules: WorkflowSchedule[],
  opts?: MockWorkflowSchedulesOptions,
): Promise<MockWorkflowSchedulesController> {
  let currentSchedules = schedules.map((schedule) => structuredClone(schedule));

  await page.route("**/api/v1/workflow/schedules", async (route: Route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({ json: currentSchedules });
      return;
    }

    if (request.method() !== "POST") {
      await route.fallback();
      return;
    }

    const body = (request.postDataJSON() as Partial<WorkflowSchedule> & Record<string, unknown>) ?? {};
    const created =
      opts?.onCreate?.(body) ??
      ({
        id: `sched_${currentSchedules.length + 1}`,
        provider: typeof body.provider === "string" ? body.provider : "basic",
        cron: typeof body.cron === "string" ? body.cron : "* * * * *",
        timezone: typeof body.timezone === "string" ? body.timezone : undefined,
        target: (body.target as WorkflowSchedule["target"]) ?? {
          plugin: {
            name: "github",
            operation: "issues.create",
          },
        },
        paused: Boolean(body.paused),
        createdAt: "2026-04-21T00:00:00Z",
        updatedAt: "2026-04-21T00:00:00Z",
      } satisfies WorkflowSchedule);

    if ("status" in created) {
      await route.fulfill({ status: created.status, json: created.json });
      return;
    }

    currentSchedules = [structuredClone(created), ...currentSchedules];
    await route.fulfill({ json: created });
  });

  await page.route("**/api/v1/workflow/schedules/**", async (route: Route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const scheduleID = parts[parts.length - 2] === "schedules"
      ? parts[parts.length - 1]
      : parts[parts.length - 2];
    const current = currentSchedules.find((schedule) => schedule.id === scheduleID);

    if (!current) {
      await route.fulfill({ status: 404, json: { error: "not found" } });
      return;
    }

    if (request.method() === "GET") {
      await route.fulfill({ json: current });
      return;
    }

    if (request.method() === "DELETE") {
      currentSchedules = currentSchedules.filter((schedule) => schedule.id !== scheduleID);
      await route.fulfill({ json: { status: "deleted" } });
      return;
    }

    if (request.method() === "POST") {
      if (parts[parts.length - 1] === "pause") {
        current.paused = true;
        current.updatedAt = new Date().toISOString();
        await route.fulfill({ json: current });
        return;
      }
      if (parts[parts.length - 1] === "resume") {
        current.paused = false;
        current.updatedAt = new Date().toISOString();
        await route.fulfill({ json: current });
        return;
      }
    }

    if (request.method() !== "PUT") {
      await route.fallback();
      return;
    }

    const body = (request.postDataJSON() as Partial<WorkflowSchedule> & Record<string, unknown>) ?? {};
    const updated =
      opts?.onUpdate?.(structuredClone(current), body) ??
      ({
        ...current,
        provider: typeof body.provider === "string" && body.provider ? body.provider : current.provider,
        cron: typeof body.cron === "string" ? body.cron : current.cron,
        timezone: typeof body.timezone === "string" ? body.timezone : current.timezone,
        target: (body.target as WorkflowSchedule["target"]) ?? current.target,
        paused: typeof body.paused === "boolean" ? body.paused : current.paused,
        updatedAt: "2026-04-21T01:00:00Z",
      } satisfies WorkflowSchedule);

    if ("status" in updated) {
      await route.fulfill({ status: updated.status, json: updated.json });
      return;
    }

    currentSchedules = currentSchedules.map((schedule) =>
      schedule.id === scheduleID ? structuredClone(updated) : schedule,
    );
    await route.fulfill({ json: updated });
  });

  return {
    setSchedules(nextSchedules) {
      currentSchedules = nextSchedules.map((schedule) => structuredClone(schedule));
    },
    getSchedules() {
      return currentSchedules.map((schedule) => structuredClone(schedule));
    },
  };
}

export async function mockWorkflowEventTriggers(
  page: Page,
  triggers: WorkflowEventTrigger[],
  opts?: MockWorkflowEventTriggersOptions,
): Promise<MockWorkflowEventTriggersController> {
  let currentTriggers = triggers.map((trigger) => structuredClone(trigger));

  await page.route("**/api/v1/workflow/event-triggers", async (route: Route, request) => {
    if (request.method() === "GET") {
      await route.fulfill({ json: currentTriggers });
      return;
    }

    if (request.method() !== "POST") {
      await route.fallback();
      return;
    }

    const body = (request.postDataJSON() as Partial<WorkflowEventTrigger> & Record<string, unknown>) ?? {};
    const created =
      opts?.onCreate?.(body) ??
      ({
        id: `trg_${currentTriggers.length + 1}`,
        provider: typeof body.provider === "string" ? body.provider : "basic",
        match: (body.match as WorkflowEventTrigger["match"]) ?? { type: "task.updated" },
        target: (body.target as WorkflowEventTrigger["target"]) ?? {
          plugin: {
            name: "github",
            operation: "issues.create",
          },
        },
        paused: Boolean(body.paused),
        createdAt: "2026-04-21T00:00:00Z",
        updatedAt: "2026-04-21T00:00:00Z",
      } satisfies WorkflowEventTrigger);

    if ("status" in created) {
      await route.fulfill({ status: created.status, json: created.json });
      return;
    }

    currentTriggers = [structuredClone(created), ...currentTriggers];
    await route.fulfill({ json: created });
  });

  await page.route("**/api/v1/workflow/event-triggers/**", async (route: Route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const triggerID = parts[parts.length - 2] === "event-triggers"
      ? parts[parts.length - 1]
      : parts[parts.length - 2];
    const current = currentTriggers.find((trigger) => trigger.id === triggerID);

    if (!current) {
      await route.fulfill({ status: 404, json: { error: "not found" } });
      return;
    }

    if (request.method() === "GET") {
      await route.fulfill({ json: current });
      return;
    }

    if (request.method() === "DELETE") {
      currentTriggers = currentTriggers.filter((trigger) => trigger.id !== triggerID);
      await route.fulfill({ json: { status: "deleted" } });
      return;
    }

    if (request.method() === "POST") {
      if (parts[parts.length - 1] === "pause") {
        current.paused = true;
        current.updatedAt = new Date().toISOString();
        await route.fulfill({ json: current });
        return;
      }
      if (parts[parts.length - 1] === "resume") {
        current.paused = false;
        current.updatedAt = new Date().toISOString();
        await route.fulfill({ json: current });
        return;
      }
    }

    if (request.method() !== "PUT") {
      await route.fallback();
      return;
    }

    const body = (request.postDataJSON() as Partial<WorkflowEventTrigger> & Record<string, unknown>) ?? {};
    const updated =
      opts?.onUpdate?.(structuredClone(current), body) ??
      ({
        ...current,
        provider: typeof body.provider === "string" && body.provider ? body.provider : current.provider,
        match: (body.match as WorkflowEventTrigger["match"]) ?? current.match,
        target: (body.target as WorkflowEventTrigger["target"]) ?? current.target,
        paused: typeof body.paused === "boolean" ? body.paused : current.paused,
        updatedAt: "2026-04-21T01:00:00Z",
      } satisfies WorkflowEventTrigger);

    if ("status" in updated) {
      await route.fulfill({ status: updated.status, json: updated.json });
      return;
    }

    currentTriggers = currentTriggers.map((trigger) =>
      trigger.id === triggerID ? structuredClone(updated) : trigger,
    );
    await route.fulfill({ json: updated });
  });

  return {
    setTriggers(nextTriggers) {
      currentTriggers = nextTriggers.map((trigger) => structuredClone(trigger));
    },
    getTriggers() {
      return currentTriggers.map((trigger) => structuredClone(trigger));
    },
  };
}

function cloneRecordArray<T>(value: Record<string, T[]>): Record<string, T[]> {
  return Object.fromEntries(
    Object.entries(value).map(([key, items]) => [
      key,
      items.map((item) => structuredClone(item)),
    ]),
  );
}

type CustomFixtures = {
  authenticatedPage: Page;
};

export const test = base.extend<CustomFixtures>({
  authenticatedPage: async ({ page }, runAuthenticatedPage) => {
    await page.addInitScript(() => {
      localStorage.setItem("user_email", "test@gestalt.dev");
    });
    await runAuthenticatedPage(page);
  },
});

export { expect };
