import { test as base, expect, type Page, type Route } from "@playwright/test";
import type {
  APIToken,
  Integration,
  ManagedIdentity,
  WorkflowEventTrigger,
  WorkflowRun,
  WorkflowSchedule,
} from "../src/lib/api";

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
  opts?: { onDisconnect?: (name: string) => void },
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
      opts?.onDisconnect?.(name);
      route.fulfill({ json: { status: "disconnected" } });
    } else {
      route.fallback();
    }
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
  await page.route("**/api/v1/identities", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: identities });
    } else {
      route.fallback();
    }
  });
}

export async function mockAuthInfo(
  page: Page,
  info: { provider: string; displayName: string; loginSupported?: boolean },
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
          plugin: "github",
          operation: "issues.create",
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
          plugin: "github",
          operation: "issues.create",
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
