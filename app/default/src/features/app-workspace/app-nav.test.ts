import { describe, expect, it } from "vitest";
import {
  adminSurfaceForPathname,
  APP_ADMIN_NAV,
  workspaceDocumentTitle,
  workflowAdminPageLabel,
  workspaceLocationForPathname,
} from "./app-nav";

describe("workspaceLocationForPathname", () => {
  it("resolves overview for the app root", () => {
    expect(workspaceLocationForPathname("/apps/example-app", "example-app")).toEqual({
      id: "overview",
      label: "Overview",
      to: "/apps/$app",
      isOverview: true,
    });
  });

  it("resolves nested admin surfaces from the nav catalog", () => {
    expect(
      workspaceLocationForPathname("/apps/example-app/admin/workflows", "example-app"),
    ).toMatchObject({
      id: "workflows",
      label: "Workflows",
      isOverview: false,
    });
    expect(
      workspaceLocationForPathname(
        "/apps/example-app/admin/workflows/runs/run_1",
        "example-app",
      ),
    ).toMatchObject({
      id: "workflows",
      label: "Workflows",
    });
    expect(
      workspaceLocationForPathname("/apps/slack/admin/service-accounts", "slack"),
    ).toMatchObject({
      id: "service-accounts",
      label: "Service accounts",
    });
  });

  it("keeps version subpaths under Versions via longest match", () => {
    expect(
      workspaceLocationForPathname("/apps/slack/versions/1.2.3", "slack"),
    ).toMatchObject({
      id: "versions",
      label: "Versions",
      isOverview: false,
    });
  });

  it("resolves per-app Metrics for app admins", () => {
    expect(
      workspaceLocationForPathname("/apps/slack/metrics", "slack"),
    ).toMatchObject({
      id: "metrics",
      label: "Metrics",
      isOverview: false,
    });
    expect(adminSurfaceForPathname("/apps/slack/metrics", "slack")).toBe(
      "authorization",
    );
    expect(
      APP_ADMIN_NAV.find((item) => item.id === "metrics")?.requires,
    ).toBe("authorization");
  });

  it("resolves user surfaces", () => {
    expect(
      workspaceLocationForPathname("/apps/slack/operations", "slack"),
    ).toMatchObject({
      id: "operations",
      label: "Operations",
      isOverview: false,
    });
    expect(
      workspaceLocationForPathname("/apps/slack/connection/", "slack"),
    ).toMatchObject({
      id: "connection",
      label: "Connection",
      isOverview: false,
    });
  });
});

describe("workflowAdminPageLabel", () => {
  it("returns nested labels under Workflows", () => {
    expect(workflowAdminPageLabel("/apps/slack/admin/workflows", "slack")).toBe(
      null,
    );
    expect(
      workflowAdminPageLabel("/apps/slack/admin/workflows/runs/run_1", "slack"),
    ).toBe("run_1");
    expect(
      workflowAdminPageLabel(
        "/apps/slack/admin/workflows/runs/run_1/jobs/workflow/steps/diagnose",
        "slack",
      ),
    ).toBe("diagnose");
    expect(
      workflowAdminPageLabel("/apps/slack/admin/workflows/definitions", "slack"),
    ).toBe(null);
    expect(
      workflowAdminPageLabel(
        "/apps/slack/admin/workflows/definitions/app_slack_notify",
        "slack",
      ),
    ).toBe("app_slack_notify");
  });

  it("prefers an explicit run label over the encoded run id", () => {
    expect(
      workflowAdminPageLabel(
        "/apps/slack/admin/workflows/runs/eyJraW5kIjoidGVtcG9yYWwtcnVuIn0",
        "slack",
        { runLabel: "ai-spend-tracker.runs.sync.fanout.execute" },
      ),
    ).toBe("ai-spend-tracker.runs.sync.fanout.execute");
  });
});

describe("workspaceDocumentTitle", () => {
  it("uses the app label alone on overview", () => {
    expect(
      workspaceDocumentTitle("example-app", {
        id: "overview",
        label: "Overview",
        to: "/apps/$app",
        isOverview: true,
      }),
    ).toBe("example-app");
  });

  it("puts the surface first on deep routes", () => {
    expect(
      workspaceDocumentTitle("example-app", {
        id: "workflows",
        label: "Workflows",
        to: "/apps/$app/admin/workflows",
        isOverview: false,
      }),
    ).toBe("Workflows · example-app");
  });

  it("nests run and definition pages under Workflows", () => {
    expect(
      workspaceDocumentTitle(
        "example-app",
        {
          id: "workflows",
          label: "Workflows",
          to: "/apps/$app/admin/workflows",
          isOverview: false,
        },
        {
          pathname: "/apps/example-app/admin/workflows/runs/run_1",
          app: "example-app",
        },
      ),
    ).toBe("run_1 · Workflows · example-app");
  });
});
