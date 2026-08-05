import { describe, expect, it } from "vitest";
import {
  workspaceDocumentTitle,
  workflowAdminPageLabel,
  workspaceLocationForPathname,
} from "./app-nav";

describe("workspaceLocationForPathname", () => {
  it("resolves overview for the app root", () => {
    expect(workspaceLocationForPathname("/apps/g-issues", "g-issues")).toEqual({
      id: "overview",
      label: "Overview",
      to: "/apps/$app",
      isOverview: true,
    });
  });

  it("resolves nested admin surfaces from the nav catalog", () => {
    expect(
      workspaceLocationForPathname("/apps/g-issues/admin/workflows", "g-issues"),
    ).toMatchObject({
      id: "workflows",
      label: "Workflows",
      isOverview: false,
    });
    expect(
      workspaceLocationForPathname(
        "/apps/g-issues/admin/workflows/runs/run_1",
        "g-issues",
      ),
    ).toMatchObject({
      id: "workflows",
      label: "Workflows",
    });
    expect(
      workspaceLocationForPathname("/apps/slack/admin/agent-identities", "slack"),
    ).toMatchObject({
      id: "agent-identities",
      label: "Agent identities",
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
    ).toBe("Definitions");
    expect(
      workflowAdminPageLabel(
        "/apps/slack/admin/workflows/definitions/app_slack_notify",
        "slack",
      ),
    ).toBe("app_slack_notify");
  });
});

describe("workspaceDocumentTitle", () => {
  it("uses the app label alone on overview", () => {
    expect(
      workspaceDocumentTitle("g-issues", {
        id: "overview",
        label: "Overview",
        to: "/apps/$app",
        isOverview: true,
      }),
    ).toBe("g-issues");
  });

  it("puts the surface first on deep routes", () => {
    expect(
      workspaceDocumentTitle("g-issues", {
        id: "workflows",
        label: "Workflows",
        to: "/apps/$app/admin/workflows",
        isOverview: false,
      }),
    ).toBe("Workflows · g-issues");
  });

  it("nests run and definition pages under Workflows", () => {
    expect(
      workspaceDocumentTitle(
        "g-issues",
        {
          id: "workflows",
          label: "Workflows",
          to: "/apps/$app/admin/workflows",
          isOverview: false,
        },
        {
          pathname: "/apps/g-issues/admin/workflows/runs/run_1",
          app: "g-issues",
        },
      ),
    ).toBe("run_1 · Workflows · g-issues");
  });
});
