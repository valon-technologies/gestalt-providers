import type { Metadata } from "next";
import { WorkflowsDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Manage Workflows",
  description: "Inspect workflow schedules, event triggers, and run history from the Gestalt CLI.",
};

export default function WorkflowsPage() {
  return <WorkflowsDocsPage />;
}
