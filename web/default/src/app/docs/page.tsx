import type { Metadata } from "next";
import { OverviewDocsPage } from "./DocsContent";

export const metadata: Metadata = {
  title: "Docs",
  description:
    "User documentation for the Gestalt CLI, API tokens, and MCP endpoint.",
};

export default function DocsPage() {
  return <OverviewDocsPage />;
}
