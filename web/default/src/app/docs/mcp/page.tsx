import type { Metadata } from "next";
import { McpDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Use With MCP",
  description: "Connect MCP clients to the current Gestalt workspace.",
};

export default function McpPage() {
  return <McpDocsPage />;
}
