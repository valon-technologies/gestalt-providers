import type { Metadata } from "next";
import { TokensDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Manage API Tokens",
  description: "Create and revoke API tokens for the current workspace.",
};

export default function TokensPage() {
  return <TokensDocsPage />;
}
