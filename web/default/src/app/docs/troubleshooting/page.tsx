import type { Metadata } from "next";
import { TroubleshootingDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Troubleshooting",
  description: "Troubleshooting for the Gestalt user guide and client workflows.",
};

export default function TroubleshootingPage() {
  return <TroubleshootingDocsPage />;
}
