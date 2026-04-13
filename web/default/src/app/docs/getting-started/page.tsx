import type { Metadata } from "next";
import { GettingStartedDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Getting Started",
  description:
    "Install the Gestalt CLI, point it at your workspace, and authenticate.",
};

export default function GettingStartedPage() {
  return <GettingStartedDocsPage />;
}
