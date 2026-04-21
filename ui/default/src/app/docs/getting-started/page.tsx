import type { Metadata } from "next";
import { GettingStartedDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Getting Started",
  description: "Install gestalt, point it at your workspace, and authenticate.",
};

export default function GettingStartedPage() {
  return <GettingStartedDocsPage />;
}
