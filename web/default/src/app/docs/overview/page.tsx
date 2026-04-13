import type { Metadata } from "next";
import { OverviewDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Overview",
  description: "Overview of the Gestalt user guide.",
};

export default function OverviewPage() {
  return <OverviewDocsPage />;
}
