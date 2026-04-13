import type { Metadata } from "next";
import { SetupDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Set Up The CLI",
  description: "Install the Gestalt CLI, point it at your workspace, and authenticate.",
};

export default function SetupPage() {
  return <SetupDocsPage />;
}
