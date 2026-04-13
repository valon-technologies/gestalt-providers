import type { Metadata } from "next";
import { ConnectDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Connect Plugins",
  description: "Connect plugins in the current Gestalt workspace.",
};

export default function ConnectPage() {
  return <ConnectDocsPage />;
}
