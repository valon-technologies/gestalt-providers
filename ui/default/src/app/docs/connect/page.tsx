import type { Metadata } from "next";
import { ConnectDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Connect Apps",
  description: "Connect apps in the current Gestalt workspace.",
};

export default function ConnectPage() {
  return <ConnectDocsPage />;
}
