import type { Metadata } from "next";
import { InvokeDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Invoke Operations",
  description: "Invoke plugin operations from the CLI or over HTTP.",
};

export default function InvokePage() {
  return <InvokeDocsPage />;
}
