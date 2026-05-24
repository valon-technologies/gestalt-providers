import type { Metadata } from "next";
import { InvokeDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Invoke Operations",
  description: "Invoke app operations from the CLI or over HTTP.",
};

export default function InvokePage() {
  return <InvokeDocsPage />;
}
