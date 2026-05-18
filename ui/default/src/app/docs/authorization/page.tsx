import type { Metadata } from "next";
import { AuthorizationDocsPage } from "../DocsContent";

export const metadata: Metadata = {
  title: "Grant Authorization",
  description: "Grant users and service accounts access to plugins.",
};

export default function AuthorizationPage() {
  return <AuthorizationDocsPage />;
}
