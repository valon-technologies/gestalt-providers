import { useParams } from "@tanstack/react-router";
import AppAdminPageClient from "@/components/AppAdminPageClient";

export default function AppAdminPage() {
  const { app } = useParams({ from: "/apps/$app/admin" });
  return <AppAdminPageClient appName={app} />;
}
