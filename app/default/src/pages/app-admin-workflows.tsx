import { useParams } from "@tanstack/react-router";
import AppWorkflowRunsPanel from "@/components/AppWorkflowRunsPanel";

export default function AppAdminWorkflowsPage() {
  const { app: appName } = useParams({ from: "/apps/$app/admin/workflows" });
  return <AppWorkflowRunsPanel key={appName} appName={appName} />;
}
