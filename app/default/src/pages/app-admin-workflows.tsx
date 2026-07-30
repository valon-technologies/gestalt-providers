import { useParams } from "@tanstack/react-router";
import AppWorkflowRunsPanel from "@/components/AppWorkflowRunsPanel";

export default function AppAdminWorkflowsPage() {
  const { app } = useParams({ from: "/apps/$app/admin/workflows" });
  return (
    <section aria-label="Workflows">
      <AppWorkflowRunsPanel key={app} appName={app} />
    </section>
  );
}
