import { useParams } from "@tanstack/react-router";
import AppWorkflowRunsPanel from "@/components/AppWorkflowRunsPanel";

export default function AppAdminWorkflowsPage() {
  const { app } = useParams({ from: "/apps/$app/admin/workflows" });
  return (
    <section aria-label="Workflows">
      <h1 className="text-2xl font-heading text-foreground">Workflows</h1>
      <div className="mt-6">
        <AppWorkflowRunsPanel key={app} appName={app} />
      </div>
    </section>
  );
}
