import { Link, useParams } from "@tanstack/react-router";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { AdminAppAccess } from "@/features/admin-access/admin-app-access";
import {
  ACCESS_RULE_HEADING,
  APP_ACCESS_NAV_LABEL,
} from "@/features/admin-access/admin-access-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import { useIntegrationsQuery } from "@/lib/queries";

export default function AdminAppPage() {
  const { app } = useParams({ from: "/admin/apps/$app" });
  const integrationsQuery = useIntegrationsQuery();
  const integration = integrationsQuery.data?.find((item) => item.name === app);
  const appLabel = integration ? getIntegrationLabel(integration) : app;
  useDocumentTitle(ACCESS_RULE_HEADING(appLabel));

  return (
    <AdminAppAccess
      appName={app}
      appLabel={appLabel}
      embedded
      heading={
        <Breadcrumb className="mb-6">
          <BreadcrumbList>
            <BreadcrumbItem>
              <BreadcrumbLink asChild>
                <Link to="/admin">{APP_ACCESS_NAV_LABEL}</Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage>{appLabel}</BreadcrumbPage>
            </BreadcrumbItem>
          </BreadcrumbList>
        </Breadcrumb>
      }
    />
  );
}
