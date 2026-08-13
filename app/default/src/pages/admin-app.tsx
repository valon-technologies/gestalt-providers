import { Link, useParams } from "@tanstack/react-router";
import Container from "@/components/Container";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { AdminAppAccess } from "@/features/admin-access/admin-app-access";
import { ACCESS_RULE_HEADING } from "@/features/admin-access/admin-access-copy";
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
    <Container as="main" className="pb-24">
      <AdminAppAccess
        appName={app}
        appLabel={appLabel}
        heading={
          <Breadcrumb className="mt-8 mb-6">
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to="/admin">Admin</Link>
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
    </Container>
  );
}
