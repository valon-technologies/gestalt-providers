import Container from "@/components/Container";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { AdminAppList } from "@/features/admin-access/admin-app-list";
import {
  ADMIN_PAGE_DESCRIPTION,
  ADMIN_PAGE_TITLE,
} from "@/features/admin-access/admin-access-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { useIntegrationsQuery } from "@/lib/queries";

export default function AdminPage() {
  useDocumentTitle(ADMIN_PAGE_TITLE);
  const integrationsQuery = useIntegrationsQuery();
  const integrations = integrationsQuery.data ?? [];
  const loading = integrationsQuery.isPending;
  const error =
    integrationsQuery.error instanceof Error
      ? integrationsQuery.error.message
      : integrationsQuery.error
        ? "Couldn't load apps. Refresh the page and try again."
        : null;

  return (
    <Container as="main" className="py-8">
      <PageHeader>
        <PageHeaderContent size="lg">
          <PageHeaderTitle>{ADMIN_PAGE_TITLE}</PageHeaderTitle>
          <PageHeaderDescription>{ADMIN_PAGE_DESCRIPTION}</PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>
      <AdminAppList
        integrations={integrations}
        loading={loading}
        error={error}
      />
    </Container>
  );
}
