import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { AdminAppList } from "@/features/admin-access/admin-app-list";
import {
  APP_ACCESS_PAGE_DESCRIPTION,
  APP_ACCESS_PAGE_TITLE,
} from "@/features/admin-access/admin-access-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { useIntegrationsQuery } from "@/lib/queries";

export default function AdminPage() {
  useDocumentTitle(APP_ACCESS_PAGE_TITLE);
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
    <>
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{APP_ACCESS_PAGE_TITLE}</PageHeaderTitle>
          <PageHeaderDescription>{APP_ACCESS_PAGE_DESCRIPTION}</PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>
      <AdminAppList
        integrations={integrations}
        loading={loading}
        error={error}
      />
    </>
  );
}
