import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import { Button } from "@/components/ui/button";
import { PageLayout } from "@/components/ui/page-layout";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import {
  SETTINGS_TOKENS_CREATE_CTA,
  SETTINGS_TOKENS_DOCUMENT_TITLE,
  SETTINGS_TOKENS_LIST_DESCRIPTION,
  SETTINGS_TOKENS_LIST_TITLE,
} from "@/features/settings/tokens-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  SETTINGS_TOKENS_NEW_PATH,
  SETTINGS_TOKENS_PATH,
} from "@/lib/managed-identity-paths";
import { PAGE_LAYOUT_READING_COLUMN_CLASS } from "@/lib/page-layout-content-top";

function SettingsBreadcrumb({ pathname }: { pathname: string }) {
  const isCreateToken = pathname === SETTINGS_TOKENS_NEW_PATH;

  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink asChild>
            <Link to={SETTINGS_TOKENS_PATH}>Settings</Link>
          </BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator />
        {isCreateToken ? (
          <>
            <BreadcrumbItem>
              <BreadcrumbLink asChild>
                <Link to={SETTINGS_TOKENS_PATH}>API tokens</Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage>Create token</BreadcrumbPage>
            </BreadcrumbItem>
          </>
        ) : (
          <BreadcrumbItem>
            <BreadcrumbPage>API tokens</BreadcrumbPage>
          </BreadcrumbItem>
        )}
      </BreadcrumbList>
    </Breadcrumb>
  );
}

export default function SettingsLayout() {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const nested = pathname === SETTINGS_TOKENS_NEW_PATH;

  // Nested create overrides with its own document title while mounted.
  useDocumentTitle(SETTINGS_TOKENS_DOCUMENT_TITLE);

  return (
    // PageLayout renders the <main>, so the Container stays a plain wrapper.
    // The list owns the page header and stays full width for the table.
    // Create is a single-column task: breadcrumbs, title, form, and callout
    // share the reading column. The content column owns the task h1.
    <Container>
      <PageLayout
        tracks="compact"
        header={
          nested ? undefined : (
            <PageHeader>
              <PageHeaderContent size="lg">
                <PageHeaderTitle>{SETTINGS_TOKENS_LIST_TITLE}</PageHeaderTitle>
                <PageHeaderDescription>
                  {SETTINGS_TOKENS_LIST_DESCRIPTION}
                </PageHeaderDescription>
              </PageHeaderContent>
              <PageHeaderActions>
                <Button asChild>
                  <Link to={SETTINGS_TOKENS_NEW_PATH}>
                    {SETTINGS_TOKENS_CREATE_CTA}
                  </Link>
                </Button>
              </PageHeaderActions>
            </PageHeader>
          )
        }
      >
        {nested ? (
          <div className={PAGE_LAYOUT_READING_COLUMN_CLASS}>
            <div className="space-y-8">
              <SettingsBreadcrumb pathname={pathname} />
              <Outlet />
            </div>
          </div>
        ) : (
          <Outlet />
        )}
      </PageLayout>
    </Container>
  );
}
