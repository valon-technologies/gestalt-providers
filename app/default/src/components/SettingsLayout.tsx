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
    // List owns one API-tokens page header; nested create uses breadcrumbs so
    // the content column can own the task h1.
    <Container className="py-12">
      <PageLayout
        tracks="compact"
        header={
          nested ? (
            <SettingsBreadcrumb pathname={pathname} />
          ) : (
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
        <Outlet />
      </PageLayout>
    </Container>
  );
}
