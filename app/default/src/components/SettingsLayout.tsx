import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import { Eyebrow } from "@/components/ui/eyebrow";
import { PageLayout } from "@/components/ui/page-layout";
import {
  PageHeader,
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
  useDocumentTitle("Settings");
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const nested = pathname === SETTINGS_TOKENS_NEW_PATH;

  return (
    // PageLayout renders the <main>, so the Container stays a plain wrapper.
    // Section roots use the Settings page header; nested create uses
    // breadcrumbs so the content column can own the task h1.
    <Container className="py-12">
      <PageLayout
        tracks="compact"
        header={
          nested ? (
            <SettingsBreadcrumb pathname={pathname} />
          ) : (
            <PageHeader>
              <PageHeaderContent size="lg">
                <Eyebrow tone="accent">Account</Eyebrow>
                <PageHeaderTitle>Settings</PageHeaderTitle>
                <PageHeaderDescription>
                  Manage personal credentials for this account.
                </PageHeaderDescription>
              </PageHeaderContent>
            </PageHeader>
          )
        }
      >
        <Outlet />
      </PageLayout>
    </Container>
  );
}
