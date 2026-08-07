import { Link, Outlet, useNavigate, useParams, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import { Eyebrow } from "@/components/ui/eyebrow";
import { NavList, NavListItem, NavListItemLabel } from "@/components/ui/nav-list";
import { PageLayout } from "@/components/ui/page-layout";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SegmentedControl } from "@/components/ui/segmented-control";
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
  SETTINGS_IDENTITIES_PATH,
  SETTINGS_TOKENS_NEW_PATH,
  SETTINGS_TOKENS_PATH,
} from "@/lib/managed-identity-paths";

type SettingsSection = "tokens" | "identities";

// Rail labels are nouns. The content PageHeader carries the fuller phrasing
// plus a description, so the rail is not a duplicate of the h1.
const SECTION_OPTIONS: Array<{ value: SettingsSection; label: string }> = [
  { value: "tokens", label: "API tokens" },
  { value: "identities", label: "Managed identities" },
];

const SECTION_PATHS: Record<SettingsSection, string> = {
  tokens: SETTINGS_TOKENS_PATH,
  identities: SETTINGS_IDENTITIES_PATH,
};

function sectionFromPathname(pathname: string): SettingsSection {
  if (pathname.startsWith(SETTINGS_IDENTITIES_PATH)) return "identities";
  return "tokens";
}

function SettingsBreadcrumb({ pathname }: { pathname: string }) {
  const params = useParams({ strict: false });
  const identityLocalId =
    typeof params.identityLocalId === "string" ? params.identityLocalId : null;
  const isCreateToken = pathname === SETTINGS_TOKENS_NEW_PATH;
  const isIdentities = pathname.startsWith(SETTINGS_IDENTITIES_PATH);
  const isIdentityDetail = Boolean(identityLocalId);

  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink asChild>
            <Link to={SETTINGS_TOKENS_PATH}>Settings</Link>
          </BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator />
        {isIdentities ? (
          isIdentityDetail ? (
            <>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to={SETTINGS_IDENTITIES_PATH}>Managed identities</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>{identityLocalId}</BreadcrumbPage>
              </BreadcrumbItem>
            </>
          ) : (
            <BreadcrumbItem>
              <BreadcrumbPage>Managed identities</BreadcrumbPage>
            </BreadcrumbItem>
          )
        ) : isCreateToken ? (
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

function isNestedSettingsPath(pathname: string, identityLocalId: string | null) {
  return (
    pathname === SETTINGS_TOKENS_NEW_PATH ||
    (pathname.startsWith(SETTINGS_IDENTITIES_PATH) && Boolean(identityLocalId))
  );
}

export default function SettingsLayout() {
  useDocumentTitle("Settings");
  const navigate = useNavigate();
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const params = useParams({ strict: false });
  const identityLocalId =
    typeof params.identityLocalId === "string" ? params.identityLocalId : null;
  const section = sectionFromPathname(pathname);
  const nested = isNestedSettingsPath(pathname, identityLocalId);

  return (
    // PageLayout renders the <main>, so the Container stays a plain wrapper.
    // Section roots use the Settings page header; nested create/detail use
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
                  Manage authorization for your account — personal API tokens and
                  shared service identities.
                </PageHeaderDescription>
              </PageHeaderContent>
            </PageHeader>
          )
        }
        pane={
          <NavList aria-label="Settings sections">
            {SECTION_OPTIONS.map((option) => (
              <NavListItem
                key={option.value}
                asChild
                active={option.value === section}
              >
                <Link to={SECTION_PATHS[option.value]}>
                  <NavListItemLabel>{option.label}</NavListItemLabel>
                </Link>
              </NavListItem>
            ))}
          </NavList>
        }
        paneMobile={
          <div className="overflow-x-auto p-1">
            <SegmentedControl
              label="Settings sections"
              options={SECTION_OPTIONS}
              value={section}
              onValueChange={(next) => {
                void navigate({ to: SECTION_PATHS[next] });
              }}
              showLabels
              size="sm"
            />
          </div>
        }
      >
        <Outlet />
      </PageLayout>
    </Container>
  );
}
