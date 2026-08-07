import { Link, Outlet, useNavigate, useRouterState } from "@tanstack/react-router";
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
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  SETTINGS_IDENTITIES_PATH,
  SETTINGS_TOKENS_PATH,
} from "@/lib/managed-identity-paths";

type SettingsSection = "tokens" | "identities";

// Rail labels are nouns. The section's own SectionHeader carries the fuller
// phrasing plus a description, so the rail is not a duplicate of the h2.
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

export default function SettingsLayout() {
  useDocumentTitle("Settings");
  const navigate = useNavigate();
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const section = sectionFromPathname(pathname);

  return (
    // PageLayout renders the <main>, so the Container stays a plain wrapper.
    <Container className="py-12">
      <PageLayout
        // "Settings" reads the same on both rail destinations, so it spans the
        // full column above the Pane (guidelines/page-layout.md).
        header={
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
        }
      >
        <Outlet />
      </PageLayout>
    </Container>
  );
}
