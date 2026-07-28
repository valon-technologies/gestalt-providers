import { Link, Outlet, useNavigate, useRouterState } from "@tanstack/react-router";
import Container from "@/components/Container";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SegmentedControl } from "@/components/ui/segmented-control";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import {
  SETTINGS_IDENTITIES_PATH,
  SETTINGS_TOKENS_PATH,
} from "@/lib/managed-identity-paths";

type SettingsSection = "tokens" | "identities";

const SECTION_OPTIONS: Array<{ value: SettingsSection; label: string }> = [
  { value: "tokens", label: "Your API Tokens" },
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
    <Container as="main" className="py-12">
      <PageHeader>
        <PageHeaderContent size="lg">
          <Eyebrow tone="brand">Account</Eyebrow>
          <PageHeaderTitle>Settings</PageHeaderTitle>
          <PageHeaderDescription>
            Manage authorization for your account — personal API tokens and
            shared service identities.
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <div className="mt-8 xl:hidden">
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

      <div className="mt-8 grid gap-10 xl:grid-cols-[220px_minmax(0,1fr)]">
        <aside className="hidden xl:block">
          <div className="sticky top-24">
            <nav className="space-y-0.5" aria-label="Settings sections">
              {SECTION_OPTIONS.map((option) => {
                const isActive = option.value === section;
                return (
                  <Link
                    key={option.value}
                    to={SECTION_PATHS[option.value]}
                    data-selected={isActive || undefined}
                    className={cn(
                      "block w-full rounded-md px-3 py-2 text-left text-sm outline-none focus-ring-inset",
                      listItemInteraction({ pointer: "css" }),
                    )}
                  >
                    {option.label}
                  </Link>
                );
              })}
            </nav>
          </div>
        </aside>

        <div className="min-w-0">
          <Outlet />
        </div>
      </div>
    </Container>
  );
}
