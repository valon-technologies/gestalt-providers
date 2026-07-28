import { useEffect, useState } from "react";
import { Link as RouterLink } from "@tanstack/react-router";
import Container from "@/components/Container";
import TokenCreateForm from "@/components/TokenCreateForm";
import TokenTable from "@/components/TokenTable";
import { SpinnerIcon } from "@/components/icons";
import { Eyebrow } from "@/components/ui/eyebrow";
import { Link } from "@/components/ui/link";
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
  useInvalidateTokens,
  useManagedIdentitiesQuery,
  useTokensQuery,
} from "@/lib/queries";

type SettingsSection = "tokens" | "identities";

const SECTION_OPTIONS: Array<{ value: SettingsSection; label: string }> = [
  { value: "tokens", label: "Your API Tokens" },
  { value: "identities", label: "Managed Identities" },
];

const SECTION_CARD =
  "rounded-lg border border-alpha bg-base-white p-6 dark:bg-surface";

function sectionFromHash(hash: string): SettingsSection {
  if (hash === "#identities") return "identities";
  return "tokens";
}

export default function SettingsPageClient() {
  useDocumentTitle("Settings");
  const [section, setSection] = useState<SettingsSection>(() =>
    typeof window !== "undefined"
      ? sectionFromHash(window.location.hash)
      : "tokens",
  );

  const tokensQuery = useTokensQuery();
  const invalidateTokens = useInvalidateTokens();
  const identitiesQuery = useManagedIdentitiesQuery();

  const tokens = tokensQuery.data ?? [];
  const tokensLoading = tokensQuery.isPending;
  const tokensError = tokensQuery.error
    ? tokensQuery.error instanceof Error
      ? tokensQuery.error.message
      : "Failed to load tokens"
    : null;

  const identities = identitiesQuery.data ?? [];
  const identitiesLoading = identitiesQuery.isPending;
  const identitiesError = identitiesQuery.error
    ? identitiesQuery.error instanceof Error
      ? identitiesQuery.error.message
      : "Failed to load identities"
    : null;

  useEffect(() => {
    function syncFromHash() {
      setSection(sectionFromHash(window.location.hash));
    }
    syncFromHash();
    window.addEventListener("hashchange", syncFromHash);
    return () => window.removeEventListener("hashchange", syncFromHash);
  }, []);

  function selectSection(next: SettingsSection) {
    setSection(next);
    const hash = next === "identities" ? "#identities" : "#authorization";
    if (window.location.hash !== hash) {
      window.history.replaceState(null, "", `${window.location.pathname}${hash}`);
    }
  }

  async function refreshTokens() {
    await invalidateTokens();
  }

  return (
    <Container as="main" className="py-12">
      <div className="grid gap-10 xl:grid-cols-[220px_minmax(0,1fr)]">
        <aside className="hidden xl:block">
          <div className="sticky top-24">
            <nav className="space-y-0.5" aria-label="Settings sections">
              {SECTION_OPTIONS.map((option) => {
                const isActive = option.value === section;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => selectSection(option.value)}
                    data-selected={isActive || undefined}
                    className={cn(
                      "block w-full rounded-md px-3 py-2 text-left text-sm",
                      listItemInteraction({ pointer: "css" }),
                    )}
                  >
                    {option.label}
                  </button>
                );
              })}
            </nav>
          </div>
        </aside>

        <div className="min-w-0">
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
              onValueChange={selectSection}
              showLabels
              size="sm"
            />
          </div>

          <div className="mt-8 space-y-6">
            {section === "tokens" ? (
              <section
                id="authorization"
                className={SECTION_CARD}
                aria-label="Your API Tokens"
              >
                <h2 className="text-lg font-heading text-foreground">
                  Your API Tokens
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  Create personal tokens for local tooling, scripts, and one-off
                  integrations. These act as you.
                </p>

                <div className="mt-8">
                  <div className="rounded-xl border border-alpha bg-base-white p-5 dark:bg-surface-raised">
                    <TokenCreateForm
                      onCreated={async () => {
                        await refreshTokens();
                      }}
                    />
                  </div>
                </div>

                {tokensError ? (
                  <p className="mt-4 text-sm text-ember-500">{tokensError}</p>
                ) : null}

                {tokensLoading ? (
                  <p className="mt-10 flex items-center gap-1.5 text-sm text-faint">
                    <SpinnerIcon className="size-4 animate-spin" aria-hidden />
                    Loading tokens…
                  </p>
                ) : !tokensError ? (
                  <div className="mt-8">
                    <TokenTable tokens={tokens} />
                  </div>
                ) : null}
              </section>
            ) : null}

            {section === "identities" ? (
              <section
                id="identities"
                className={SECTION_CARD}
                aria-label="Managed Identities"
              >
                <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
                  <div>
                    <h2 className="text-lg font-heading text-foreground">
                      Managed Identities
                    </h2>
                    <p className="mt-1 text-sm text-muted-foreground">
                      Create shared service-account subjects, grant app roles,
                      and mint subject-owned API tokens for automation.
                    </p>
                  </div>
                  <Link asChild underlineVariant="always">
                    <RouterLink to="/identities">Manage identities</RouterLink>
                  </Link>
                </div>

                {identitiesLoading ? (
                  <p className="mt-5 flex items-center gap-1.5 text-sm text-faint">
                    <SpinnerIcon className="size-4 animate-spin" aria-hidden />
                    Loading identities…
                  </p>
                ) : null}

                {identitiesError ? (
                  <p className="mt-5 text-sm text-ember-500">
                    {identitiesError}
                  </p>
                ) : null}

                {!identitiesLoading &&
                !identitiesError &&
                identities.length === 0 ? (
                  <p className="mt-5 text-sm text-faint">
                    No managed identities yet.
                  </p>
                ) : null}

                {!identitiesLoading && identities.length > 0 ? (
                  <ul className="mt-5 divide-y divide-alpha rounded-lg border border-alpha">
                    {identities.map((identity) => (
                      <li
                        key={identity.subjectId}
                        className="flex flex-col gap-1 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
                      >
                        <div className="min-w-0">
                          <Link asChild underlineVariant="always">
                            <RouterLink
                              to="/identities"
                              search={{ id: identity.subjectId }}
                            >
                              {identity.displayName || identity.subjectId}
                            </RouterLink>
                          </Link>
                          <p className="mt-0.5 font-mono text-xs text-muted-foreground">
                            {identity.subjectId}
                          </p>
                        </div>
                        <span className="text-xs uppercase tracking-[0.16em] text-muted-foreground-soft">
                          {identity.kind}
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : null}
              </section>
            ) : null}
          </div>
        </div>
      </div>
    </Container>
  );
}
