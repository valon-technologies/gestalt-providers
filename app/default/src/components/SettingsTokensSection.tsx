import { Link } from "@tanstack/react-router";
import TokenTable from "@/components/TokenTable";
import { SpinnerIcon } from "@/components/icons";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import {
  SETTINGS_TOKENS_ACTIVE_SECTION,
  SETTINGS_TOKENS_CREATE_CTA,
  SETTINGS_TOKENS_DOCUMENT_TITLE,
  SETTINGS_TOKENS_LIST_DESCRIPTION,
  SETTINGS_TOKENS_LIST_TITLE,
} from "@/features/settings/tokens-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { SETTINGS_TOKENS_NEW_PATH } from "@/lib/managed-identity-paths";
import { useTokensQuery } from "@/lib/queries";

/**
 * Settings tokens inventory — list + single CTA into the create route.
 * Create/mint lives on `/settings/tokens/new` (SettingsTokenCreate).
 */
export default function SettingsTokensSection() {
  useDocumentTitle(SETTINGS_TOKENS_DOCUMENT_TITLE);
  const tokensQuery = useTokensQuery();

  const tokens = tokensQuery.data ?? [];
  const tokensLoading = tokensQuery.isPending;
  const tokensError = tokensQuery.error
    ? tokensQuery.error instanceof Error
      ? tokensQuery.error.message
      : "Failed to load tokens"
    : null;

  return (
    <section
      className="scroll-mt-24 space-y-8"
      aria-label={SETTINGS_TOKENS_LIST_TITLE}
    >
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{SETTINGS_TOKENS_LIST_TITLE}</PageHeaderTitle>
          <PageHeaderDescription>
            {SETTINGS_TOKENS_LIST_DESCRIPTION}
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions>
          <Button asChild>
            <Link to={SETTINGS_TOKENS_NEW_PATH}>{SETTINGS_TOKENS_CREATE_CTA}</Link>
          </Button>
        </PageHeaderActions>
      </PageHeader>

      {tokensError ? (
        <p className="text-sm text-destructive">{tokensError}</p>
      ) : null}

      <div className="space-y-4">
        {!tokensLoading && !tokensError && tokens.length > 0 ? (
          <SectionHeader>
            <SectionHeaderContent size="sm">
              <SectionHeaderTitle as="h2" size="sm">
                {SETTINGS_TOKENS_ACTIVE_SECTION}
              </SectionHeaderTitle>
            </SectionHeaderContent>
          </SectionHeader>
        ) : null}

        {tokensLoading ? (
          <p className="flex items-center gap-1.5 text-sm text-muted-foreground">
            <SpinnerIcon className="size-4 animate-spin" aria-hidden />
            Loading tokens…
          </p>
        ) : !tokensError ? (
          <TokenTable tokens={tokens} />
        ) : null}
      </div>
    </section>
  );
}
