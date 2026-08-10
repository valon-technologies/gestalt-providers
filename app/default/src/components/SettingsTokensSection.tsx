import TokenTable from "@/components/TokenTable";
import { SpinnerIcon } from "@/components/icons";
import { SETTINGS_TOKENS_LIST_TITLE } from "@/features/settings/tokens-copy";
import { useTokensQuery } from "@/lib/queries";

/**
 * Settings tokens inventory — table under the layout page header.
 * Create/mint lives on `/settings/tokens/new` (SettingsTokenCreate).
 */
export default function SettingsTokensSection() {
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
      className="scroll-mt-24 space-y-6"
      aria-label={SETTINGS_TOKENS_LIST_TITLE}
    >
      {tokensError ? (
        <p className="text-sm text-destructive">{tokensError}</p>
      ) : null}

      {tokensLoading ? (
        <p className="flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading tokens…
        </p>
      ) : !tokensError ? (
        <TokenTable tokens={tokens} />
      ) : null}
    </section>
  );
}
