import TokenCreateForm from "@/components/TokenCreateForm";
import TokenTable from "@/components/TokenTable";
import { SpinnerIcon } from "@/components/icons";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { useInvalidateTokens, useTokensQuery } from "@/lib/queries";

export default function SettingsTokensSection() {
  const tokensQuery = useTokensQuery();
  const invalidateTokens = useInvalidateTokens();

  const tokens = tokensQuery.data ?? [];
  const tokensLoading = tokensQuery.isPending;
  const tokensError = tokensQuery.error
    ? tokensQuery.error instanceof Error
      ? tokensQuery.error.message
      : "Failed to load tokens"
    : null;

  async function refreshTokens() {
    await invalidateTokens();
  }

  return (
    <section className="scroll-mt-24 space-y-8" aria-label="Your API tokens">
      <SectionHeader>
        <SectionHeaderContent>
          <SectionHeaderTitle>Your API tokens</SectionHeaderTitle>
          <SectionHeaderDescription>
            Create personal tokens for local tooling, scripts, and one-off
            integrations. These act as you.
          </SectionHeaderDescription>
        </SectionHeaderContent>
      </SectionHeader>

      <div
        id="authorization"
        className="rounded-xl border border-border bg-card p-5 text-card-foreground"
      >
        <TokenCreateForm
          controlsClassName="max-w-[50%]"
          onCreated={async () => {
            await refreshTokens();
          }}
        />
      </div>

      {tokensError ? (
        <p className="text-sm text-destructive">{tokensError}</p>
      ) : null}

      <div className="space-y-4">
        <SectionHeader>
          <SectionHeaderContent size="sm">
            <SectionHeaderTitle as="h3" size="sm">
              Active tokens
            </SectionHeaderTitle>
          </SectionHeaderContent>
        </SectionHeader>

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
