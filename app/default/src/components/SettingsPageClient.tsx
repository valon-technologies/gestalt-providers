import { Link } from "@tanstack/react-router";
import Container from "@/components/Container";
import TokenCreateForm from "@/components/TokenCreateForm";
import TokenTable from "@/components/TokenTable";
import { Eyebrow } from "@/components/ui/eyebrow";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  useInvalidateTokens,
  useTokensQuery,
} from "@/hooks/use-server-queries";

export default function SettingsPageClient() {
  const tokensQuery = useTokensQuery();
  const invalidateTokens = useInvalidateTokens();

  const tokens = tokensQuery.data ?? [];
  const tokensLoading = tokensQuery.isPending;
  const tokensError =
    tokensQuery.error instanceof Error
      ? tokensQuery.error.message
      : tokensQuery.error
        ? "Failed to load tokens"
        : null;

  async function refreshTokens() {
    await invalidateTokens();
  }

  return (
    <Container as="main" className="py-12">
      <PageHeader>
        <PageHeaderContent>
          <div className="flex flex-col gap-3">
            <Eyebrow>Account</Eyebrow>
            <PageHeaderTitle size="lg">Settings</PageHeaderTitle>
          </div>
          <PageHeaderDescription className="max-w-3xl">
            Manage authorization for your account — personal API tokens and
            shared service identities.
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <section
        id="authorization"
        className="mt-12 rounded-2xl border border-alpha bg-base-white p-6 dark:bg-surface"
      >
        <SettingsSectionIntro
          eyebrow="Authorization"
          title="Your API Tokens"
          description="Create personal tokens for local tooling, scripts, and one-off integrations. These act as you."
        />

        <div className="mt-8">
          <div className="rounded-xl border border-alpha bg-base-white p-5 dark:bg-surface-raised">
            <TokenCreateForm onCreated={refreshTokens} />
          </div>
        </div>

        {tokensError && <p className="mt-4 text-sm text-ember-500">{tokensError}</p>}

        {tokensLoading ? (
          <p className="mt-10 text-sm text-faint">Loading...</p>
        ) : !tokensError ? (
          <div className="mt-8">
            <TokenTable tokens={tokens} onRevoked={refreshTokens} />
          </div>
        ) : null}
      </section>

      <section className="mt-6 rounded-2xl border border-alpha bg-base-white p-6 dark:bg-surface">
        <SettingsSectionIntro
          eyebrow="Authorization"
          title="Managed Identities"
          description="Create shared service-account subjects, grant app roles, and mint subject-owned API tokens for automation."
        />
        <Link
          to="/identities"
          className="mt-6 inline-flex rounded-md border border-alpha px-4 py-2 text-sm font-medium text-foreground transition-colors duration-150 hover:border-alpha-strong hover:bg-base-100 dark:hover:bg-surface-raised"
        >
          Manage identities
        </Link>
      </section>
    </Container>
  );
}

function SettingsSectionIntro({
  eyebrow,
  title,
  description,
}: {
  eyebrow: string;
  title: string;
  description: string;
}) {
  return (
    <div className="max-w-2xl">
      <Eyebrow>{eyebrow}</Eyebrow>
      <h2 className="mt-2 font-heading text-xl text-foreground">{title}</h2>
      <p className="mt-2 text-sm text-muted-foreground">{description}</p>
    </div>
  );
}
