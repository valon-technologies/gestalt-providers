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
import { useTokensQuery } from "@/lib/queries";

export default function SettingsPage() {
  const tokensQuery = useTokensQuery();
  const tokens = tokensQuery.data ?? [];
  const tokensError = tokensQuery.error
    ? tokensQuery.error instanceof Error
      ? tokensQuery.error.message
      : "Failed to load tokens"
    : null;

  return (
    <Container as="main" className="py-12">
      <PageHeader>
        <PageHeaderContent size="lg">
          <Eyebrow>Account</Eyebrow>
          <PageHeaderTitle>Settings</PageHeaderTitle>
          <PageHeaderDescription>
            Manage authorization for your account — personal API tokens and
            shared service identities.
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <section
        id="authorization"
        className="mt-12 rounded-2xl border border-border bg-card p-6"
      >
        <SettingsSectionIntro
          eyebrow="Authorization"
          title="Your API Tokens"
          description="Create personal tokens for local tooling, scripts, and one-off integrations. These act as you."
        />

        <div className="mt-8">
          <div className="rounded-xl border border-border bg-background p-5">
            <TokenCreateForm />
          </div>
        </div>

        {tokensError && <p className="mt-4 text-sm text-destructive">{tokensError}</p>}

        {tokensQuery.isPending ? (
          <p className="mt-10 text-sm text-muted-foreground-soft">Loading...</p>
        ) : !tokensError ? (
          <div className="mt-8">
            <TokenTable tokens={tokens} />
          </div>
        ) : null}
      </section>

      <section className="mt-6 rounded-2xl border border-border bg-card p-6">
        <SettingsSectionIntro
          eyebrow="Authorization"
          title="Managed Identities"
          description="Create shared service-account subjects, grant app roles, and mint subject-owned API tokens for automation."
        />
        <Link
          to="/identities"
          className="mt-6 inline-flex rounded-md border border-border px-4 py-2 text-sm font-medium text-foreground transition-colors duration-150 hover:border-border hover:bg-muted"
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
    <div>
      <span className="label-text">{eyebrow}</span>
      <h2 className="mt-2 text-xl font-heading text-foreground">{title}</h2>
      <p className="mt-2 max-w-3xl text-sm text-muted-foreground">{description}</p>
    </div>
  );
}
