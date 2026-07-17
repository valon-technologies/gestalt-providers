import { useEffect, useRef, useState } from "react";
import { Link } from "@tanstack/react-router";
import { getTokens, type APIToken } from "@/lib/api";
import Container from "@/components/Container";
import TokenCreateForm from "@/components/TokenCreateForm";
import TokenTable from "@/components/TokenTable";

export default function SettingsPageClient() {
  const [tokens, setTokens] = useState<APIToken[]>([]);
  const [tokensLoading, setTokensLoading] = useState(true);
  const [tokensError, setTokensError] = useState<string | null>(null);

  const tokenLoadRequestIdRef = useRef(0);

  async function loadTokens() {
    const requestID = tokenLoadRequestIdRef.current + 1;
    tokenLoadRequestIdRef.current = requestID;

    try {
      const nextTokens = await getTokens();
      if (tokenLoadRequestIdRef.current !== requestID) return;
      setTokens(nextTokens);
      setTokensError(null);
    } catch (err) {
      if (tokenLoadRequestIdRef.current !== requestID) return;
      setTokensError(err instanceof Error ? err.message : "Failed to load tokens");
    } finally {
      if (tokenLoadRequestIdRef.current === requestID) {
        setTokensLoading(false);
      }
    }
  }

  useEffect(() => {
    void loadTokens();
  }, []);

  return (
    <Container as="main" className="py-12">
      <div>
        <span className="label-text">Account</span>
        <h1 className="mt-2 text-2xl font-heading text-primary">Settings</h1>
        <p className="mt-3 max-w-3xl text-sm text-muted">
          Manage authorization for your account — personal API tokens and
          shared service identities.
        </p>
      </div>

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
            <TokenCreateForm onCreated={loadTokens} />
          </div>
        </div>

        {tokensError && <p className="mt-4 text-sm text-ember-500">{tokensError}</p>}

        {tokensLoading ? (
          <p className="mt-10 text-sm text-faint">Loading...</p>
        ) : !tokensError ? (
          <div className="mt-8">
            <TokenTable tokens={tokens} onRevoked={loadTokens} />
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
          className="mt-6 inline-flex rounded-md border border-alpha px-4 py-2 text-sm font-medium text-primary transition-colors duration-150 hover:border-alpha-strong hover:bg-base-100 dark:hover:bg-surface-raised"
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
      <h2 className="mt-2 text-xl font-heading text-primary">{title}</h2>
      <p className="mt-2 max-w-3xl text-sm text-muted">{description}</p>
    </div>
  );
}
