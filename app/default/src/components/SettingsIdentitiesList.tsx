import { useState } from "react";
import { Link, useNavigate } from "@tanstack/react-router";
import IdentitySummaryCard from "@/components/IdentitySummaryCard";
import { SpinnerIcon } from "@/components/icons";
import { Button } from "@/components/ui/button";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
  InputGroupText,
} from "@/components/ui/input-group";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { managedIdentityLocalId } from "@/lib/managed-identity-paths";
import {
  useAuthInfoQuery,
  useCreateManagedIdentityMutation,
  useManagedIdentitiesQuery,
} from "@/lib/queries";

function managedIdentityLocalIdFromName(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 128);
}

function IdentitiesSectionHeader({
  description,
}: {
  description: string;
}) {
  return (
    <PageHeader>
      <PageHeaderContent size="md">
        <PageHeaderTitle>Managed identities</PageHeaderTitle>
        <PageHeaderDescription>{description}</PageHeaderDescription>
      </PageHeaderContent>
    </PageHeader>
  );
}

export default function SettingsIdentitiesList() {
  const navigate = useNavigate();
  const authInfoQuery = useAuthInfoQuery();
  const identitiesAvailable =
    authInfoQuery.data === undefined
      ? authInfoQuery.isPending
        ? null
        : true
      : authInfoQuery.data.provider !== "none";
  const identitiesQuery = useManagedIdentitiesQuery(identitiesAvailable === true);
  const createIdentity = useCreateManagedIdentityMutation();
  const [displayNameInput, setDisplayNameInput] = useState("");
  const [identityLocalID, setIdentityLocalID] = useState("");
  const [identityIDEdited, setIdentityIDEdited] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  const identities = identitiesQuery.data ?? [];
  const loading = identitiesQuery.isPending;
  const error = identitiesQuery.error
    ? identitiesQuery.error instanceof Error
      ? identitiesQuery.error.message
      : "Failed to load identities"
    : null;

  async function handleCreate(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const fd = new FormData(form);
    const displayName = (fd.get("displayName") as string)?.trim();
    const id = (fd.get("identityID") as string)?.trim();
    if (!displayName || !id) return;

    setCreateError(null);
    try {
      const identity = await createIdentity.mutateAsync({ id, displayName });
      await navigate({
        to: "/settings/identities/$identityLocalId",
        params: { identityLocalId: managedIdentityLocalId(identity.subjectId) },
      });
    } catch (err) {
      setCreateError(
        err instanceof Error ? err.message : "Failed to create identity",
      );
    }
  }

  if (identitiesAvailable === null) {
    return (
      <p className="flex items-center gap-1.5 text-sm text-muted-foreground">
        <SpinnerIcon className="size-4 animate-spin" aria-hidden />
        Loading…
      </p>
    );
  }

  if (identitiesAvailable === false) {
    return (
      <section className="scroll-mt-24 space-y-4" aria-label="Managed identities">
        <IdentitiesSectionHeader description="Managed identities require platform auth and are unavailable when auth is disabled." />
        <Link
          to="/apps"
          className="inline-flex text-sm text-muted-foreground transition-colors duration-150 hover:text-foreground"
        >
          &larr; Back to apps
        </Link>
      </section>
    );
  }

  return (
    <section className="scroll-mt-24 space-y-8" aria-label="Managed identities">
      <IdentitiesSectionHeader description="Create shared service-account subjects and grant app roles for automation." />

      <form
        onSubmit={handleCreate}
        className="rounded-xl border border-border bg-card p-5 text-card-foreground"
      >
        <FieldGroup className="gap-5">
          <Field>
            <FieldLabel htmlFor="identity-display-name">Display name</FieldLabel>
            <FieldContent className="max-w-[50%]">
              <InputGroup>
                <InputGroupInput
                  id="identity-display-name"
                  name="displayName"
                  type="text"
                  required
                  placeholder="e.g. Release Bot"
                  value={displayNameInput}
                  onChange={(event) => {
                    const nextValue = event.target.value;
                    setDisplayNameInput(nextValue);
                    if (!identityIDEdited) {
                      setIdentityLocalID(managedIdentityLocalIdFromName(nextValue));
                    }
                  }}
                  autoComplete="off"
                />
              </InputGroup>
            </FieldContent>
          </Field>

          <Field>
            <FieldLabel htmlFor="identity-id">Identity ID</FieldLabel>
            <FieldContent className="max-w-[50%]">
              <InputGroup>
                <InputGroupAddon align="inline-start">
                  <InputGroupText className="font-mono">
                    service_account:
                  </InputGroupText>
                </InputGroupAddon>
                <InputGroupInput
                  id="identity-id"
                  name="identityID"
                  type="text"
                  required
                  pattern="[A-Za-z0-9._-]{1,128}"
                  placeholder="release-bot"
                  value={identityLocalID}
                  onChange={(event) => {
                    setIdentityIDEdited(true);
                    setIdentityLocalID(event.target.value);
                  }}
                  autoComplete="off"
                  className="font-mono"
                />
              </InputGroup>
              <FieldDescription>
                Letters, numbers, dots, underscores, and hyphens.
              </FieldDescription>
            </FieldContent>
          </Field>
        </FieldGroup>

        {createError ? (
          <p className="mt-4 text-sm text-destructive">{createError}</p>
        ) : null}

        <Button type="submit" className="mt-5" disabled={createIdentity.isPending}>
          {createIdentity.isPending ? "Creating..." : "Create identity"}
        </Button>
      </form>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}

      {loading ? (
        <p className="flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading identities…
        </p>
      ) : !error && identities.length === 0 ? (
        <p className="text-sm text-muted-foreground">No managed identities yet.</p>
      ) : !error ? (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {identities.map((identity) => (
            <IdentitySummaryCard key={identity.id} identity={identity} />
          ))}
        </div>
      ) : null}
    </section>
  );
}
