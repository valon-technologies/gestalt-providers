import { useState } from "react";
import { Link } from "@tanstack/react-router";
import TokenCreateForm, {
  SETTINGS_TOKEN_CREATE_TRACK,
} from "@/components/TokenCreateForm";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SETTINGS_TOKEN_CREATE_CANCEL,
  SETTINGS_TOKEN_CREATE_DESCRIPTION,
  SETTINGS_TOKEN_CREATE_DOCUMENT_TITLE,
  SETTINGS_TOKEN_CREATE_DONE,
  SETTINGS_TOKEN_CREATE_TITLE,
  SETTINGS_TOKEN_CREATED_DESCRIPTION,
  SETTINGS_TOKEN_CREATED_TITLE,
  SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION,
} from "@/features/settings/tokens-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { cn } from "@/lib/cn";
import { SETTINGS_TOKENS_PATH } from "@/lib/managed-identity-paths";

/**
 * Settings create-token task page.
 *
 * Owns task chrome (title / cancel / done) and surface copy. The shared
 * TokenCreateForm owns minting + one-time secret revelation; navigation and
 * settings-native wording stay here so Build can reuse the form without
 * settings chrome.
 */
export default function SettingsTokenCreate() {
  useDocumentTitle(SETTINGS_TOKEN_CREATE_DOCUMENT_TITLE);
  const [phase, setPhase] = useState<"compose" | "reveal">("compose");
  const revealed = phase === "reveal";

  return (
    <section
      id="create-token"
      className="scroll-mt-24 space-y-8"
      aria-label={SETTINGS_TOKEN_CREATE_TITLE}
    >
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>
            {revealed ? SETTINGS_TOKEN_CREATED_TITLE : SETTINGS_TOKEN_CREATE_TITLE}
          </PageHeaderTitle>
          <PageHeaderDescription>
            {revealed
              ? SETTINGS_TOKEN_CREATED_DESCRIPTION
              : SETTINGS_TOKEN_CREATE_DESCRIPTION}
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <TokenCreateForm
        className={SETTINGS_TOKEN_CREATE_TRACK.form}
        controlsClassName={SETTINGS_TOKEN_CREATE_TRACK.controls}
        appAccessPanelClassName={SETTINGS_TOKEN_CREATE_TRACK.appAccessPanel}
        actionsClassName={SETTINGS_TOKEN_CREATE_TRACK.actions}
        plaintextResultDescription={SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION}
        onCreated={async () => {
          setPhase("reveal");
        }}
        plaintextResultActions={
          <Button asChild className="w-fit">
            <Link to={SETTINGS_TOKENS_PATH}>{SETTINGS_TOKEN_CREATE_DONE}</Link>
          </Button>
        }
        submitAccessory={
          revealed ? null : (
            <Link
              to={SETTINGS_TOKENS_PATH}
              className={cn(
                buttonVariants({ variant: "ghost" }),
                "shrink-0 text-muted-foreground",
              )}
            >
              {SETTINGS_TOKEN_CREATE_CANCEL}
            </Link>
          )
        }
      />
    </section>
  );
}
