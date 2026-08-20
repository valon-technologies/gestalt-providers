import { useState } from "react";
import { Link } from "@tanstack/react-router";
import TokenCreateForm from "@/components/TokenCreateForm";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { DOCS_MCP_PATH } from "@/docs/docs-data";
import { SETTINGS_TOKEN_CREATE_TRACK } from "@/features/settings/token-create-layout";
import {
  SETTINGS_TOKEN_CREATE_CANCEL,
  SETTINGS_TOKEN_CREATE_CONTINUE,
  SETTINGS_TOKEN_CREATE_DESCRIPTION,
  SETTINGS_TOKEN_CREATE_DOCUMENT_TITLE,
  SETTINGS_TOKEN_CREATE_DONE,
  SETTINGS_TOKEN_CREATE_TITLE,
  SETTINGS_TOKEN_CREATED_DESCRIPTION,
  SETTINGS_TOKEN_CREATED_TITLE,
  SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION,
} from "@/features/settings/tokens-copy";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { SETTINGS_TOKENS_PATH } from "@/lib/managed-identity-paths";

/**
 * Settings create-token task page.
 *
 * Owns task chrome (title / cancel / continue to MCP Clients / back to the
 * token list) and surface copy. The shared TokenCreateForm owns minting +
 * one-time secret revelation; navigation and settings-native wording stay
 * here so Build can reuse the form without settings chrome.
 */
export default function SettingsTokenCreate() {
  useDocumentTitle(SETTINGS_TOKEN_CREATE_DOCUMENT_TITLE);
  const [revealed, setRevealed] = useState(false);

  return (
    <section
      id="create-token"
      className="scroll-mt-24 space-y-8"
      aria-label={SETTINGS_TOKEN_CREATE_TITLE}
    >
      <PageHeader>
        <PageHeaderContent size="lg">
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
        onRevealChange={setRevealed}
        plaintextResultActions={
          <div className="flex w-full flex-row flex-nowrap items-center justify-end gap-3">
            <Button
              variant="ghost"
              asChild
              className="shrink-0 text-muted-foreground"
            >
              <Link to={SETTINGS_TOKENS_PATH}>
                {SETTINGS_TOKEN_CREATE_DONE}
              </Link>
            </Button>
            <Button asChild className="w-fit">
              <Link to={DOCS_MCP_PATH}>{SETTINGS_TOKEN_CREATE_CONTINUE}</Link>
            </Button>
          </div>
        }
        submitAccessory={
          revealed ? null : (
            <Button
              variant="ghost"
              asChild
              className="shrink-0 text-muted-foreground"
            >
              <Link to={SETTINGS_TOKENS_PATH}>
                {SETTINGS_TOKEN_CREATE_CANCEL}
              </Link>
            </Button>
          )
        }
      />
    </section>
  );
}
