import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { CopyableCode } from "@/components/ui/copyable-code";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { Switch } from "@/components/ui/switch";
import type { AppAdminAutoDeploy } from "@/features/registry/types";
import { SpinnerIcon } from "@/components/icons";
import { CircleAlert } from "lucide-react";

const ROLLOUT_LAST_ERROR_PATTERN = /^rollout for (.+) failed$/i;

const SNAPSHOT_CHIP_CLASS =
  "inline-flex max-w-full align-baseline text-xs [&_code]:text-xs";

function parseRolloutLastError(message: string): string | null {
  const match = ROLLOUT_LAST_ERROR_PATTERN.exec(message.trim());
  return match?.[1]?.trim() ?? null;
}

function VersionChip({ version }: { version: string }) {
  return (
    <CopyableCode
      value={version}
      className={SNAPSHOT_CHIP_CLASS}
      tooltip="Copy version"
    >
      {version}
    </CopyableCode>
  );
}

export function AppAdminAutoDeployToggle({
  autoDeploy,
  title,
  description,
  adjacentHint = null,
  disabled,
  updating,
  updateError = null,
  onChange,
}: {
  autoDeploy: AppAdminAutoDeploy;
  title: string;
  description: string | null;
  /** Table-level hint when automatic mode hides manual Deploy (shown in/near the card). */
  adjacentHint?: string | null;
  disabled?: boolean;
  updating?: boolean;
  updateError?: string | null;
  onChange: (enabled: boolean) => void;
}) {
  const toggleId = "app-admin-auto-deploy-toggle";
  const lastError = autoDeploy.lastError?.trim();
  const rolloutFailedVersion = lastError ? parseRolloutLastError(lastError) : null;
  const mutationError = updateError?.trim();
  const cardDescription = description?.trim() || null;
  const blockedHint = adjacentHint?.trim() || null;
  const descriptionInCard = cardDescription ?? blockedHint;
  const descriptionTestId = cardDescription
    ? "auto-deploy-toggle-description"
    : blockedHint
      ? "manual-deploy-blocked-reason"
      : undefined;
  const extraHint =
    cardDescription && blockedHint ? blockedHint : null;
  const controlDisabled = Boolean(disabled || updating);

  return (
    <div className="space-y-3">
      <FieldGroup
        className="max-w-lg"
        data-testid="app-admin-auto-deploy"
        aria-label="Automatic deployment"
      >
        <FieldLabel
          htmlFor={toggleId}
          data-disabled={controlDisabled ? true : undefined}
        >
          <Field
            orientation="horizontal"
            data-disabled={controlDisabled ? true : undefined}
          >
            <FieldContent>
              <FieldTitle>{title}</FieldTitle>
              {descriptionInCard ? (
                <FieldDescription data-testid={descriptionTestId}>
                  {descriptionInCard}
                </FieldDescription>
              ) : null}
            </FieldContent>
            <div className="flex shrink-0 items-start gap-2">
              {updating ? (
                <span data-testid="auto-deploy-toggle-spinner" aria-hidden>
                  <SpinnerIcon className="mt-0.5 size-4 animate-spin text-muted-foreground" />
                </span>
              ) : null}
              <Switch
                id={toggleId}
                checked={autoDeploy.enabled}
                disabled={controlDisabled}
                onCheckedChange={onChange}
                data-testid="auto-deploy-toggle"
              />
            </div>
          </Field>
        </FieldLabel>
      </FieldGroup>

      {extraHint ? (
        <p
          className="max-w-lg text-pretty text-sm text-muted-foreground"
          data-testid="manual-deploy-blocked-reason"
        >
          {extraHint}
        </p>
      ) : null}

      {mutationError ? (
        <Alert variant="destructive" data-testid="auto-deploy-update-error">
          <CircleAlert aria-hidden="true" />
          <AlertDescription>{mutationError}</AlertDescription>
        </Alert>
      ) : null}

      {lastError ? (
        <Alert variant="destructive" data-testid="auto-deploy-last-error">
          <CircleAlert aria-hidden="true" />
          {rolloutFailedVersion ? (
            <>
              <AlertTitle>Automatic deploy failed</AlertTitle>
              <AlertDescription className="space-y-2">
                <p>
                  Couldn&apos;t roll out{" "}
                  <VersionChip version={rolloutFailedVersion} />
                  to the fleet.
                </p>
                {autoDeploy.enabled ? (
                  <>
                    <p>
                      Automatic deploy is still on — it was not turned off by this
                      failure.
                    </p>
                    <p>
                      Use Retry deploy on the failed version in the table below.
                    </p>
                  </>
                ) : null}
              </AlertDescription>
            </>
          ) : (
            <AlertDescription>
              Automatic deploy failed. Try Retry deploy in the table below, or refresh
              and try again.
            </AlertDescription>
          )}
        </Alert>
      ) : null}
    </div>
  );
}
