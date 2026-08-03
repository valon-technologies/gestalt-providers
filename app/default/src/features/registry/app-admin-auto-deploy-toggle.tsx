import {
  Alert,
  AlertActions,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { CopyableCode } from "@/components/ui/copyable-code";
import { Switch } from "@/components/ui/switch";
import type { AppAdminAutoDeploy } from "@/features/registry/types";
import { CircleAlert, Loader2 } from "lucide-react";

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
  disabled,
  updating,
  updateError = null,
  onChange,
}: {
  autoDeploy: AppAdminAutoDeploy;
  title: string;
  description: string | null;
  disabled?: boolean;
  updating?: boolean;
  updateError?: string | null;
  onChange: (enabled: boolean) => void;
}) {
  const toggleId = "app-admin-auto-deploy-toggle";
  const lastError = autoDeploy.lastError?.trim();
  const rolloutFailedVersion = lastError ? parseRolloutLastError(lastError) : null;
  const mutationError = updateError?.trim();

  return (
    <div className="space-y-2">
      <Alert
        variant="default"
        layout="banner"
        data-testid="app-admin-auto-deploy"
        aria-label="Automatic deployment"
      >
        <div className="min-w-0 grow basis-64 space-y-0.5">
          <AlertTitle className="line-clamp-none">
            <label htmlFor={toggleId} className="cursor-pointer">
              {title}
            </label>
          </AlertTitle>
          {description ? <AlertDescription>{description}</AlertDescription> : null}
        </div>
        <AlertActions className="self-center">
          {updating ? (
            <Loader2
              className="size-4 animate-spin text-muted-foreground"
              aria-hidden="true"
              data-testid="auto-deploy-toggle-spinner"
            />
          ) : null}
          <Switch
            id={toggleId}
            checked={autoDeploy.enabled}
            disabled={disabled || updating}
            onCheckedChange={onChange}
            data-testid="auto-deploy-toggle"
          />
        </AlertActions>
      </Alert>

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
