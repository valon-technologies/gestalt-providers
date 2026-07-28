import {
  Field,
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field";
import { Switch } from "@/components/ui/switch";
import type { AppAdminAutoDeploy } from "@/features/registry/types";
import { Loader2 } from "lucide-react";

const SECTION_CARD =
  "rounded-lg border border-alpha bg-base-white p-6 dark:bg-surface";

export function AppAdminAutoDeployToggle({
  autoDeploy,
  disabled,
  updating,
  updateError = null,
  onChange,
}: {
  autoDeploy: AppAdminAutoDeploy;
  disabled?: boolean;
  updating?: boolean;
  updateError?: string | null;
  onChange: (enabled: boolean) => void;
}) {
  const toggleId = "app-admin-auto-deploy-toggle";
  const lastError = autoDeploy.lastError?.trim();
  const mutationError = updateError?.trim();

  return (
    <section
      className={SECTION_CARD}
      data-testid="app-admin-auto-deploy"
      aria-label="Automatic deployment"
    >
      <FieldGroup>
        <Field orientation="horizontal">
          <FieldContent>
            <FieldLabel htmlFor={toggleId}>Automatically deploy new snapshots</FieldLabel>
            <FieldDescription>
              Admit the newest published snapshot across the fleet without a manual deploy.
            </FieldDescription>
          </FieldContent>
          <div className="flex shrink-0 items-center gap-2 self-center">
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
          </div>
        </Field>
      </FieldGroup>

      {mutationError ? (
        <p className="mt-4 text-sm text-destructive" data-testid="auto-deploy-update-error">
          {mutationError}
        </p>
      ) : null}

      {lastError ? (
        <p className="mt-4 text-sm text-destructive" data-testid="auto-deploy-last-error">
          {lastError}
        </p>
      ) : null}
    </section>
  );
}
