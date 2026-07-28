import { Checkbox } from "@/components/ui/checkbox";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldLabel,
} from "@/components/ui/field";
import type { AppAdminAutoDeploy } from "@/features/registry/types";
import { Loader2 } from "lucide-react";

export function AppAdminAutoDeployToggle({
  autoDeploy,
  disabled,
  updating,
  onChange,
}: {
  autoDeploy: AppAdminAutoDeploy;
  disabled?: boolean;
  updating?: boolean;
  onChange: (enabled: boolean) => void;
}) {
  const toggleId = "app-admin-auto-deploy-toggle";
  const lastError = autoDeploy.lastError?.trim();

  return (
    <section
      className="space-y-3 rounded-2xl border border-border bg-card p-6 text-card-foreground"
      data-testid="app-admin-auto-deploy"
    >
      <Field orientation="horizontal">
        <Checkbox
          id={toggleId}
          checked={autoDeploy.enabled}
          disabled={disabled || updating}
          onCheckedChange={(checked: boolean | "indeterminate") => onChange(checked === true)}
          data-testid="auto-deploy-toggle"
        />
        <FieldContent>
          <FieldLabel htmlFor={toggleId}>Automatically deploy new snapshots</FieldLabel>
          <FieldDescription>
            Admit the newest published snapshot across the fleet without a manual deploy.
          </FieldDescription>
        </FieldContent>
        {updating ? (
          <Loader2
            className="size-4 shrink-0 animate-spin text-muted-foreground"
            aria-hidden="true"
            data-testid="auto-deploy-toggle-spinner"
          />
        ) : null}
      </Field>

      {lastError ? (
        <p className="text-sm text-destructive" data-testid="auto-deploy-last-error">
          {lastError}
        </p>
      ) : null}
    </section>
  );
}
