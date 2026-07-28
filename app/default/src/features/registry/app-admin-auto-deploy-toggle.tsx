import {
  Field,
  FieldContent,
  FieldDescription,
  FieldLabel,
} from "@/components/ui/field";
import { Switch } from "@/components/ui/switch";
import type { AppAdminAutoDeploy } from "@/features/registry/types";
import { cn } from "@/lib/cn";
import { Loader2 } from "lucide-react";

export function AppAdminAutoDeployToggle({
  autoDeploy,
  disabled,
  updating,
  updateError,
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
  const enabled = autoDeploy.enabled;

  return (
    <section
      className="space-y-3 rounded-2xl border border-border bg-card p-6 text-card-foreground"
      data-testid="app-admin-auto-deploy"
    >
      <Field orientation="horizontal">
        <FieldContent>
          <FieldLabel htmlFor={toggleId}>Automatically deploy new snapshots</FieldLabel>
          <FieldDescription>
            {enabled
              ? "New published snapshots are admitted across the fleet automatically. Turn off to deploy manually."
              : "Admit the newest published snapshot across the fleet without a manual deploy."}
          </FieldDescription>
        </FieldContent>
        <div className="flex shrink-0 items-center gap-2 self-start">
          <span
            className={cn(
              "min-w-[1.75rem] text-right text-sm font-medium tabular-nums",
              enabled ? "text-foreground" : "text-muted-foreground",
            )}
            data-testid="auto-deploy-state"
            aria-hidden="true"
          >
            {enabled ? "On" : "Off"}
          </span>
          <Switch
            id={toggleId}
            checked={enabled}
            disabled={disabled || updating}
            onCheckedChange={onChange}
            data-testid="auto-deploy-toggle"
            aria-label="Automatically deploy new snapshots"
          />
          {updating ? (
            <Loader2
              className="size-4 shrink-0 animate-spin text-muted-foreground"
              aria-hidden="true"
              data-testid="auto-deploy-toggle-spinner"
            />
          ) : null}
        </div>
      </Field>

      {updateError ? (
        <p className="text-sm text-destructive" data-testid="auto-deploy-update-error">
          {updateError}
        </p>
      ) : null}

      {lastError ? (
        <p className="text-sm text-destructive" data-testid="auto-deploy-last-error">
          {lastError}
        </p>
      ) : null}
    </section>
  );
}
