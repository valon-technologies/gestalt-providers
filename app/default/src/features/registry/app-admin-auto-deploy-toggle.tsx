import {
  Alert,
  AlertActions,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { Switch } from "@/components/ui/switch";
import type { AppAdminAutoDeploy } from "@/features/registry/types";
import { Loader2 } from "lucide-react";

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
    <Alert
      variant="default"
      layout="banner"
      data-testid="app-admin-auto-deploy"
      aria-label="Automatic deployment"
    >
      <div className="min-w-0 grow basis-64 space-y-0.5">
        <AlertTitle className="line-clamp-none">
          <label htmlFor={toggleId} className="cursor-pointer">
            Automatically deploy new snapshots
          </label>
        </AlertTitle>
        <AlertDescription>
          Admit the newest published snapshot across the fleet without a manual deploy.
          {mutationError ? (
            <p className="text-destructive" data-testid="auto-deploy-update-error">
              {mutationError}
            </p>
          ) : null}
          {lastError ? (
            <p className="text-destructive" data-testid="auto-deploy-last-error">
              {lastError}
            </p>
          ) : null}
        </AlertDescription>
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
  );
}
