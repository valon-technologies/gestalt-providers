import { useEffect, useRef } from "react";
import type { MouseEvent, SyntheticEvent } from "react";
import type { Integration } from "@/lib/api";
import {
  badgeVariantFromTone,
  getAppSurfaces,
  primaryConnectLabel,
} from "@/lib/catalogFilters";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import { normalizeIntegrationStatus } from "@/lib/integrationStatus";
import { Badge } from "@/components/Badge";
import Button from "@/components/Button";
import IntegrationIcon from "@/components/IntegrationIcon";
import { CloseIcon } from "@/components/icons";

type AppListingDetailProps = {
  integration: Integration;
  onClose: () => void;
  onConnect: () => void;
  onOpenApp: () => void;
  readOnly?: boolean;
};

export default function AppListingDetail({
  integration,
  onClose,
  onConnect,
  onOpenApp,
  readOnly = false,
}: AppListingDetailProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const label = getIntegrationLabel(integration);
  const headingId = `app-listing-heading-${integration.name}`;
  const status = normalizeIntegrationStatus(integration);
  const surfaces = getAppSurfaces(integration);
  const connectLabel = primaryConnectLabel(integration);
  const mountedPath = integration.mountedPath?.trim();
  const showOpenApp = status.connected || !connectLabel;

  useEffect(() => {
    dialogRef.current?.showModal();
  }, []);

  function handleCancel(e: SyntheticEvent<HTMLDialogElement>) {
    e.preventDefault();
    onClose();
  }

  function handleBackdropClick(e: MouseEvent<HTMLDialogElement>) {
    if (e.target === e.currentTarget) {
      onClose();
    }
  }

  return (
    <dialog
      ref={dialogRef}
      onClose={onClose}
      onCancel={handleCancel}
      onClick={handleBackdropClick}
      aria-labelledby={headingId}
      className="fixed inset-0 z-50 m-auto w-[min(100%-2rem,28rem)] max-h-[min(100%-2rem,36rem)] overflow-y-auto rounded-xl border border-alpha bg-base-white p-0 text-foreground shadow-xl backdrop:bg-base-950/40 dark:bg-surface"
      data-testid={`app-listing-detail-${integration.name}`}
    >
      <div className="flex items-start justify-between gap-3 border-b border-alpha px-5 py-4">
        <div className="flex min-w-0 items-start gap-3">
          <IntegrationIcon iconSvg={integration.iconSvg} />
          <div className="min-w-0">
            <h2 id={headingId} className="text-lg font-heading text-foreground">
              {label}
            </h2>
            <div className="mt-2 flex flex-wrap items-center gap-1.5">
              <Badge
                size="sm"
                variant={badgeVariantFromTone(status.tone)}
                aria-label={status.summaryLabel}
              >
                {status.summaryLabel}
              </Badge>
              {surfaces.hasUi ? (
                <Badge size="sm" variant="secondary">
                  App page
                </Badge>
              ) : null}
            </div>
          </div>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-faint transition-colors hover:bg-alpha-5 hover:text-muted-foreground"
          aria-label={`Close ${label} details`}
        >
          <CloseIcon className="h-4 w-4" />
        </button>
      </div>

      <div className="space-y-4 px-5 py-4">
        {integration.description ? (
          <p className="text-sm text-muted-foreground">{integration.description}</p>
        ) : (
          <p className="text-sm text-faint">No description provided.</p>
        )}

        {mountedPath ? (
          <p className="text-sm text-muted-foreground">
            Has an app page. Open it from App admin after you connect.
          </p>
        ) : null}

        <p className="text-xs text-faint">
          Connecting lets this workspace use {label} with your credentials. You
          can disconnect anytime from settings.
        </p>
      </div>

      <div className="flex flex-wrap items-center justify-end gap-2 border-t border-alpha px-5 py-4">
        {showOpenApp ? (
          <Button type="button" variant="secondary" onClick={onOpenApp}>
            Open app
          </Button>
        ) : (
          <button
            type="button"
            onClick={onOpenApp}
            className="mr-auto text-sm text-muted-foreground transition-colors hover:text-foreground"
          >
            Open App admin
          </button>
        )}
        {!readOnly && connectLabel ? (
          <Button type="button" onClick={onConnect}>
            {connectLabel}
          </Button>
        ) : null}
      </div>
    </dialog>
  );
}
