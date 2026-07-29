import type { Integration } from "@/lib/api";
import type { ConnectionContext } from "@/lib/integrationStatus";
import IntegrationConnectionPanel, {
  type ConnectionPanelView,
} from "./IntegrationConnectionPanel";

interface IntegrationSettingsModalProps {
  integration: Integration;
  onClose: () => void;
  onStartOAuth: (instance?: string, connection?: string) => void;
  onSubmitToken: (
    credential: string | Record<string, string>,
    connectionParams?: Record<string, string>,
    instance?: string,
    connection?: string,
  ) => void;
  onDisconnect: (instance?: string, connection?: string) => void;
  reconnecting: boolean;
  disconnecting: boolean;
  submitting: boolean;
  error: string | null;
  readOnly?: boolean;
  connectionContext?: ConnectionContext;
  initialView?: ConnectionPanelView;
  destructiveActionLabel?: "Disconnect" | "Remove app";
  presentation?: "modal" | "inline";
}

/** @deprecated Prefer IntegrationConnectionPanel — dialog wrapper for catalog/modal flows. */
export default function IntegrationSettingsModal({
  presentation = "modal",
  onClose,
  ...props
}: IntegrationSettingsModalProps) {
  return (
    <IntegrationConnectionPanel
      {...props}
      variant={presentation === "inline" ? "inline" : "dialog"}
      onClose={onClose}
      showHeader
    />
  );
}
