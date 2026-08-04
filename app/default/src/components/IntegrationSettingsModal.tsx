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
  onSelectInstance?: (
    instance: string,
    connection?: string,
  ) => void | Promise<void>;
  reconnecting: boolean;
  disconnecting: boolean;
  selectingInstance?: boolean;
  submitting: boolean;
  error: string | null;
  onClearError?: () => void;
  readOnly?: boolean;
  connectionContext?: ConnectionContext;
  initialView?: ConnectionPanelView;
  destructiveActionLabel?: "Disconnect" | "Remove app";
  presentation?: "modal" | "inline";
  onDisconnectDialogClose?: () => void;
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
