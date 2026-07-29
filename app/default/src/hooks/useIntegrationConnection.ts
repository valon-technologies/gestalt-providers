import { useEffect, useRef, useState } from "react";
import {
  PENDING_CONNECTION_PATH,
  resolveAPIPath,
  startIntegrationOAuth,
  connectManualIntegration,
  disconnectIntegration,
} from "@/lib/api";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import type { Integration } from "@/lib/api";

type ConnectionTarget = {
  instance?: string;
  connection?: string;
};

type PendingSelection = {
  action: string;
  pendingToken: string;
};

type StartOAuthFn = (
  integration: string,
  scopes?: string[],
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
) => Promise<{ url: string; state: string }>;

type ConnectManualFn = (
  integration: string,
  credential: string | Record<string, string>,
  connectionParams?: Record<string, string>,
  instance?: string,
  connection?: string,
  returnPath?: string,
) => Promise<{
  status: string;
  integration?: string;
  selectionUrl?: string;
  pendingToken?: string;
}>;

type DisconnectFn = (
  integration: string,
  instance?: string,
  connection?: string,
) => Promise<void>;

export function useIntegrationConnection({
  integration,
  onConnected,
  onDisconnected,
  onStatusMessage,
  startOAuth = startIntegrationOAuth,
  connectManual = connectManualIntegration,
  disconnect = disconnectIntegration,
  returnPath,
  onFlowComplete,
}: {
  integration: Integration;
  onConnected?: () => void;
  onDisconnected?: () => void;
  onStatusMessage?: (message: string) => void;
  startOAuth?: StartOAuthFn;
  connectManual?: ConnectManualFn;
  disconnect?: DisconnectFn;
  returnPath?: string;
  /** Called after connect/disconnect completes (e.g. close modal). */
  onFlowComplete?: () => void;
}) {
  const label = getIntegrationLabel(integration);
  const [loading, setLoading] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [pendingOAuthTarget, setPendingOAuthTarget] = useState<ConnectionTarget>(
    {},
  );
  const [pendingSelection, setPendingSelection] =
    useState<PendingSelection | null>(null);
  const pendingSelectionFormRef = useRef<HTMLFormElement>(null);

  useEffect(() => {
    if (!pendingSelection) return;
    pendingSelectionFormRef.current?.submit();
  }, [pendingSelection]);

  async function beginOAuth(
    connectionParams?: Record<string, string>,
    target: ConnectionTarget = pendingOAuthTarget,
  ) {
    setLoading(true);
    setError(null);
    try {
      const { url } = await startOAuth(
        integration.name,
        undefined,
        connectionParams,
        target.instance,
        target.connection,
        returnPath,
      );
      window.location.href = url;
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Couldn't start sign-in. Try again.",
      );
      setLoading(false);
    }
  }

  async function handleStartOAuth(
    instance?: string,
    connection?: string,
    connectionParams?: Record<string, string>,
  ) {
    const target = { instance, connection };
    setPendingOAuthTarget(target);
    await beginOAuth(connectionParams, target);
  }

  async function handleSubmitToken(
    credential: string | Record<string, string>,
    connectionParams?: Record<string, string>,
    instance?: string,
    connection?: string,
  ) {
    setSubmitting(true);
    setError(null);
    try {
      const result = await connectManual(
        integration.name,
        credential,
        connectionParams,
        instance,
        connection,
        returnPath,
      );
      if (result.status === "selection_required") {
        if (!result.pendingToken) {
          throw new Error(
            "Connection requires selection, but the server did not return a pending token.",
          );
        }
        onFlowComplete?.();
        setPendingSelection({
          action: resolveAPIPath(
            result.selectionUrl || PENDING_CONNECTION_PATH,
          ),
          pendingToken: result.pendingToken,
        });
      } else {
        onFlowComplete?.();
        onStatusMessage?.(`${label} connected successfully.`);
        onConnected?.();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Couldn't connect. Try again.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDisconnect(instance?: string, connection?: string) {
    setDisconnecting(true);
    setError(null);
    try {
      await disconnect(integration.name, instance, connection);
      onStatusMessage?.(`${label} disconnected.`);
      onDisconnected?.();
      onFlowComplete?.();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Couldn't disconnect. Try again.",
      );
    } finally {
      setDisconnecting(false);
    }
  }

  function clearError() {
    setError(null);
  }

  return {
    loading,
    disconnecting,
    submitting,
    error,
    pendingSelection,
    pendingSelectionFormRef,
    handleStartOAuth,
    handleSubmitToken,
    handleDisconnect,
    clearError,
  };
}
