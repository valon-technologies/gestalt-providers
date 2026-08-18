import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { toast } from "sonner";
import {
  PENDING_CONNECTION_PATH,
  resolveAPIPath,
  startIntegrationOAuth,
  connectManualIntegration,
  disconnectIntegration,
  selectPreferredInstance,
  isAPIErrorStatus,
} from "@/lib/api";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import type { Integration } from "@/lib/api";
import { rememberConnectionReturnPath, sanitizeAuthReturnPath } from "@/lib/authReturn";
import {
  appIsConnectedCopy,
  oauthConnectedToastMessage,
  refetchIntegrationConnected,
} from "@/lib/oauthConnectConfirm";
import {
  closeOAuthPopup,
  navigateOAuthPopup,
  openOAuthPopup,
  watchOAuthPopup,
} from "@/lib/oauthPopup";
import { commitIntegrationDisconnect } from "@/lib/queries";
import { userFacingError } from "@/lib/user-facing-error";

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

type SelectInstanceFn = (
  integration: string,
  instance: string,
  connection?: string,
) => Promise<unknown>;

export function useIntegrationConnection({
  integration,
  onConnected,
  onDisconnected,
  startOAuth = startIntegrationOAuth,
  connectManual = connectManualIntegration,
  disconnect = disconnectIntegration,
  selectInstance = selectPreferredInstance,
  returnPath,
  onFlowComplete,
}: {
  integration: Integration;
  onConnected?: () => void;
  onDisconnected?: () => void;
  startOAuth?: StartOAuthFn;
  connectManual?: ConnectManualFn;
  disconnect?: DisconnectFn;
  selectInstance?: SelectInstanceFn;
  returnPath?: string;
  /** Called after connect/disconnect completes (e.g. close modal). */
  onFlowComplete?: () => void;
}) {
  const label = getIntegrationLabel(integration);
  const queryClient = useQueryClient();
  const [loading, setLoading] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [selectingInstance, setSelectingInstance] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [pendingSelection, setPendingSelection] =
    useState<PendingSelection | null>(null);
  const pendingSelectionFormRef = useRef<HTMLFormElement>(null);
  const stopWatchingOAuthPopupRef = useRef<(() => void) | null>(null);
  const onConnectedRef = useRef(onConnected);
  const onFlowCompleteRef = useRef(onFlowComplete);
  onConnectedRef.current = onConnected;
  onFlowCompleteRef.current = onFlowComplete;

  useEffect(() => {
    if (!pendingSelection) return;
    pendingSelectionFormRef.current?.submit();
  }, [pendingSelection]);

  useEffect(
    () => () => {
      stopWatchingOAuthPopupRef.current?.();
    },
    [],
  );

  async function finishOAuthPopup() {
    let connected = false;
    try {
      connected = await refetchIntegrationConnected(
        queryClient,
        integration.name,
      );
    } catch (err) {
      setLoading(false);
      setError(
        userFacingError(
          err,
          `Couldn't confirm ${label} is connected. Try again.`,
          "connect",
        ),
      );
      return;
    }
    setLoading(false);
    const toastMessage = oauthConnectedToastMessage(connected, label);
    if (toastMessage) {
      toast.success(toastMessage);
    }
    onConnectedRef.current?.();
    onFlowCompleteRef.current?.();
  }

  async function handleStartOAuth(
    instance?: string,
    connection?: string,
    connectionParams?: Record<string, string>,
  ): Promise<boolean> {
    const popup = openOAuthPopup();
    setLoading(true);
    setError(null);
    try {
      const oauthReturnPath =
        returnPath === undefined ? undefined : sanitizeAuthReturnPath(returnPath);
      const { url } = await startOAuth(
        integration.name,
        undefined,
        connectionParams,
        instance,
        connection,
        oauthReturnPath,
      );
      rememberConnectionReturnPath(oauthReturnPath);
      if (popup && !popup.closed) {
        navigateOAuthPopup(popup, url);
        stopWatchingOAuthPopupRef.current?.();
        stopWatchingOAuthPopupRef.current = watchOAuthPopup(popup, () => {
          stopWatchingOAuthPopupRef.current = null;
          void finishOAuthPopup();
        });
        return true;
      }
      window.location.href = url;
      return true;
    } catch (err) {
      closeOAuthPopup(popup);
      setError(
        userFacingError(err, "Couldn't start sign-in. Try again.", "sign_in"),
      );
      setLoading(false);
      return false;
    }
  }

  async function handleSubmitToken(
    credential: string | Record<string, string>,
    connectionParams?: Record<string, string>,
    instance?: string,
    connection?: string,
  ): Promise<boolean> {
    setSubmitting(true);
    setError(null);
    try {
      const oauthReturnPath =
        returnPath === undefined ? undefined : sanitizeAuthReturnPath(returnPath);
      const result = await connectManual(
        integration.name,
        credential,
        connectionParams,
        instance,
        connection,
        oauthReturnPath,
      );
      if (result.status === "selection_required") {
        if (!result.pendingToken) {
          throw new Error("Connection setup is incomplete. Try again.");
        }
        rememberConnectionReturnPath(oauthReturnPath);
        onFlowComplete?.();
        setPendingSelection({
          action: resolveAPIPath(
            result.selectionUrl || PENDING_CONNECTION_PATH,
          ),
          pendingToken: result.pendingToken,
        });
      } else {
        onFlowComplete?.();
        toast.success(appIsConnectedCopy(label));
        onConnected?.();
      }
      return true;
    } catch (err) {
      setError(
        userFacingError(err, `Couldn't connect ${label}. Try again.`, "connect"),
      );
      return false;
    } finally {
      setSubmitting(false);
    }
  }

  async function commitDisconnect(instance?: string, connection?: string) {
    await commitIntegrationDisconnect(queryClient, integration.name, {
      instance,
      connection,
    });
    onDisconnected?.();
    onFlowComplete?.();
  }

  async function handleDisconnect(instance?: string, connection?: string) {
    setDisconnecting(true);
    setError(null);
    try {
      await disconnect(integration.name, instance, connection);
      await commitDisconnect(instance, connection);
      toast.success(`${label} disconnected.`);
    } catch (err) {
      // Domain: missing credential is already the desired end state — reconcile.
      if (isAPIErrorStatus(err, 404)) {
        await commitDisconnect(instance, connection);
        toast.success(`${label} is no longer connected.`);
      } else {
        setError(
          userFacingError(
            err,
            `Couldn't disconnect ${label}. Try again.`,
            "disconnect",
          ),
        );
      }
    } finally {
      setDisconnecting(false);
    }
  }

  async function handleSelectInstance(instance: string, connection?: string) {
    setSelectingInstance(true);
    setError(null);
    try {
      await selectInstance(integration.name, instance, connection);
      toast.success(`${label} account updated.`);
      onConnected?.();
      onFlowComplete?.();
    } catch (err) {
      setError(
        userFacingError(
          err,
          "Couldn't choose that account. Try again.",
          "select_instance",
        ),
      );
    } finally {
      setSelectingInstance(false);
    }
  }

  function clearError() {
    setError(null);
  }

  return {
    loading,
    disconnecting,
    selectingInstance,
    submitting,
    error,
    pendingSelection,
    pendingSelectionFormRef,
    handleStartOAuth,
    handleSubmitToken,
    handleDisconnect,
    handleSelectInstance,
    clearError,
  };
}
