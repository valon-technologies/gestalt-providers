import { useCallback, useState } from "react";
import {
  readActiveExemplarId,
  readIntroSeenFlag,
  readMcpInstalledFlag,
  readTrySeenFlag,
  readStoredApiToken,
  readStoredApiTokenGrantId,
  readStoredSelectedTokenId,
  readStoredTokenName,
  readStoredInstallAgent,
  sessionApiTokenBoundToSelection,
  writeActiveExemplarId,
  writeIntroSeenFlag,
  writeMcpInstalledFlag,
  writeTrySeenFlag,
  writeStoredApiToken,
  writeStoredApiTokenGrantId,
  writeStoredSelectedTokenId,
  writeStoredTokenName,
  writeStoredInstallAgent,
  type BuildExemplarId,
  type BuildInstallAgentId,
} from "@/lib/buildPaths";

export type BuildSession = {
  apiToken: string;
  apiTokenGrantId: string;
  setApiToken: (token: string, grantId?: string) => void;
  tokenName: string;
  setTokenName: (name: string) => void;
  selectedTokenId: string;
  setSelectedTokenId: (id: string) => void;
  selectedInstallAgent: BuildInstallAgentId | "";
  setSelectedInstallAgent: (id: BuildInstallAgentId | "") => void;
  mcpInstalled: boolean;
  markMcpInstalled: () => void;
  activeExemplarId: BuildExemplarId;
  setActiveExemplarId: (id: BuildExemplarId) => void;
  welcomeSeen: boolean;
  markWelcomeSeen: () => void;
  trySeen: boolean;
  markTrySeen: () => void;
};

/** Client session for the Setup journey — survives step navigation via sessionStorage. */
export function useBuildSession(): BuildSession {
  const [apiToken, setApiTokenState] = useState(readStoredApiToken);
  const [apiTokenGrantId, setApiTokenGrantIdState] = useState(
    readStoredApiTokenGrantId,
  );
  const [tokenName, setTokenNameState] = useState(readStoredTokenName);
  const [selectedTokenId, setSelectedTokenIdState] = useState(
    readStoredSelectedTokenId,
  );
  const [selectedInstallAgent, setSelectedInstallAgentState] = useState<
    BuildInstallAgentId | ""
  >(readStoredInstallAgent);
  const [mcpInstalled, setMcpInstalled] = useState(readMcpInstalledFlag);
  const [activeExemplarId, setActiveExemplarIdState] =
    useState(readActiveExemplarId);
  const [welcomeSeen, setWelcomeSeen] = useState(readIntroSeenFlag);
  const [trySeen, setTrySeen] = useState(readTrySeenFlag);

  const setApiToken = useCallback((token: string, grantId?: string) => {
    const trimmed = token.trim();
    writeStoredApiToken(trimmed);
    if (grantId) {
      writeStoredApiTokenGrantId(grantId);
      setApiTokenGrantIdState(grantId);
    } else if (!trimmed) {
      writeStoredApiTokenGrantId("");
      setApiTokenGrantIdState("");
    }
    setApiTokenState(trimmed);
  }, []);

  const clearApiTokenUnlessGrant = useCallback((grantId: string) => {
    if (sessionApiTokenBoundToSelection(readStoredApiTokenGrantId(), grantId)) {
      return;
    }
    if (!readStoredApiToken() && !readStoredApiTokenGrantId()) {
      return;
    }
    writeStoredApiToken("");
    writeStoredApiTokenGrantId("");
    setApiTokenState("");
    setApiTokenGrantIdState("");
  }, []);

  const setTokenName = useCallback((name: string) => {
    writeStoredTokenName(name);
    setTokenNameState(name);
  }, []);

  const setSelectedTokenId = useCallback((id: string) => {
    clearApiTokenUnlessGrant(id);
    writeStoredSelectedTokenId(id);
    setSelectedTokenIdState(id);
  }, [clearApiTokenUnlessGrant]);

  const setSelectedInstallAgent = useCallback((id: BuildInstallAgentId | "") => {
    writeStoredInstallAgent(id);
    setSelectedInstallAgentState(id);
  }, []);

  const markMcpInstalled = useCallback(() => {
    writeMcpInstalledFlag(true);
    setMcpInstalled(true);
  }, []);

  const setActiveExemplarId = useCallback((id: BuildExemplarId) => {
    writeActiveExemplarId(id);
    setActiveExemplarIdState(id);
  }, []);

  const markWelcomeSeen = useCallback(() => {
    writeIntroSeenFlag(true);
    setWelcomeSeen(true);
  }, []);

  const markTrySeen = useCallback(() => {
    writeTrySeenFlag(true);
    setTrySeen(true);
  }, []);

  return {
    apiToken,
    apiTokenGrantId,
    setApiToken,
    tokenName,
    setTokenName,
    selectedTokenId,
    setSelectedTokenId,
    selectedInstallAgent,
    setSelectedInstallAgent,
    mcpInstalled,
    markMcpInstalled,
    activeExemplarId,
    setActiveExemplarId,
    welcomeSeen,
    markWelcomeSeen,
    trySeen,
    markTrySeen,
  };
}
