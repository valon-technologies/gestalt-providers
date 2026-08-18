import { useCallback, useState } from "react";
import {
  addMcpInstalledAgent,
  readActiveExemplarId,
  readIntroSeenFlag,
  readMcpInstalledAgents,
  readTrySeenFlag,
  readStoredApiToken,
  readStoredApiTokenGrantId,
  readStoredTokenName,
  readStoredInstallAgent,
  writeActiveExemplarId,
  writeIntroSeenFlag,
  writeMcpInstalledAgents,
  writeTrySeenFlag,
  writeStoredApiToken,
  writeStoredApiTokenGrantId,
  writeStoredTokenName,
  writeStoredInstallAgent,
  type BuildExemplarId,
} from "@/lib/buildPaths";
import type { BuildInstallAgentId } from "@/lib/assistantHosts";

export type BuildSession = {
  apiToken: string;
  apiTokenGrantId: string;
  setApiToken: (token: string, grantId?: string) => void;
  tokenName: string;
  setTokenName: (name: string) => void;
  installAgentId: BuildInstallAgentId | "";
  setInstallAgentId: (id: BuildInstallAgentId | "") => void;
  mcpInstalledAgents: readonly BuildInstallAgentId[];
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
  const [installAgentId, setInstallAgentIdState] = useState<
    BuildInstallAgentId | ""
  >(readStoredInstallAgent);
  const [mcpInstalledAgents, setMcpInstalledAgents] = useState(
    readMcpInstalledAgents,
  );
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

  const setTokenName = useCallback((name: string) => {
    writeStoredTokenName(name);
    setTokenNameState(name);
  }, []);

  const setInstallAgentId = useCallback((id: BuildInstallAgentId | "") => {
    writeStoredInstallAgent(id);
    setInstallAgentIdState(id);
  }, []);

  const markMcpInstalled = useCallback(() => {
    setMcpInstalledAgents((current) => {
      const next = addMcpInstalledAgent(current, installAgentId);
      writeMcpInstalledAgents(next);
      return next;
    });
  }, [installAgentId]);

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
    installAgentId,
    setInstallAgentId,
    mcpInstalledAgents,
    markMcpInstalled,
    activeExemplarId,
    setActiveExemplarId,
    welcomeSeen,
    markWelcomeSeen,
    trySeen,
    markTrySeen,
  };
}
