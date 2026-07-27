import { useCallback, useState } from "react";
import {
  readActiveExemplarId,
  readIntroSeenFlag,
  readMcpInstalledFlag,
  readStoredApiToken,
  readStoredApiTokenGrantId,
  readStoredSelectedTokenId,
  readStoredTokenName,
  readStoredInstallAgent,
  writeActiveExemplarId,
  writeIntroSeenFlag,
  writeMcpInstalledFlag,
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
  introSeen: boolean;
  markIntroSeen: () => void;
};

/** Client session for the Build journey — survives step navigation via sessionStorage. */
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
  const [introSeen, setIntroSeen] = useState(readIntroSeenFlag);

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
    const bound = readStoredApiTokenGrantId();
    if (bound && bound !== grantId) {
      writeStoredApiToken("");
      writeStoredApiTokenGrantId("");
      setApiTokenState("");
      setApiTokenGrantIdState("");
    }
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

  const markIntroSeen = useCallback(() => {
    writeIntroSeenFlag(true);
    setIntroSeen(true);
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
    introSeen,
    markIntroSeen,
  };
}
