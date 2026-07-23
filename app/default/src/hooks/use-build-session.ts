import { useCallback, useState } from "react";
import {
  readActiveExemplarId,
  readIntroSeenFlag,
  readMcpInstalledFlag,
  readStoredApiToken,
  readStoredSelectedTokenId,
  readStoredTokenName,
  writeActiveExemplarId,
  writeIntroSeenFlag,
  writeMcpInstalledFlag,
  writeStoredApiToken,
  writeStoredSelectedTokenId,
  writeStoredTokenName,
  type BuildExemplarId,
} from "@/lib/buildPaths";

export type BuildSession = {
  apiToken: string;
  setApiToken: (token: string) => void;
  tokenName: string;
  setTokenName: (name: string) => void;
  selectedTokenId: string;
  setSelectedTokenId: (id: string) => void;
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
  const [tokenName, setTokenNameState] = useState(readStoredTokenName);
  const [selectedTokenId, setSelectedTokenIdState] = useState(
    readStoredSelectedTokenId,
  );
  const [mcpInstalled, setMcpInstalled] = useState(readMcpInstalledFlag);
  const [activeExemplarId, setActiveExemplarIdState] =
    useState(readActiveExemplarId);
  const [introSeen, setIntroSeen] = useState(readIntroSeenFlag);

  const setApiToken = useCallback((token: string) => {
    writeStoredApiToken(token);
    setApiTokenState(token);
  }, []);

  const setTokenName = useCallback((name: string) => {
    writeStoredTokenName(name);
    setTokenNameState(name);
  }, []);

  const setSelectedTokenId = useCallback((id: string) => {
    writeStoredSelectedTokenId(id);
    setSelectedTokenIdState(id);
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
    setApiToken,
    tokenName,
    setTokenName,
    selectedTokenId,
    setSelectedTokenId,
    mcpInstalled,
    markMcpInstalled,
    activeExemplarId,
    setActiveExemplarId,
    introSeen,
    markIntroSeen,
  };
}
