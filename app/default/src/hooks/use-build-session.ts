import { useCallback, useState } from "react";
import {
  readActiveExemplarId,
  readIntroSeenFlag,
  readMcpInstalledFlag,
  readStoredApiToken,
  writeActiveExemplarId,
  writeIntroSeenFlag,
  writeMcpInstalledFlag,
  writeStoredApiToken,
  type BuildExemplarId,
} from "@/lib/buildPaths";

export type BuildSession = {
  apiToken: string;
  setApiToken: (token: string) => void;
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
  const [mcpInstalled, setMcpInstalled] = useState(readMcpInstalledFlag);
  const [activeExemplarId, setActiveExemplarIdState] =
    useState(readActiveExemplarId);
  const [introSeen, setIntroSeen] = useState(readIntroSeenFlag);

  const setApiToken = useCallback((token: string) => {
    writeStoredApiToken(token);
    setApiTokenState(token);
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
    mcpInstalled,
    markMcpInstalled,
    activeExemplarId,
    setActiveExemplarId,
    introSeen,
    markIntroSeen,
  };
}
