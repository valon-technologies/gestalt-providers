import { createContext, useContext, type ReactNode } from "react";
import type { Integration } from "@/lib/api";
import type { AppAdminRegistryContextValue } from "@/features/registry/app-admin-registry-context";
import type { AppAdminSurface } from "./app-nav";

export type AppWorkspaceCapabilities = {
  registry: boolean;
  workflows: boolean;
  authorization: boolean;
};

export type AppWorkspaceContextValue = {
  app: string;
  integration: Integration | null;
  loading: boolean;
  error: string | null;
  capabilities: AppWorkspaceCapabilities;
  showConnectionNav: boolean;
  registryOutlet?: AppAdminRegistryContextValue;
  reloadIntegration: () => void;
};

const AppWorkspaceContext = createContext<AppWorkspaceContextValue | null>(null);

export function AppWorkspaceProvider({
  value,
  children,
}: {
  value: AppWorkspaceContextValue;
  children: ReactNode;
}) {
  return (
    <AppWorkspaceContext.Provider value={value}>
      {children}
    </AppWorkspaceContext.Provider>
  );
}

export function useAppWorkspace(): AppWorkspaceContextValue {
  const value = useContext(AppWorkspaceContext);
  if (!value) {
    throw new Error("useAppWorkspace must be used within AppWorkspaceProvider");
  }
  return value;
}

export function hasAdminSurface(
  capabilities: AppWorkspaceCapabilities,
  surface: AppAdminSurface,
): boolean {
  return capabilities[surface];
}

export function showAdminGroup(capabilities: AppWorkspaceCapabilities): boolean {
  return capabilities.registry || capabilities.authorization;
}
