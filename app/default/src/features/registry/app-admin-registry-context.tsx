import { createContext, useContext } from "react";
import type { AppAdminOutletContext } from "@/pages/app-admin-outlet-context";

const AppAdminRegistryContext = createContext<AppAdminOutletContext | null>(null);

export function AppAdminRegistryProvider({
  value,
  children,
}: {
  value: AppAdminOutletContext;
  children: React.ReactNode;
}) {
  return (
    <AppAdminRegistryContext.Provider value={value}>
      {children}
    </AppAdminRegistryContext.Provider>
  );
}

export function useAppAdminRegistryContext() {
  const context = useContext(AppAdminRegistryContext);
  if (!context) {
    throw new Error("useAppAdminRegistryContext requires AppAdminRegistryProvider");
  }
  return context;
}
