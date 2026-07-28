/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

"use client";

import { createContext, useContext, type ReactNode } from "react";

export const SearchHighlightQueryContext = createContext("");

export function useSearchHighlightQueryContext(): string {
  return useContext(SearchHighlightQueryContext);
}

/** Optional override for tests / non-table surfaces. DataTable bridges the same value. */
export function SearchHighlightProvider({
  query,
  children,
}: {
  query: string;
  children: ReactNode;
}) {
  return (
    <SearchHighlightQueryContext.Provider value={query}>
      {children}
    </SearchHighlightQueryContext.Provider>
  );
}
