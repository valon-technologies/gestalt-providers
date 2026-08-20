import { useEffect, useId, type ReactNode } from "react";
import { useNavigate, useRouterState } from "@tanstack/react-router";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";

export function resolveHashTabId(
  hash: string,
  ids: readonly string[],
  fallbackId: string,
  aliases: Readonly<Record<string, string>> = {},
): string {
  if (ids.includes(hash)) return hash;
  const aliased = aliases[hash];
  if (aliased && ids.includes(aliased)) return aliased;
  return fallbackId;
}

export function useHashTab(
  ids: readonly string[],
  fallbackId: string,
  aliases: Readonly<Record<string, string>> = {},
) {
  const { hash, pathname } = useRouterState({
    select: (state) => ({
      hash: state.location.hash.replace(/^#/, ""),
      pathname: state.location.pathname,
    }),
  });
  const navigate = useNavigate();
  const activeId = resolveHashTabId(hash, ids, fallbackId, aliases);
  const canonicalFromAlias = aliases[hash];

  useEffect(() => {
    if (
      canonicalFromAlias &&
      ids.includes(canonicalFromAlias) &&
      canonicalFromAlias !== hash
    ) {
      void navigate({ to: pathname, hash: canonicalFromAlias, replace: true });
    }
  }, [canonicalFromAlias, hash, ids, navigate, pathname]);

  function selectTab(id: string) {
    void navigate({ to: pathname, hash: id, replace: true });
  }

  return [activeId, selectTab] as const;
}

export function DocsOptionSwitcher<V extends string>({
  label,
  options,
  value,
  onValueChange,
  hashAliases,
  children,
}: {
  label: string;
  options: ReadonlyArray<SegmentedControlOption<V>>;
  value: V;
  onValueChange: (value: V) => void;
  /** Extra hashes that should scroll this switcher (legacy aliases). */
  hashAliases?: Readonly<Record<string, string>>;
  children: ReactNode;
}) {
  // Stable panel id — never the option value. Hash-backed switchers write
  // `#${value}` for shareable selection; a matching DOM id would scroll the
  // panel under sticky app chrome.
  const panelId = useId();
  const activeLabel =
    options.find((option) => option.value === value)?.label ?? label;
  const hashIds = [
    ...new Set([
      ...options.map((option) => option.value),
      ...Object.keys(hashAliases ?? {}),
      // Alias targets (e.g. dest-chatgpt) must scroll this switcher after
      // the URL is rewritten off the leftover mcp-* hash.
      ...Object.values(hashAliases ?? {}),
    ]),
  ].join(" ");

  return (
    <div
      data-typeset-chrome
      data-docs-option-switcher
      data-docs-hash-ids={hashIds}
      className="scroll-mt-[var(--page-layout-anchor-offset)]"
    >
      {/*
        Horizontal scroll for long labelled tracks. `overflow-x-auto` forces
        y-clipping too (CSS overflow pairing), so pad the clip edges for outward
        focus rings.
      */}
      <div className="not-typeset -mx-1 -mt-1 min-w-0 overflow-x-auto px-1 pb-1 pt-1">
        <SegmentedControl
          size="sm"
          label={label}
          value={value}
          onValueChange={onValueChange}
          options={options}
          panelId={panelId}
          showLabels
        />
      </div>
      <div
        id={panelId}
        role="region"
        aria-label={`${label}: ${activeLabel}`}
        aria-live="polite"
      >
        {children}
      </div>
    </div>
  );
}
