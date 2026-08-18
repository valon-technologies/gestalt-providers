import { useId, type ReactNode } from "react";
import { useNavigate, useRouterState } from "@tanstack/react-router";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";

export function useHashTab(ids: readonly string[], fallbackId: string) {
  const { hash, pathname } = useRouterState({
    select: (state) => ({
      hash: state.location.hash.replace(/^#/, ""),
      pathname: state.location.pathname,
    }),
  });
  const navigate = useNavigate();
  const activeId = ids.includes(hash) ? hash : fallbackId;

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
  children,
}: {
  label: string;
  options: ReadonlyArray<SegmentedControlOption<V>>;
  value: V;
  onValueChange: (value: V) => void;
  children: ReactNode;
}) {
  // Stable panel id — never the option value. Hash-backed switchers write
  // `#${value}` for shareable selection; a matching DOM id would scroll the
  // panel under sticky app chrome.
  const panelId = useId();
  const activeLabel =
    options.find((option) => option.value === value)?.label ?? label;

  return (
    <div
      data-typeset-chrome
      data-docs-option-switcher
      data-docs-hash-ids={options.map((option) => option.value).join(" ")}
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
