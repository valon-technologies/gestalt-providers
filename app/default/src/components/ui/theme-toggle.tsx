"use client";

import { MonitorIcon, MoonIcon, SunIcon } from "@/components/icons";
import { useTheme, type Theme } from "@/hooks/use-theme";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";

/**
 * Gestalt console vendor of Valon Registry `ThemeToggle`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/theme-toggle.tsx`). Theme state stays on
 * console `@/hooks/use-theme` (owns `.dark` + `localStorage`) so main.tsx
 * bootstrap is unchanged; public props match Registry (`tooltips` default true).
 */

const THEME_OPTIONS: ReadonlyArray<SegmentedControlOption<Theme>> = [
  { value: "light", label: "Light", icon: SunIcon },
  { value: "dark", label: "Dark", icon: MoonIcon },
  { value: "system", label: "System", icon: MonitorIcon },
];

export type ThemeToggleProps = {
  orientation?: "horizontal" | "vertical";
  showLabels?: boolean;
  tooltips?: boolean;
  size?: "xs" | "sm" | "default";
  label?: string;
  className?: string;
};

/** Light / Dark / System switcher — sliding pill over icon segments. */
export function ThemeToggle({
  orientation = "horizontal",
  showLabels = false,
  tooltips = true,
  size = "default",
  label = "Theme",
  className,
}: ThemeToggleProps) {
  const { theme, setTheme } = useTheme();
  return (
    <SegmentedControl
      options={THEME_OPTIONS}
      value={theme}
      onValueChange={setTheme}
      label={label}
      orientation={orientation}
      showLabels={showLabels}
      tooltips={tooltips}
      size={size}
      className={className}
    />
  );
}
ThemeToggle.displayName = "ThemeToggle";
