
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { MonitorIcon, MoonIcon, SunIcon } from "@/components/icons";
import { useTheme, type Theme } from "@/hooks/use-theme";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";
import { cn } from "@/lib/cn";

const THEME_OPTIONS: ReadonlyArray<SegmentedControlOption<Theme>> = [
  { value: "light", label: "Light", icon: SunIcon },
  { value: "dark", label: "Dark", icon: MoonIcon },
  { value: "system", label: "System", icon: MonitorIcon },
];

/** Surface that hosts the toggle — call sites declare which chrome owns it. */
export type ThemeTogglePlacement = "header" | "menu";

/** Shared icon+tooltip presentation until a placement needs distinct defaults. */
const ICON_TOOLTIP_PRESENTATION = {
  showLabels: false,
  tooltips: true,
} as const;

const PLACEMENT_DEFAULTS: Record<
  ThemeTogglePlacement,
  { showLabels: boolean; tooltips: boolean; className?: string }
> = {
  header: ICON_TOOLTIP_PRESENTATION,
  menu: ICON_TOOLTIP_PRESENTATION,
};

export type ThemeToggleProps = {
  /** Presentation surface. Defaults encode header vs menu discoverability. */
  placement?: ThemeTogglePlacement;
  orientation?: "horizontal" | "vertical";
  /** Overrides placement default when set. */
  showLabels?: boolean;
  /** Overrides placement default when set. */
  tooltips?: boolean;
  size?: "xs" | "sm" | "default";
  label?: string;
  /** Prefer over `label` when a visible section label already names the control. */
  labelledBy?: string;
  className?: string;
};

/** Light / Dark / System switcher — sliding pill over icon segments. */
export function ThemeToggle({
  placement = "header",
  orientation = "horizontal",
  showLabels,
  tooltips,
  size = "default",
  label = "Theme",
  labelledBy,
  className,
}: ThemeToggleProps) {
  const { theme, setTheme } = useTheme();
  const defaults = PLACEMENT_DEFAULTS[placement];
  return (
    <SegmentedControl
      options={THEME_OPTIONS}
      value={theme}
      onValueChange={setTheme}
      label={label}
      labelledBy={labelledBy}
      orientation={orientation}
      showLabels={showLabels ?? defaults.showLabels}
      tooltips={tooltips ?? defaults.tooltips}
      size={size}
      className={cn(defaults.className, className)}
    />
  );
}
ThemeToggle.displayName = "ThemeToggle";
