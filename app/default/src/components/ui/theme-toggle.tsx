
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

/** Surface that hosts the toggle — drives discoverability defaults. */
export type ThemeTogglePlacement = "header" | "menu";

const PLACEMENT_DEFAULTS: Record<
  ThemeTogglePlacement,
  { showLabels: boolean; tooltips: boolean; className?: string }
> = {
  // Compact chrome: icons + hover names.
  header: { showLabels: false, tooltips: true },
  // Account menu: icons + tooltips under a Theme section label.
  menu: {
    showLabels: false,
    tooltips: true,
  },
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
      orientation={orientation}
      showLabels={showLabels ?? defaults.showLabels}
      tooltips={tooltips ?? defaults.tooltips}
      size={size}
      className={cn(defaults.className, className)}
    />
  );
}
ThemeToggle.displayName = "ThemeToggle";
