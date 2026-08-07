/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 *
 * Local extension: `placement` (`header` | `menu`) for chrome-specific defaults.
 * Accessible-name contract matches Registry (label XOR labelledBy).
 * Forwards Registry `variant` (default | outline) onto SegmentedControl.
 */

import { MonitorIcon, MoonIcon, SunIcon } from "@/components/icons";
import { useTheme, type Theme } from "@/hooks/use-theme";
import {
  SegmentedControl,
  type SegmentedControlNameProps,
  type SegmentedControlOption,
  type SegmentedControlVariant,
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

/** Same ownership as SegmentedControl; `label` defaults to `"Theme"` when omitted. */
export type ThemeToggleNameProps =
  | Extract<SegmentedControlNameProps, { labelledBy: string }>
  | {
      labelledBy?: never;
      label?: string;
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
  /**
   * Track chrome forwarded to SegmentedControl. Use `outline` on muted
   * surfaces (sidebar, rail) so the well still reads.
   */
  variant?: SegmentedControlVariant;
  className?: string;
} & ThemeToggleNameProps;

/** Light / Dark / System switcher — sliding pill over icon segments. */
export function ThemeToggle({
  placement = "header",
  orientation = "horizontal",
  showLabels,
  tooltips,
  size = "default",
  variant = "default",
  label,
  labelledBy,
  className,
}: ThemeToggleProps) {
  const { theme, setTheme } = useTheme();
  const defaults = PLACEMENT_DEFAULTS[placement];
  // ThemeToggle owns the default product name; SegmentedControl owns aria emission.
  const nameProps: SegmentedControlNameProps =
    typeof labelledBy === "string" && labelledBy.trim() !== ""
      ? { labelledBy: labelledBy.trim() }
      : { label: label?.trim() || "Theme" };
  return (
    <SegmentedControl
      options={THEME_OPTIONS}
      value={theme}
      onValueChange={setTheme}
      orientation={orientation}
      showLabels={showLabels ?? defaults.showLabels}
      tooltips={tooltips ?? defaults.tooltips}
      size={size}
      variant={variant}
      className={cn(defaults.className, className)}
      {...nameProps}
    />
  );
}
ThemeToggle.displayName = "ThemeToggle";
