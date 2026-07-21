import { MonitorIcon, MoonIcon, SunIcon } from "@/components/icons";
import { useTheme, type Theme } from "@/hooks/use-theme";
import {
  SegmentedControl,
  type SegmentedControlOption,
} from "@/components/ui/segmented-control";

const THEME_OPTIONS: ReadonlyArray<SegmentedControlOption<Theme>> = [
  { value: "light", label: "Light", icon: SunIcon },
  { value: "dark", label: "Dark", icon: MoonIcon },
  { value: "system", label: "System", icon: MonitorIcon },
];

export type ThemeToggleProps = {
  orientation?: "horizontal" | "vertical";
  showLabels?: boolean;
  size?: "xs" | "sm" | "default";
  label?: string;
  className?: string;
};

/** Light / Dark / System switcher — sliding pill over icon segments. */
export function ThemeToggle({
  orientation = "horizontal",
  showLabels = false,
  size = "sm",
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
      size={size}
      className={className}
    />
  );
}
