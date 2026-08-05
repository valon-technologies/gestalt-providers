import { Palette } from "lucide-react";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  DEFAULT_TENANT_THEME_LABEL,
  type ThemeSource,
  applyThemeSource,
  readThemeSource,
  THEME_SOURCE_STORAGE_KEY,
} from "@/lib/theme-source";

const tenantThemeLabel =
  import.meta.env.VITE_THEME_SWITCHER_TENANT_LABEL?.trim() ||
  DEFAULT_TENANT_THEME_LABEL;

/** Development preview control for the generic and runtime tenant themes. */
export function ThemeSwitcher() {
  const [source, setSource] = useState<ThemeSource>(readThemeSource);

  useEffect(() => {
    applyThemeSource(source);
    localStorage.setItem(THEME_SOURCE_STORAGE_KEY, source);
  }, [source]);

  function handleSourceChange(value: string) {
    if (value === "default" || value === "tenant") {
      setSource(value);
    }
  }

  const activeLabel = source === "default" ? "Default" : tenantThemeLabel;

  return (
    <div className="fixed bottom-4 right-4 z-[60]">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            type="button"
            variant="outline"
            size="icon-lg"
            aria-label={`Open theme switcher. Current theme: ${activeLabel}`}
            title={`Theme: ${activeLabel}`}
            className="rounded-full bg-card shadow-lg"
          >
            <Palette aria-hidden className="size-5" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" side="top" sideOffset={10}>
          <DropdownMenuLabel>Preview theme</DropdownMenuLabel>
          <DropdownMenuRadioGroup
            value={source}
            onValueChange={handleSourceChange}
            aria-label="Theme preview"
          >
            <DropdownMenuRadioItem value="default">
              Default theme
            </DropdownMenuRadioItem>
            <DropdownMenuRadioItem value="tenant">
              {tenantThemeLabel} theme
            </DropdownMenuRadioItem>
          </DropdownMenuRadioGroup>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
