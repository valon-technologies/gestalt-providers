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

const THEME_SOURCE_STORAGE_KEY = "gestalt-theme-source";
const TENANT_THEME_SELECTOR = 'link[data-theme-source="tenant"]';
const tenantThemeLabel =
  import.meta.env.VITE_THEME_SWITCHER_TENANT_LABEL?.trim() || "Valon";

type ThemeSource = "default" | "tenant";

function readThemeSource(): ThemeSource {
  if (typeof window === "undefined") return "tenant";
  return localStorage.getItem(THEME_SOURCE_STORAGE_KEY) === "default"
    ? "default"
    : "tenant";
}

function applyThemeSource(source: ThemeSource) {
  const stylesheet = document.querySelector<HTMLLinkElement>(
    TENANT_THEME_SELECTOR,
  );
  if (!stylesheet) return;

  if (source === "default") {
    stylesheet.disabled = true;
    stylesheet.setAttribute("disabled", "");
  } else {
    stylesheet.disabled = false;
    stylesheet.removeAttribute("disabled");
  }
}

/** Development-only preview control for the generic and runtime tenant themes. */
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
