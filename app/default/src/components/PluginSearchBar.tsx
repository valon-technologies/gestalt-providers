import { useRef } from "react";
import { CloseIcon, SearchIcon } from "./icons";
import { INPUT_CLASSES } from "@/lib/constants";

type PluginSearchBarProps = {
  query: string;
  disabled?: boolean;
  onQueryChange: (query: string) => void;
};

/**
 * Plain filter field for the apps catalog — no suggestion flyout.
 * Typing filters the grid; matches are highlighted on the cards.
 */
export default function PluginSearchBar({
  query,
  disabled = false,
  onQueryChange,
}: PluginSearchBarProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const trimmedQuery = query.trim();

  function clearSearch() {
    onQueryChange("");
    inputRef.current?.focus();
  }

  return (
    <div className="w-full max-w-sm">
      <div className="relative">
        <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-faint" />
        <input
          ref={inputRef}
          type="search"
          aria-label="Search apps"
          autoComplete="off"
          disabled={disabled}
          value={query}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder="Search apps"
          className={`w-full pl-9 pr-10 ${INPUT_CLASSES} disabled:cursor-not-allowed disabled:opacity-60`}
        />
        {trimmedQuery.length > 0 && !disabled ? (
          <button
            type="button"
            className="absolute right-2 top-1/2 z-30 flex h-7 w-7 -translate-y-1/2 items-center justify-center rounded-md text-faint transition-colors duration-150 hover:bg-alpha-5 hover:text-muted-foreground"
            aria-label="Clear app search"
            onClick={clearSearch}
          >
            <CloseIcon className="h-4 w-4" />
          </button>
        ) : null}
      </div>
    </div>
  );
}
