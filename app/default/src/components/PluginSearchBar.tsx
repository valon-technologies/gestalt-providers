import { useRef } from "react";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group";
import { cn } from "@/lib/cn";
import { CloseIcon, SearchIcon } from "./icons";

type PluginSearchBarProps = {
  query: string;
  disabled?: boolean;
  onQueryChange: (query: string) => void;
};

/**
 * Apps catalog search — Registry InputGroup (leading search icon, clear
 * addon). No call-site chrome overrides on the control.
 */
export default function PluginSearchBar({
  query,
  disabled = false,
  onQueryChange,
}: PluginSearchBarProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const trimmedQuery = query.trim();
  const showClear = trimmedQuery.length > 0 && !disabled;

  function clearSearch() {
    onQueryChange("");
    inputRef.current?.focus();
  }

  return (
    <div className="w-full max-w-sm">
      <InputGroup>
        <InputGroupAddon align="inline-start">
          <SearchIcon aria-hidden />
        </InputGroupAddon>
        <InputGroupInput
          ref={inputRef}
          type="search"
          aria-label="Search apps"
          autoComplete="off"
          disabled={disabled}
          value={query}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder="Search apps"
          className="[&::-webkit-search-cancel-button]:hidden"
        />
        <InputGroupAddon align="inline-end">
          <InputGroupButton
            aria-label={showClear ? "Clear app search" : undefined}
            aria-hidden={!showClear}
            tabIndex={showClear ? undefined : -1}
            className={cn(!showClear && "pointer-events-none invisible")}
            onMouseDown={(event) => {
              event.preventDefault();
            }}
            onClick={(event) => {
              event.preventDefault();
              if (!showClear) return;
              clearSearch();
            }}
          >
            <CloseIcon className="size-4" />
          </InputGroupButton>
        </InputGroupAddon>
      </InputGroup>
    </div>
  );
}
