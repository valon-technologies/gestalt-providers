import { useRef } from "react";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group";
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
        {trimmedQuery.length > 0 && !disabled ? (
          <InputGroupAddon align="inline-end">
            <InputGroupButton
              aria-label="Clear app search"
              onMouseDown={(event) => {
                event.preventDefault();
              }}
              onClick={(event) => {
                event.preventDefault();
                clearSearch();
              }}
            >
              <CloseIcon className="size-4" />
            </InputGroupButton>
          </InputGroupAddon>
        ) : null}
      </InputGroup>
    </div>
  );
}
