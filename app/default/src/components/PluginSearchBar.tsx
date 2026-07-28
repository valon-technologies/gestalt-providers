import { useRef } from "react";
import { CloseIcon, SearchIcon } from "./icons";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group";

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
    <InputGroup className="w-full max-w-sm">
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
      />
      {trimmedQuery.length > 0 && !disabled ? (
        <InputGroupAddon align="inline-end">
          <InputGroupButton
            size="icon-xs"
            aria-label="Clear app search"
            onClick={clearSearch}
          >
            <CloseIcon />
          </InputGroupButton>
        </InputGroupAddon>
      ) : null}
    </InputGroup>
  );
}
