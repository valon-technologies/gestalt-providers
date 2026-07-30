import {
  InputGroup,
  InputGroupAddon,
  InputGroupClearAddon,
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
  const trimmedQuery = query.trim();
  const showClear = trimmedQuery.length > 0 && !disabled;

  function clearSearch() {
    onQueryChange("");
  }

  return (
    <div className="w-full max-w-sm">
      <InputGroup>
        <InputGroupAddon align="inline-start">
          <SearchIcon aria-hidden />
        </InputGroupAddon>
        <InputGroupInput
          type="search"
          aria-label="Search apps"
          autoComplete="off"
          disabled={disabled}
          value={query}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder="Search apps"
        />
        <InputGroupClearAddon
          visible={showClear}
          aria-label="Clear app search"
          onClear={clearSearch}
        >
          <CloseIcon className="size-4" />
        </InputGroupClearAddon>
      </InputGroup>
    </div>
  );
}
