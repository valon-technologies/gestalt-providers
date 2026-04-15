"use client";

import { Combobox, ComboboxButton, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import { useRef, useState } from "react";
import type { Integration } from "@/lib/api";
import { INPUT_CLASSES } from "@/lib/constants";
import { filterIntegrations, getIntegrationLabel } from "@/lib/integrationSearch";
import { CloseIcon, SearchIcon } from "./icons";

type PluginSearchBarProps = {
  integrations: Integration[];
  query: string;
  disabled?: boolean;
  onQueryChange: (query: string) => void;
};

export default function PluginSearchBar({
  integrations,
  query,
  disabled = false,
  onQueryChange,
}: PluginSearchBarProps) {
  const [selectedIntegration, setSelectedIntegration] = useState<Integration | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const trimmedQuery = query.trim();
  const matchingIntegrations = filterIntegrations(integrations, query).slice(0, 8);

  function clearSearch() {
    setSelectedIntegration(null);
    onQueryChange("");
    inputRef.current?.focus();
  }

  function selectIntegration(integration: Integration | null) {
    setSelectedIntegration(integration);
    if (!integration) {
      return;
    }
    onQueryChange(getIntegrationLabel(integration));
  }

  return (
    <div className="w-full max-w-sm">
      <div className="relative">
        <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-faint" />
        <Combobox
          value={selectedIntegration}
          onChange={selectIntegration}
          disabled={disabled}
          immediate
        >
          <ComboboxInput
            ref={inputRef}
            aria-label="Search plugins"
            autoComplete="off"
            className={`w-full pl-9 pr-10 ${INPUT_CLASSES} disabled:cursor-not-allowed disabled:opacity-60`}
            displayValue={() => query}
            onChange={(event) => {
              setSelectedIntegration(null);
              onQueryChange(event.target.value);
            }}
            placeholder="Search plugins"
          />
          {trimmedQuery.length > 0 && !disabled && (
            <ComboboxButton
              className="absolute right-2 top-1/2 z-30 flex h-7 w-7 -translate-y-1/2 items-center justify-center rounded-md text-faint transition-colors duration-150 hover:bg-alpha-5 hover:text-muted"
              aria-label="Clear plugin search"
              onMouseDown={(event) => {
                event.preventDefault();
              }}
              onClick={(event) => {
                event.preventDefault();
                clearSearch();
              }}
            >
              <CloseIcon className="h-4 w-4" />
            </ComboboxButton>
          )}
          {trimmedQuery.length > 0 && !disabled && (
            <ComboboxOptions className="absolute left-0 top-full z-20 mt-2 max-h-80 w-full overflow-auto rounded-lg border border-alpha bg-base-white p-1 shadow-dropdown dark:bg-surface">
              {matchingIntegrations.length > 0 ? (
                matchingIntegrations.map((integration) => {
                  const label = getIntegrationLabel(integration);
                  const showName = integration.displayName && integration.displayName !== integration.name;
                  const secondaryText = showName
                    ? integration.name
                    : integration.description;

                  return (
                    <ComboboxOption
                      key={integration.name}
                      value={integration}
                      className="cursor-pointer rounded-md px-3 py-2 transition-colors duration-150 data-[focus]:bg-base-100 dark:data-[focus]:bg-surface-raised"
                    >
                      <div className="text-sm font-medium text-primary">
                        {label}
                      </div>
                      {secondaryText && (
                        <div className="mt-0.5 text-xs text-muted">
                          {secondaryText}
                        </div>
                      )}
                    </ComboboxOption>
                  );
                })
              ) : (
                <div className="px-3 py-2 text-sm text-muted">
                  No matching plugins.
                </div>
              )}
            </ComboboxOptions>
          )}
        </Combobox>
      </div>
    </div>
  );
}
