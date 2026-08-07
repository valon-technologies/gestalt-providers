/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { ChevronDown } from "lucide-react";

import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { SelectionCheck } from "@/components/ui/selection-check";
import { disclosureCaretClassName } from "@/lib/disclosure-caret";
import { cn } from "@/lib/cn";

export type PersonOption = {
  value: string;
  label: string;
  subtitle?: string;
  /** Optional photo URL — apps own directory media; fallback uses `initials` / label. */
  avatarUrl?: string;
  /** Optional initials override when `avatarUrl` is missing or fails to load. */
  initials?: string;
  /**
   * Person identity chip. Defaults to true. Set false for non-person scopes
   * (e.g. Everyone) — no disc and no leading gutter.
   */
  showAvatar?: boolean;
};

export type PeoplePickerPreset = PersonOption;

function personInitials(option: PersonOption): string {
  if (option.initials?.trim()) return option.initials.trim();
  const parts = option.label.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return parts[0]!.slice(0, 2).toUpperCase();
  return `${parts[0]![0] ?? ""}${parts[parts.length - 1]![0] ?? ""}`.toUpperCase();
}

function PersonRow({
  option,
  selected,
  trailing,
}: {
  option: PersonOption;
  selected: boolean;
  trailing?: React.ReactNode;
}) {
  const showAvatar = option.showAvatar !== false;

  return (
    <span className="min-w-0 flex-1">
      <span className="flex items-baseline gap-2">
        {showAvatar ? (
          <Avatar aria-hidden size="sm" variant="solid">
            {option.avatarUrl ? <AvatarImage src={option.avatarUrl} alt="" /> : null}
            <AvatarFallback>{personInitials(option)}</AvatarFallback>
          </Avatar>
        ) : null}
        <span className={selected ? "min-w-0 flex-1 truncate font-medium" : "min-w-0 flex-1 truncate"}>
          {option.label}
        </span>
        {trailing}
      </span>
      {option.subtitle ? (
        <span
          className={cn(
            "block truncate text-xs text-muted-foreground",
            showAvatar && "pl-8",
          )}
        >
          {option.subtitle}
        </span>
      ) : null}
    </span>
  );
}

function matchesQuery(text: string, query: string): boolean {
  if (!query) return true;
  return text.toLowerCase().includes(query.toLowerCase());
}

function presetMatches(preset: PeoplePickerPreset, query: string): boolean {
  if (!query) return true;
  return (
    matchesQuery(preset.label, query) ||
    matchesQuery(preset.value, query) ||
    (preset.subtitle ? matchesQuery(preset.subtitle, query) : false)
  );
}

export type PeoplePickerProps = {
  value: string;
  /** Value commit — same vocabulary as Combobox/Select (`onValueChange`). */
  onValueChange: (value: string, option?: PersonOption | PeoplePickerPreset) => void;
  /** Async people search — authoritative directory results while the popover is open. */
  searchPeople: (query: string) => Promise<PersonOption[]>;
  /** Shortcut rows (e.g. Me / Everyone) rendered above search results. */
  presets?: PeoplePickerPreset[];
  placeholder?: string;
  searchPlaceholder?: string;
  /** When true, Enter with no exact match commits the typed string as the value. */
  allowCustomValue?: boolean;
  disabled?: boolean;
  /** Values to omit from async results (e.g. the signed-in user when Me is a preset). */
  excludeValues?: string[];
  className?: string;
};

export function PeoplePicker({
  value,
  onValueChange,
  searchPeople,
  presets = [],
  placeholder = "Select person",
  searchPlaceholder = "Filter people",
  allowCustomValue = false,
  disabled = false,
  excludeValues = [],
  className,
}: PeoplePickerProps) {
  const [query, setQuery] = React.useState("");
  const [options, setOptions] = React.useState<PersonOption[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [open, setOpen] = React.useState(false);
  // Canonical option source — survives when ephemeral search results omit the selection.
  const optionCacheRef = React.useRef(new Map<string, PersonOption>());
  const searchPeopleRef = React.useRef(searchPeople);
  searchPeopleRef.current = searchPeople;
  const requestIdRef = React.useRef(0);

  const trimmedQuery = query.trim();
  const normalizedQuery = trimmedQuery.toLowerCase();
  const excluded = React.useMemo(
    () => new Set(excludeValues.map((entry) => entry.toLowerCase())),
    [excludeValues],
  );

  const visiblePresets = presets.filter((preset) => presetMatches(preset, trimmedQuery));

  function rememberOption(option: PersonOption | PeoplePickerPreset) {
    optionCacheRef.current.set(option.value, option);
  }

  for (const option of options) rememberOption(option);
  for (const preset of presets) rememberOption(preset);

  const selectedOption = React.useMemo(() => {
    const preset = presets.find((entry) => entry.value === value);
    if (preset) return preset;
    return optionCacheRef.current.get(value) ?? options.find((option) => option.value === value);
  }, [value, presets, options]);

  const selectedLabel = selectedOption?.label ?? (value || placeholder);

  function handleOpenChange(nextOpen: boolean) {
    setOpen(nextOpen);
    if (!nextOpen) {
      setQuery("");
      setLoading(false);
    }
  }

  React.useEffect(() => {
    if (!open) return;
    const requestId = ++requestIdRef.current;
    let cancelled = false;
    // Pending debounce counts as loading so stale directory rows / false empties
    // do not flash before searchPeople runs.
    setLoading(true);
    const timer = window.setTimeout(() => {
      void searchPeopleRef
        .current(trimmedQuery)
        .then((people) => {
          if (cancelled || requestId !== requestIdRef.current) return;
          for (const person of people) rememberOption(person);
          setOptions(people);
        })
        .catch(() => {
          if (cancelled || requestId !== requestIdRef.current) return;
          setOptions([]);
        })
        .finally(() => {
          if (cancelled || requestId !== requestIdRef.current) return;
          setLoading(false);
        });
    }, 200);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [open, trimmedQuery]);

  function selectPerson(next: string, option?: PersonOption | PeoplePickerPreset) {
    if (option) rememberOption(option);
    onValueChange(next, option);
    setOpen(false);
    setQuery("");
    setLoading(false);
  }

  // searchPeople owns match semantics; registry only applies excludeValues.
  const directoryOptions = options.filter(
    (option) => !excluded.has(option.value.toLowerCase()),
  );

  const showEmpty =
    !loading && directoryOptions.length === 0 && visiblePresets.length === 0;

  const emptyMessage = (() => {
    if (trimmedQuery.length === 0) return "No people found.";
    if (allowCustomValue) return `No matches. Press Enter to use “${trimmedQuery}”.`;
    return "No matches.";
  })();

  return (
    <Popover open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          className={cn("group w-full justify-between", className)}
        >
          <span className="flex min-w-0 flex-1 items-center gap-2">
            {selectedOption && selectedOption.showAvatar !== false ? (
              <Avatar aria-hidden size="sm" variant="solid">
                {selectedOption.avatarUrl ? (
                  <AvatarImage src={selectedOption.avatarUrl} alt="" />
                ) : null}
                <AvatarFallback>{personInitials(selectedOption)}</AvatarFallback>
              </Avatar>
            ) : null}
            <span className="truncate">{selectedLabel}</span>
          </span>
          <ChevronDown aria-hidden className={cn(disclosureCaretClassName, "ml-2")} />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[var(--radix-popover-trigger-width)] p-0" align="start">
        <Command shouldFilter={false} className="min-h-0 flex-1">
          <CommandInput
            placeholder={searchPlaceholder}
            value={query}
            onValueChange={setQuery}
            onKeyDown={(event) => {
              if (event.key !== "Enter" || !trimmedQuery) return;
              // Rows are unmounted while loading — do not defer to cmdk on stale
              // counts or commit custom values before searchPeople returns.
              if (loading) {
                event.preventDefault();
                event.stopPropagation();
                return;
              }
              const exact = directoryOptions.find(
                (option) =>
                  option.value.toLowerCase() === normalizedQuery ||
                  option.label.toLowerCase() === normalizedQuery,
              );
              if (exact) {
                event.preventDefault();
                event.stopPropagation();
                selectPerson(exact.value, exact);
                return;
              }
              // Visible rows exist — let cmdk commit the highlighted item.
              if (visiblePresets.length > 0 || directoryOptions.length > 0) return;
              if (allowCustomValue) {
                event.preventDefault();
                event.stopPropagation();
                selectPerson(trimmedQuery, {
                  value: trimmedQuery,
                  label: trimmedQuery,
                });
              }
            }}
          />
          <CommandList className="min-h-0 flex-1">
            {loading ? <CommandEmpty>Searching…</CommandEmpty> : null}
            {showEmpty ? <CommandEmpty>{emptyMessage}</CommandEmpty> : null}
            {!loading && (visiblePresets.length > 0 || directoryOptions.length > 0) ? (
              <CommandGroup>
                {visiblePresets.map((preset, index) => {
                  const selected = value === preset.value;
                  return (
                    <React.Fragment key={preset.value}>
                      {index > 0 ? <CommandSeparator alwaysRender /> : null}
                      <CommandItem
                        value={preset.value}
                        onSelect={() => selectPerson(preset.value, preset)}
                        className="items-baseline"
                      >
                        <PersonRow
                          option={preset}
                          selected={selected}
                          trailing={
                            <SelectionCheck checked={selected} tone="solid" className="ml-auto" />
                          }
                        />
                      </CommandItem>
                    </React.Fragment>
                  );
                })}
                {visiblePresets.length > 0 && directoryOptions.length > 0 ? (
                  <CommandSeparator alwaysRender />
                ) : null}
                {directoryOptions.map((option) => {
                  const selected = value === option.value;
                  return (
                    <CommandItem
                      key={option.value}
                      value={option.value}
                      onSelect={() => selectPerson(option.value, option)}
                      className="items-baseline"
                    >
                      <PersonRow
                        option={option}
                        selected={selected}
                        trailing={
                          <SelectionCheck checked={selected} tone="solid" className="ml-auto" />
                        }
                      />
                    </CommandItem>
                  );
                })}
              </CommandGroup>
            ) : null}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
