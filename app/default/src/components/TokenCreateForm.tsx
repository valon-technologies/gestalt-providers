import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
} from "@headlessui/react";
import { useId, useMemo, useState } from "react";
import {
  createToken,
  getIntegrationOperations,
  type IntegrationOperation,
} from "@/lib/api";
import {
  encodeTokenScopes,
  expiresInFromChoice,
  formatExpirationDayLabel,
  hasEffectiveScopes,
  type ExpirationChoice,
  type TokenScopeMode,
} from "@/lib/tokenScopes";
import {
  useIntegrationsQuery,
  useInvalidateTokens,
} from "@/hooks/use-server-queries";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { CodeBlock } from "@/components/ui/code-block";
import {
  Field,
  FieldDescription,
  FieldError,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/RadioGroup";
import {
  CalendarIcon,
  CheckIcon,
  ChevronDownIcon,
  SearchIcon,
} from "@/components/icons";
import { HighlightMatch } from "@/components/HighlightMatch";
import {
  filterIntegrations,
  getIntegrationLabel,
} from "@/lib/integrationSearch";
import { cn } from "@/lib/cn";

interface TokenCreateFormProps {
  /** Called with the one-time plaintext token after a successful create. */
  onCreated: (plaintext: string) => void | Promise<void>;
}

type DayPreset = 7 | 30 | 60 | 90;

type ExpirationOption =
  | { id: "7d" | "30d" | "60d" | "90d"; kind: "days"; days: DayPreset }
  | { id: "custom"; kind: "custom" }
  | { id: "none"; kind: "none" };

const DAY_PRESETS: DayPreset[] = [7, 30, 60, 90];

function buildExpirationOptions(): ExpirationOption[] {
  const dayOptions: ExpirationOption[] = DAY_PRESETS.map((days) => ({
    id: `${days}d` as "7d" | "30d" | "60d" | "90d",
    kind: "days",
    days,
  }));
  return [
    ...dayOptions,
    { id: "custom", kind: "custom" },
    { id: "none", kind: "none" },
  ];
}

function expirationOptionLabel(option: ExpirationOption, now: Date): string {
  if (option.kind === "days") {
    return formatExpirationDayLabel(option.days, now);
  }
  if (option.kind === "custom") {
    return "Custom…";
  }
  return "No expiration";
}

type SelectedAppState = {
  /** When true (default), encode bare appId. When false, encode checked ops only. */
  allOperations: boolean;
  /** Checked operation ids when allOperations is false. */
  operationIds: Set<string>;
};

/** Collapse display label vs machine id for “same app?” (GitHub ≈ github). */
function normalizeAppIdKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

export default function TokenCreateForm({ onCreated }: TokenCreateFormProps) {
  const idPrefix = useId();
  const nameId = `${idPrefix}-name`;
  const expirationId = `${idPrefix}-expiration`;
  const customDateId = `${idPrefix}-custom-date`;
  const appAccessId = `${idPrefix}-app-access`;
  const appSearchId = `${idPrefix}-app-search`;

  const [name, setName] = useState("");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [plaintext, setPlaintext] = useState<string | null>(null);

  const [expirationIdSelected, setExpirationIdSelected] =
    useState<ExpirationOption["id"]>("30d");
  const [customDate, setCustomDate] = useState("");

  const [scopeMode, setScopeMode] = useState<TokenScopeMode>("all");
  const [appQuery, setAppQuery] = useState("");
  const [selectedApps, setSelectedApps] = useState<
    Record<string, SelectedAppState>
  >({});
  const [opsByApp, setOpsByApp] = useState<
    Record<string, IntegrationOperation[] | "loading" | "error">
  >({});

  const invalidateTokens = useInvalidateTokens();
  const integrationsQuery = useIntegrationsQuery({
    enabled: scopeMode === "select",
  });
  const integrations = integrationsQuery.data ?? null;
  const integrationsError =
    integrationsQuery.error instanceof Error
      ? integrationsQuery.error.message
      : integrationsQuery.error
        ? "Failed to load apps"
        : null;

  const now = useMemo(() => new Date(), []);
  const expirationOptions = useMemo(() => buildExpirationOptions(), []);
  const selectedExpiration =
    expirationOptions.find((o) => o.id === expirationIdSelected) ??
    expirationOptions[1];

  function ensureOpsLoaded(appName: string) {
    setOpsByApp((prev) => {
      if (prev[appName] !== undefined) return prev;
      return { ...prev, [appName]: "loading" };
    });

    void getIntegrationOperations(appName)
      .then((ops) => {
        setOpsByApp((prev) => ({
          ...prev,
          [appName]: ops.filter((op) => op.visible !== false),
        }));
      })
      .catch(() => {
        setOpsByApp((prev) => ({ ...prev, [appName]: "error" }));
      });
  }

  function toggleApp(appName: string, checked: boolean) {
    setSelectedApps((prev) => {
      if (!checked) {
        const next = { ...prev };
        delete next[appName];
        return next;
      }
      ensureOpsLoaded(appName);
      return {
        ...prev,
        [appName]: { allOperations: true, operationIds: new Set() },
      };
    });
  }

  function setAppAllOperations(appName: string, all: boolean) {
    setSelectedApps((prev) => {
      const current = prev[appName];
      if (!current) return prev;
      if (all) {
        return {
          ...prev,
          [appName]: { allOperations: true, operationIds: new Set() },
        };
      }
      const ops = opsByApp[appName];
      const allIds =
        Array.isArray(ops) ? ops.map((op) => op.id) : [...current.operationIds];
      return {
        ...prev,
        [appName]: {
          allOperations: false,
          operationIds: new Set(allIds),
        },
      };
    });
  }

  function toggleOperation(appName: string, opId: string, checked: boolean) {
    setSelectedApps((prev) => {
      const current = prev[appName];
      if (!current) return prev;

      const nextIds = new Set(
        current.allOperations
          ? Array.isArray(opsByApp[appName])
            ? (opsByApp[appName] as IntegrationOperation[]).map((op) => op.id)
            : []
          : current.operationIds,
      );

      if (checked) nextIds.add(opId);
      else nextIds.delete(opId);

      const ops = opsByApp[appName];
      const total = Array.isArray(ops) ? ops.length : nextIds.size;
      const allSelected = total > 0 && nextIds.size === total;

      return {
        ...prev,
        [appName]: {
          allOperations: allSelected,
          operationIds: allSelected ? new Set() : nextIds,
        },
      };
    });
  }

  function buildSelections() {
    return Object.entries(selectedApps).map(([appId, state]) => ({
      appId,
      operations: state.allOperations
        ? ({ kind: "all" } as const)
        : ({
            kind: "ops" as const,
            operationIds: [...state.operationIds],
          }),
    }));
  }

  function buildExpirationChoice(): ExpirationChoice {
    if (selectedExpiration.kind === "days") {
      return { kind: "days", days: selectedExpiration.days };
    }
    if (selectedExpiration.kind === "none") {
      return { kind: "none" };
    }
    return { kind: "custom", date: customDate };
  }

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const trimmedName = name.trim();
    if (!trimmedName) return;

    const selections = buildSelections();
    if (!hasEffectiveScopes(scopeMode, selections)) {
      setFieldError("Select at least one app.");
      return;
    }

    if (
      selectedExpiration.kind === "custom" &&
      expiresInFromChoice({ kind: "custom", date: customDate }) == null
    ) {
      setFieldError("Choose a future expiration date.");
      return;
    }

    setFieldError(null);
    setCreating(true);
    setError(null);
    setPlaintext(null);

    const scopes = encodeTokenScopes(scopeMode, selections);
    const expiresIn = expiresInFromChoice(buildExpirationChoice());

    try {
      const result = await createToken(trimmedName, scopes, expiresIn);
      setPlaintext(result.token);
      setName("");
      setScopeMode("all");
      setSelectedApps({});
      setAppQuery("");
      setExpirationIdSelected("30d");
      setCustomDate("");
      await invalidateTokens();
      await onCreated(result.token);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create token");
    } finally {
      setCreating(false);
    }
  }

  const filteredApps = useMemo(
    () => filterIntegrations(integrations ?? [], appQuery),
    [integrations, appQuery],
  );

  const selectedAppNames = Object.keys(selectedApps);

  return (
    <>
      <form onSubmit={handleSubmit} className="flex max-w-xl flex-col gap-4">
        <FieldGroup className="gap-5">
          <Field>
            <FieldLabel htmlFor={nameId}>Token name</FieldLabel>
            <Input
              id={nameId}
              name="name"
              type="text"
              required
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. ci-pipeline"
              autoComplete="off"
            />
          </Field>

          <Field>
            <FieldLabel id={expirationId}>Expiration</FieldLabel>
            <Listbox
              value={selectedExpiration}
              onChange={(option: ExpirationOption) => {
                setExpirationIdSelected(option.id);
                setFieldError(null);
              }}
            >
              <div className="relative">
                <ListboxButton
                  aria-labelledby={expirationId}
                  className={cn(
                    "relative flex w-full items-center gap-2 rounded-md border border-input bg-transparent px-3 py-2 text-left text-sm text-foreground shadow-xs",
                    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-base-950/10",
                    "dark:focus-visible:ring-base-200/10",
                  )}
                >
                  <CalendarIcon className="size-4 shrink-0 text-muted-foreground" />
                  <span className="min-w-0 flex-1 truncate">
                    {expirationOptionLabel(selectedExpiration, now)}
                  </span>
                  <ChevronDownIcon className="size-4 shrink-0 text-muted-foreground" />
                </ListboxButton>
                <ListboxOptions
                  anchor="bottom start"
                  className="z-50 mt-1 max-h-72 w-[var(--button-width)] overflow-auto rounded-md border border-alpha bg-base-white p-1 shadow-dropdown dark:bg-surface"
                >
                  {expirationOptions.map((option) => (
                    <ListboxOption
                      key={option.id}
                      value={option}
                      className="flex cursor-pointer items-center gap-2 rounded-sm px-2 py-1.5 text-sm text-foreground data-focus:bg-alpha-5 data-selected:font-medium"
                    >
                      {({ selected }) => (
                        <>
                          <span className="flex size-4 shrink-0 items-center justify-center">
                            {selected ? (
                              <CheckIcon className="size-3.5" />
                            ) : null}
                          </span>
                          <span className="min-w-0 flex-1 truncate">
                            {expirationOptionLabel(option, now)}
                          </span>
                        </>
                      )}
                    </ListboxOption>
                  ))}
                </ListboxOptions>
              </div>
            </Listbox>
            {selectedExpiration.kind === "custom" ? (
              <Input
                id={customDateId}
                type="date"
                value={customDate}
                onChange={(e) => {
                  setCustomDate(e.target.value);
                  setFieldError(null);
                }}
                className="mt-2"
                aria-label="Custom expiration date"
              />
            ) : null}
            <FieldDescription>
              Tokens expire at the end of the selected day. Choose no expiration
              only for long-lived automation you will rotate yourself.
            </FieldDescription>
          </Field>

          <Field>
            <FieldLabel id={appAccessId}>App access</FieldLabel>
            <RadioGroup
              aria-labelledby={appAccessId}
              value={scopeMode}
              onValueChange={(value) => {
                setScopeMode(value as TokenScopeMode);
                setFieldError(null);
              }}
              className="gap-3"
            >
              <label className="flex cursor-pointer items-start gap-3">
                <RadioGroupItem value="all" className="mt-0.5" aria-label="All apps" />
                <span className="min-w-0">
                  <span className="block text-sm font-medium text-foreground">
                    All apps
                  </span>
                  <span className="block text-sm text-muted-foreground">
                    This token can use any app your account can access.
                  </span>
                </span>
              </label>
              <label className="flex cursor-pointer items-start gap-3">
                <RadioGroupItem
                  value="select"
                  className="mt-0.5"
                  aria-label="Only select apps"
                />
                <span className="min-w-0">
                  <span className="block text-sm font-medium text-foreground">
                    Only select apps
                  </span>
                  <span className="block text-sm text-muted-foreground">
                    Limit this token to specific apps and optionally operations.
                  </span>
                </span>
              </label>
            </RadioGroup>

            {scopeMode === "select" ? (
              <div className="mt-3 space-y-3 rounded-md border border-alpha p-3">
                <div className="relative">
                  <SearchIcon className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    id={appSearchId}
                    type="search"
                    value={appQuery}
                    onChange={(e) => setAppQuery(e.target.value)}
                    placeholder="Search apps"
                    className="pl-8"
                    aria-label="Search apps"
                    autoComplete="off"
                  />
                </div>

                {integrationsError ? (
                  <p className="text-sm text-ember-500">{integrationsError}</p>
                ) : integrations === null || integrationsQuery.isPending ? (
                  <p className="text-sm text-muted-foreground">Loading apps…</p>
                ) : filteredApps.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No apps found.</p>
                ) : (
                  <ul className="max-h-56 space-y-1 overflow-auto">
                    {filteredApps.map((app) => {
                      const checked = selectedApps[app.name] != null;
                      const label = getIntegrationLabel(app);
                      // Show machine id only when it adds info beyond the label
                      // (e.g. "Jira Cloud" → jira), not GitHub/github twins.
                      const showAppId =
                        normalizeAppIdKey(label) !== normalizeAppIdKey(app.name);
                      return (
                        <li key={app.name}>
                          <label className="flex cursor-pointer items-center gap-2 rounded-sm px-1.5 py-1.5 hover:bg-alpha-5">
                            <Checkbox
                              checked={checked}
                              onCheckedChange={(value) =>
                                toggleApp(app.name, value === true)
                              }
                              aria-label={`Select ${label}`}
                            />
                            <span className="min-w-0 flex-1">
                              <span className="block truncate text-sm font-medium text-foreground">
                                <HighlightMatch text={label} query={appQuery} />
                              </span>
                              {showAppId ? (
                                <span className="block truncate text-xs text-muted-foreground">
                                  <HighlightMatch
                                    text={app.name}
                                    query={appQuery}
                                  />
                                </span>
                              ) : null}
                            </span>
                          </label>
                        </li>
                      );
                    })}
                  </ul>
                )}

                {selectedAppNames.length > 0 ? (
                  <div className="space-y-3 border-t border-alpha pt-3">
                    {selectedAppNames.map((appName) => {
                      const state = selectedApps[appName];
                      const app = integrations?.find((a) => a.name === appName);
                      const label = app?.displayName ?? appName;
                      const opsState = opsByApp[appName];
                      return (
                        <div key={appName} className="space-y-2">
                          <p className="text-sm font-medium text-foreground">
                            {label}
                          </p>
                          {opsState === "loading" || opsState === undefined ? (
                            <p className="text-xs text-muted-foreground">
                              Loading operations…
                            </p>
                          ) : opsState === "error" ? (
                            <p className="text-xs text-ember-500">
                              Could not load operations. Token will grant all
                              operations for this app.
                            </p>
                          ) : opsState.length === 0 ? (
                            <p className="text-xs text-muted-foreground">
                              No listed operations — token grants full access to
                              this app.
                            </p>
                          ) : (
                            <ul className="space-y-1 pl-1">
                              <li>
                                <label className="flex cursor-pointer items-center gap-2 rounded-sm px-1 py-1 hover:bg-alpha-5">
                                  <Checkbox
                                    checked={state.allOperations}
                                    onCheckedChange={(value) =>
                                      setAppAllOperations(
                                        appName,
                                        value === true,
                                      )
                                    }
                                    aria-label={`All operations for ${label}`}
                                  />
                                  <span className="text-sm text-foreground">
                                    All operations
                                  </span>
                                </label>
                              </li>
                              {opsState.map((op) => {
                                const opChecked =
                                  state.allOperations ||
                                  state.operationIds.has(op.id);
                                return (
                                  <li key={op.id}>
                                    <label className="flex cursor-pointer items-center gap-2 rounded-sm px-1 py-1 hover:bg-alpha-5">
                                      <Checkbox
                                        checked={opChecked}
                                        onCheckedChange={(value) =>
                                          toggleOperation(
                                            appName,
                                            op.id,
                                            value === true,
                                          )
                                        }
                                        aria-label={`${op.title ?? op.id} for ${label}`}
                                      />
                                      <span className="min-w-0">
                                        <span className="block truncate text-sm text-foreground">
                                          {op.title ?? op.id}
                                        </span>
                                        {op.title ? (
                                          <span className="block truncate text-xs text-muted-foreground">
                                            {op.id}
                                          </span>
                                        ) : null}
                                      </span>
                                    </label>
                                  </li>
                                );
                              })}
                            </ul>
                          )}
                        </div>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            ) : null}

            {fieldError ? <FieldError>{fieldError}</FieldError> : null}
          </Field>
        </FieldGroup>

        <Button type="submit" disabled={creating} className="w-fit">
          {creating ? "Creating..." : "Create Token"}
        </Button>
      </form>

      {plaintext && (
        <div className="mt-6 space-y-3 rounded-lg border border-gold-300 bg-gold-50 p-5 dark:border-gold-700 dark:bg-gold-950/30">
          <p className="text-sm font-medium text-gold-800 dark:text-gold-300">
            Copy this token now. It will not be shown again.
          </p>
          <CodeBlock
            code={plaintext}
            language="plaintext"
            filename="token"
          />
        </div>
      )}

      {error && <p className="mt-4 text-sm text-ember-500">{error}</p>}
    </>
  );
}
