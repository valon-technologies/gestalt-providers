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
import {
  CheckboxTree,
  type CheckboxTreeNode,
} from "@/components/ui/checkbox-tree";
import {
  Field,
  FieldDescription,
  FieldError,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group";
import {
  Alert,
  AlertDescription,
} from "@/components/ui/alert";
import { RadioGroup, RadioGroupItem } from "@/components/RadioGroup";
import {
  CalendarIcon,
  CheckIcon,
  ChevronDownIcon,
  CopyIcon,
  SearchIcon,
} from "@/components/icons";
import { Info } from "lucide-react";
import { HighlightMatch } from "@/components/HighlightMatch";
import {
  filterIntegrations,
  getIntegrationLabel,
} from "@/lib/integrationSearch";
import { cn } from "@/lib/cn";

interface TokenCreateFormProps {
  /**
   * Called with the one-time plaintext token after a successful create.
   * `created` carries the durable id + name for session persistence.
   */
  onCreated: (
    plaintext: string,
    created: { id: string; name: string },
  ) => void | Promise<void>;
  /** Controlled token name — persists via parent when provided with onNameChange. */
  name?: string;
  onNameChange?: (name: string) => void;
  /** Uncontrolled initial name when `name` is omitted. */
  defaultName?: string;
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

/** Scope grammar leaf: `app:operation` — matches encodeTokenScopes. */
function operationLeafId(appName: string, operationId: string): string {
  return `${appName}:${operationId}`;
}

function parseOperationLeafId(
  leafId: string,
): { appName: string; operationId: string } | null {
  const sep = leafId.indexOf(":");
  if (sep <= 0) return null;
  return {
    appName: leafId.slice(0, sep),
    operationId: leafId.slice(sep + 1),
  };
}

function listedOperations(
  opsState: IntegrationOperation[] | "loading" | "error" | undefined,
): IntegrationOperation[] | null {
  return Array.isArray(opsState) && opsState.length > 0 ? opsState : null;
}

/** Build CheckboxTree nodes: app parents → operation leaves (Registry Nested tree). */
function buildAppAccessTree(
  selectedAppNames: readonly string[],
  selectedApps: Record<string, SelectedAppState>,
  opsByApp: Record<string, IntegrationOperation[] | "loading" | "error">,
  integrations: { name: string; displayName?: string }[] | null,
): CheckboxTreeNode[] {
  return selectedAppNames
    .map((appName) => {
      const app = integrations?.find((item) => item.name === appName);
      const label = app?.displayName?.trim() || appName;
      const ops = listedOperations(opsByApp[appName]);
      if (!ops) {
        // Loading / error / empty — app is a leaf (bare app scope = all ops).
        return { id: appName, label };
      }
      return {
        id: appName,
        label,
        children: ops.map((op) => ({
          id: operationLeafId(appName, op.id),
          label: op.title?.trim() || op.id,
        })),
      };
    })
    .filter((node) => selectedApps[node.id] != null);
}

/** Derive CheckboxTree leaf value from SelectedAppState + loaded ops. */
function leafValueFromSelectedApps(
  selectedApps: Record<string, SelectedAppState>,
  opsByApp: Record<string, IntegrationOperation[] | "loading" | "error">,
): string[] {
  const leaves: string[] = [];
  for (const [appName, state] of Object.entries(selectedApps)) {
    const ops = listedOperations(opsByApp[appName]);
    if (!ops) {
      leaves.push(appName);
      continue;
    }
    if (state.allOperations) {
      for (const op of ops) {
        leaves.push(operationLeafId(appName, op.id));
      }
      continue;
    }
    for (const opId of state.operationIds) {
      leaves.push(operationLeafId(appName, opId));
    }
  }
  return leaves;
}

/**
 * Map CheckboxTree leaf ids back to SelectedAppState.
 * Unchecking every leaf under an app removes it from the selection set.
 */
function selectedAppsFromLeafValue(
  leafIds: readonly string[],
  previous: Record<string, SelectedAppState>,
  opsByApp: Record<string, IntegrationOperation[] | "loading" | "error">,
): Record<string, SelectedAppState> {
  const byApp = new Map<string, Set<string>>();
  const bareApps = new Set<string>();

  for (const leafId of leafIds) {
    const parsed = parseOperationLeafId(leafId);
    if (parsed) {
      const set = byApp.get(parsed.appName) ?? new Set<string>();
      set.add(parsed.operationId);
      byApp.set(parsed.appName, set);
      continue;
    }
    bareApps.add(leafId);
  }

  const next: Record<string, SelectedAppState> = {};

  for (const appName of bareApps) {
    next[appName] = previous[appName] ?? {
      allOperations: true,
      operationIds: new Set(),
    };
    if (!listedOperations(opsByApp[appName])) {
      next[appName] = { allOperations: true, operationIds: new Set() };
    }
  }

  for (const [appName, operationIds] of byApp) {
    const ops = listedOperations(opsByApp[appName]);
    if (!ops) {
      next[appName] = { allOperations: true, operationIds: new Set() };
      continue;
    }
    const allSelected =
      ops.length > 0 && ops.every((op) => operationIds.has(op.id));
    next[appName] = allSelected
      ? { allOperations: true, operationIds: new Set() }
      : { allOperations: false, operationIds };
  }

  return next;
}

export default function TokenCreateForm({
  onCreated,
  name: nameProp,
  onNameChange,
  defaultName = "",
}: TokenCreateFormProps) {
  const idPrefix = useId();
  const nameId = `${idPrefix}-name`;
  const expirationId = `${idPrefix}-expiration`;
  const customDateId = `${idPrefix}-custom-date`;
  const appAccessId = `${idPrefix}-app-access`;
  const appSearchId = `${idPrefix}-app-search`;

  const isNameControlled = nameProp !== undefined;
  const [nameUncontrolled, setNameUncontrolled] = useState(defaultName);
  const name = isNameControlled ? nameProp : nameUncontrolled;

  function setName(next: string) {
    if (!isNameControlled) {
      setNameUncontrolled(next);
    }
    onNameChange?.(next);
  }

  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [plaintext, setPlaintext] = useState<string | null>(null);
  const [tokenCopied, setTokenCopied] = useState(false);

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
      setTokenCopied(false);
      if (!isNameControlled) {
        setName("");
      }
      setScopeMode("all");
      setSelectedApps({});
      setAppQuery("");
      setExpirationIdSelected("30d");
      setCustomDate("");
      await invalidateTokens();
      await onCreated(result.token, {
        id: result.id,
        name: trimmedName,
      });
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

  const accessTree = useMemo(
    () =>
      buildAppAccessTree(
        selectedAppNames,
        selectedApps,
        opsByApp,
        integrations,
      ),
    [selectedAppNames, selectedApps, opsByApp, integrations],
  );

  const accessTreeValue = useMemo(
    () => leafValueFromSelectedApps(selectedApps, opsByApp),
    [selectedApps, opsByApp],
  );

  const accessTreeStatusNotes: {
    appName: string;
    tone: "muted" | "error";
    text: string;
  }[] = [];
  for (const appName of selectedAppNames) {
    const opsState = opsByApp[appName];
    const app = integrations?.find((item) => item.name === appName);
    const label = app?.displayName?.trim() || appName;
    if (opsState === "loading" || opsState === undefined) {
      accessTreeStatusNotes.push({
        appName,
        tone: "muted",
        text: `Loading operations for ${label}…`,
      });
      continue;
    }
    if (opsState === "error") {
      accessTreeStatusNotes.push({
        appName,
        tone: "error",
        text: `Could not load operations for ${label}. Token will grant all operations for this app.`,
      });
      continue;
    }
    if (opsState.length === 0) {
      accessTreeStatusNotes.push({
        appName,
        tone: "muted",
        text: `No listed operations for ${label} — token grants full access to this app.`,
      });
    }
  }

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
                    <p className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                      Selected apps & permissions
                    </p>
                    {accessTreeStatusNotes.length > 0 ? (
                      <ul className="space-y-1">
                        {accessTreeStatusNotes.map((note) => (
                          <li
                            key={note.appName}
                            className={
                              note.tone === "error"
                                ? "text-xs text-ember-500"
                                : "text-xs text-muted-foreground"
                            }
                          >
                            {note.text}
                          </li>
                        ))}
                      </ul>
                    ) : null}
                    <CheckboxTree
                      tree={accessTree}
                      value={accessTreeValue}
                      onValueChange={(nextLeaves) => {
                        setSelectedApps((prev) =>
                          selectedAppsFromLeafValue(
                            nextLeaves,
                            prev,
                            opsByApp,
                          ),
                        );
                        setFieldError(null);
                      }}
                      showIcons={false}
                      className="max-h-72 max-w-none overflow-auto"
                    />
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
        <div className="mt-6 space-y-2">
          <InputGroup>
            <InputGroupInput
              value={plaintext}
              readOnly
              aria-label="API token"
              className="font-mono text-sm"
              onFocus={(event) => event.currentTarget.select()}
            />
            <InputGroupAddon align="inline-end">
              <InputGroupButton
                size="icon-xs"
                aria-label={tokenCopied ? "Copied" : "Copy token"}
                title={tokenCopied ? "Copied" : "Copy"}
                onClick={() => {
                  void navigator.clipboard.writeText(plaintext).then(() => {
                    setTokenCopied(true);
                    window.setTimeout(() => setTokenCopied(false), 2000);
                  });
                }}
              >
                {tokenCopied ? (
                  <CheckIcon className="size-3.5" />
                ) : (
                  <CopyIcon className="size-3.5" />
                )}
              </InputGroupButton>
            </InputGroupAddon>
          </InputGroup>
          <Alert>
            <Info aria-hidden />
            <AlertDescription className="font-normal">
              We&apos;ll use this token for this example. You can delete it later
              and create a safer one if you want.
            </AlertDescription>
          </Alert>
        </div>
      )}

      {error && <p className="mt-4 text-sm text-ember-500">{error}</p>}
    </>
  );
}
