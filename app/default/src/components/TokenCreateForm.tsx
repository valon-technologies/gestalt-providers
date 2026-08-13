import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
} from "@headlessui/react";
import * as React from "react";
import { useId, useImperativeHandle, useMemo, useState } from "react";
import {
  createToken,
  getIntegrationOperations,
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
  buildCatalogAccessTree,
  leafValueFromSelectedApps,
  selectedAppsFromLeafValue,
  type OpsByApp,
  type SelectedAppState,
} from "@/lib/token-scope-selection";
import {
  appsCatalogQueryStatus,
  useIntegrationsQuery,
  useInvalidateTokens,
} from "@/lib/queries";
import ErrorNotice from "@/components/ErrorNotice";
import {
  APPS_CATALOG_UNAVAILABLE,
  userFacingError,
} from "@/lib/user-facing-error";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { CheckboxTree } from "@/components/ui/checkbox-tree";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldError,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Separator } from "@/components/ui/separator";
import {
  CalendarIcon,
  CheckIcon,
  ChevronDownIcon,
  CopyIcon,
  SearchIcon,
} from "@/components/icons";
import { SelectionCheck } from "@/components/ui/selection-check";
import { Spinner } from "@/components/ui/spinner";
import { IsoDateField } from "@/components/ui/iso-date-field";
import { Info } from "lucide-react";
import { filterIntegrations } from "@/lib/integrationSearch";
import { cn } from "@/lib/cn";

interface TokenCreateFormProps {
  /**
   * Called with the one-time plaintext token after a successful create.
   * `created` carries the durable id + name for session persistence.
   */
  onCreated?: (
    plaintext: string,
    created: { id: string; name: string },
  ) => void | Promise<void>;
  /**
   * Fired when the one-time secret reveal block appears or clears.
   * Surfaces that own chrome (Settings title / Cancel) should derive from this
   * instead of mirroring plaintext state locally.
   */
  onRevealChange?: (revealed: boolean) => void;
  /** Controlled token name — persists via parent when provided with onNameChange. */
  name?: string;
  onNameChange?: (name: string) => void;
  /** Uncontrolled initial name when `name` is omitted. */
  defaultName?: string;
  /** When false, omit the submit button (caller creates on its own schedule). */
  showSubmit?: boolean;
  /** Settings stacks labels; Build authorize uses inline label + control rows. */
  fieldOrientation?: "vertical" | "horizontal";
  /** When false, skip the post-create plaintext copy block. */
  showPlaintextResult?: boolean;
  /**
   * Alert under the one-time secret. Defaults to settings-native one-time
   * secret handling. Build tutorial framing must be passed explicitly when
   * Build shows plaintext (Build normally uses `showPlaintextResult={false}`).
   */
  plaintextResultDescription?: string;
  /** Optional actions rendered under the plaintext result (e.g. Done). */
  plaintextResultActions?: React.ReactNode;
  /** Optional control rendered beside the submit button (e.g. Cancel). */
  submitAccessory?: React.ReactNode;
  /** Optional max width for token name + expiration controls (e.g. `max-w-sm`). */
  controlsClassName?: string;
  /** Width for the “only select apps” bordered picker panel. */
  appAccessPanelClassName?: string;
  /** Width for the actions divider + Cancel/Create row. */
  actionsClassName?: string;
  /** Applied to the create form shell (fields + divider + actions share this width). */
  className?: string;
}

/** Safe default when a caller shows plaintext without surface-specific copy. */
const DEFAULT_PLAINTEXT_RESULT_DESCRIPTION =
  "Copy this token now. We won't show the full value again. Store it in your secret manager or shell environment.";

export type TokenCreateFormHandle = {
  create: () => Promise<boolean>;
};

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

const TokenCreateForm = React.forwardRef<
  TokenCreateFormHandle,
  TokenCreateFormProps
>(function TokenCreateForm(
  {
    onCreated,
    onRevealChange,
    name: nameProp,
    onNameChange,
    defaultName = "",
    showSubmit = true,
    fieldOrientation = "vertical",
    showPlaintextResult = true,
    plaintextResultDescription = DEFAULT_PLAINTEXT_RESULT_DESCRIPTION,
    plaintextResultActions,
    submitAccessory,
    controlsClassName,
    appAccessPanelClassName,
    actionsClassName,
    className,
  },
  ref,
) {
  const idPrefix = useId();
  const nameId = `${idPrefix}-name`;
  const expirationId = `${idPrefix}-expiration`;
  const customDateId = `${idPrefix}-custom-date`;
  const appAccessId = `${idPrefix}-app-access`;
  const appAccessAllId = `${idPrefix}-app-access-all`;
  const appAccessSelectId = `${idPrefix}-app-access-select`;
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
  const [opsByApp, setOpsByApp] = useState<OpsByApp>({});

  const invalidateTokens = useInvalidateTokens();
  const integrationsQuery = useIntegrationsQuery({
    enabled: scopeMode === "select",
  });
  const catalog = appsCatalogQueryStatus(integrationsQuery);
  const integrations =
    catalog.status === "loading" && catalog.integrations.length === 0
      ? null
      : catalog.integrations;
  const integrationsError =
    catalog.status === "unavailable"
      ? userFacingError(catalog.error, APPS_CATALOG_UNAVAILABLE)
      : null;
  const blockingCatalogError =
    Boolean(integrationsError) &&
    (integrations === null || integrations.length === 0);

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

  async function runCreate(): Promise<boolean> {
    const trimmedName = name.trim();
    if (!trimmedName) return false;

    const selections = buildSelections();
    if (!hasEffectiveScopes(scopeMode, selections)) {
      setFieldError("Select at least one app.");
      return false;
    }

    if (
      selectedExpiration.kind === "custom" &&
      expiresInFromChoice({ kind: "custom", date: customDate }) == null
    ) {
      setFieldError("Choose a future expiration date.");
      return false;
    }

    setFieldError(null);
    setCreating(true);
    setError(null);
    if (showPlaintextResult) {
      setPlaintext(null);
      onRevealChange?.(false);
    }

    const scopes = encodeTokenScopes(scopeMode, selections);
    const expiresIn = expiresInFromChoice(buildExpirationChoice());

    try {
      const result = await createToken(trimmedName, scopes, expiresIn);
      if (showPlaintextResult) {
        setPlaintext(result.token);
        setTokenCopied(false);
        onRevealChange?.(true);
      }
      if (!isNameControlled) {
        setName("");
      }
      setScopeMode("all");
      setSelectedApps({});
      setAppQuery("");
      setExpirationIdSelected("30d");
      setCustomDate("");
      await invalidateTokens();
      await onCreated?.(result.token, {
        id: result.id,
        name: trimmedName,
      });
      return true;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create token");
      return false;
    } finally {
      setCreating(false);
    }
  }

  useImperativeHandle(
    ref,
    () => ({
      create: runCreate,
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- runCreate closes over latest draft state
    [
      name,
      scopeMode,
      selectedApps,
      selectedExpiration,
      customDate,
      showPlaintextResult,
    ],
  );

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    await runCreate();
  }

  const filteredApps = useMemo(
    () => filterIntegrations(integrations ?? [], appQuery),
    [integrations, appQuery],
  );

  const selectedAppNames = Object.keys(selectedApps);

  const accessTree = useMemo(
    () => buildCatalogAccessTree(filteredApps, opsByApp),
    [filteredApps, opsByApp],
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

  const showPlaintext = showPlaintextResult && plaintext != null;

  return (
    <>
      {!showPlaintext ? (
        <form
          onSubmit={handleSubmit}
          className={cn("flex w-full flex-col gap-6", className)}
        >
          <FieldGroup className="gap-5">
            <Field
              orientation={fieldOrientation}
              controlWidth={controlsClassName ? "intrinsic" : "full"}
            >
              <FieldLabel htmlFor={nameId}>Token name</FieldLabel>
              <FieldContent className={controlsClassName}>
                <Input
                  id={nameId}
                  name="name"
                  type="text"
                  required
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="e.g. ci-pipeline"
                  autoComplete="off"
                  className="w-full"
                />
              </FieldContent>
            </Field>

            <Field
              orientation={fieldOrientation}
              controlWidth={controlsClassName ? "intrinsic" : "full"}
            >
              <FieldLabel id={expirationId}>Expiration</FieldLabel>
              <FieldContent className={controlsClassName}>
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
                        "relative flex w-full items-center gap-2 rounded-md border border-input bg-transparent px-3 py-2 text-left text-sm text-foreground",
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
                                  <SelectionCheck tone="solid" />
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
                  <IsoDateField
                    id={customDateId}
                    label="Custom expiration date"
                    value={customDate}
                    onChange={(wire) => {
                      setCustomDate(wire);
                      setFieldError(null);
                    }}
                    className="mt-2"
                    controlWidth="full"
                    required
                    clearable
                  />
                ) : null}
                {fieldOrientation === "vertical" ? (
                  <FieldDescription>
                    Tokens expire at the end of the selected day. Choose no
                    expiration only for long-lived automation you will rotate
                    yourself.
                  </FieldDescription>
                ) : null}
              </FieldContent>
            </Field>

            <Field orientation={fieldOrientation}>
              <FieldLabel id={appAccessId}>App access</FieldLabel>
              <FieldContent>
                <RadioGroup
                  aria-labelledby={appAccessId}
                  value={scopeMode}
                  onValueChange={(value) => {
                    setScopeMode(value as TokenScopeMode);
                    setFieldError(null);
                  }}
                  className="gap-3"
                >
                  <div className="flex items-start gap-2">
                    <RadioGroupItem
                      value="all"
                      id={appAccessAllId}
                      className="mt-0.5"
                    />
                    <div className="grid min-w-0 gap-1">
                      <Label htmlFor={appAccessAllId} variant="inline">
                        All apps
                      </Label>
                      <p className="text-sm text-muted-foreground">
                        This token can use any app your account can access.
                      </p>
                    </div>
                  </div>
                  <div className="space-y-3">
                    <div className="flex items-start gap-2">
                      <RadioGroupItem
                        value="select"
                        id={appAccessSelectId}
                        className="mt-0.5"
                      />
                      <div className="grid min-w-0 gap-1">
                        <Label htmlFor={appAccessSelectId} variant="inline">
                          Only select apps
                        </Label>
                        <p className="text-sm text-muted-foreground">
                          Limit this token to specific apps and optionally
                          operations.
                        </p>
                      </div>
                    </div>

                    {scopeMode === "select" ? (
                      <Card
                        variant="outline"
                        className={cn(
                          "ms-6 overflow-hidden p-0",
                          appAccessPanelClassName ?? "max-w-md",
                        )}
                      >
                        <div className="px-3 pt-3">
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
                        </div>

                        {integrationsError ? (
                          <div className="px-3 pb-3 pt-2">
                            <ErrorNotice
                              message={integrationsError}
                              retrying={integrationsQuery.isFetching}
                              onRetry={() => {
                                void integrationsQuery.refetch();
                              }}
                            />
                          </div>
                        ) : null}
                        {blockingCatalogError ? null : integrations === null ? (
                          <p className="flex items-center gap-1.5 px-3 pb-3 pt-2 text-sm text-muted-foreground">
                            <Spinner className="size-3" aria-hidden />
                            Loading apps…
                          </p>
                        ) : filteredApps.length === 0 ? (
                          <p className="px-3 pb-3 pt-2 text-sm text-muted-foreground">
                            No apps found.
                          </p>
                        ) : (
                          <>
                            {accessTreeStatusNotes.length > 0 ? (
                              <ul className="space-y-1 px-3 pt-2">
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
                            {/* Padding lives on an inner wrapper — not the
                                overflow scrollport — so the toggle's outward
                                focus-ring is not clipped (focus-ring.md). */}
                            <div className="max-h-[min(24rem,50vh)] overflow-y-auto py-1.5">
                              <div className="px-3">
                                <CheckboxTree
                                  tree={accessTree}
                                  value={accessTreeValue}
                                  density="condensed"
                                  className="max-w-none"
                                  onFolderExpand={(appName) => {
                                    ensureOpsLoaded(appName);
                                  }}
                                  onValueChange={(nextLeaves) => {
                                    const next = selectedAppsFromLeafValue(
                                      nextLeaves,
                                      selectedApps,
                                      opsByApp,
                                    );
                                    setSelectedApps(next);
                                    for (const appName of Object.keys(next)) {
                                      ensureOpsLoaded(appName);
                                    }
                                    setFieldError(null);
                                  }}
                                />
                              </div>
                            </div>
                          </>
                        )}
                      </Card>
                    ) : null}
                  </div>
                </RadioGroup>

                {fieldError ? <FieldError>{fieldError}</FieldError> : null}
              </FieldContent>
            </Field>
          </FieldGroup>

          {showSubmit || submitAccessory ? (
            <div className={cn("space-y-6", actionsClassName)}>
              <Separator />
              <div className="flex flex-row flex-nowrap items-center justify-end gap-3">
                {submitAccessory}
                {showSubmit ? (
                  <Button
                    type="submit"
                    loading={creating}
                    className="w-auto shrink-0"
                  >
                    Create token
                  </Button>
                ) : null}
              </div>
            </div>
          ) : null}
        </form>
      ) : null}

      {showPlaintext ? (
        <div className="space-y-4">
          <div className="space-y-2">
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
                {plaintextResultDescription}
              </AlertDescription>
            </Alert>
          </div>
          {plaintextResultActions}
        </div>
      ) : null}

      {error && <p className="mt-4 text-sm text-destructive">{error}</p>}
    </>
  );
});

export default TokenCreateForm;
