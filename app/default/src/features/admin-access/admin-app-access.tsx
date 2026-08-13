import {
  useCallback,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from "react";
import { toast } from "sonner";
import type { AppAuthorizationMember } from "@/lib/api";
import { APIError, isAPIErrorStatus } from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Field,
  FieldDescription,
  FieldLabel,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  NavList,
  NavListItem,
  NavListItemLabel,
} from "@/components/ui/nav-list";
import { PageLayout } from "@/components/ui/page-layout";
import { PageLayoutPaneMobileNav } from "@/components/ui/page-layout-pane-mobile-nav";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  SectionHeader,
  SectionHeaderActions,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { SpinnerIcon } from "@/components/icons";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { usePageLayoutAnchorOffsetPx } from "@/lib/page-layout-anchor-offset";
import {
  choiceCardClassName,
  choiceCardContentClassName,
  choiceCardHoverClassName,
  choiceCardRadioClassName,
} from "@/lib/choice-card-chrome";
import { cn } from "@/lib/cn";
import { userFacingError } from "@/lib/user-facing-error";
import {
  useAddAppAccessMutation,
  useAppAuthorizationMembersQuery,
  useAuthorizationResourceTypesQuery,
  useDeleteAppAccessMutation,
} from "@/lib/queries";
import {
  groupRelationshipTuple,
  inferAppAccessRule,
  parseGroupSelector,
  partitionAccessEntries,
  personRelationshipTuple,
  relationshipTupleForMember,
  resourceTypeHasDefaultRole,
  ruleChoiceEnabled,
  type AppAccessEntry,
  type AppAccessRule,
} from "./admin-access";
import {
  ACCESS_RULE_CHOICES,
  ACCESS_RULE_HEADING,
  ACCESS_SECTION_IDS,
  ACCESS_SECTIONS_NAV_LABEL,
  ACCESS_WHO_NAV_LABEL,
  ADD_GROUP_DIALOG_TITLE,
  ADD_GROUP_FIELD_HINT,
  ADD_GROUP_FIELD_LABEL,
  ADD_GROUP_LABEL,
  ADD_PERSON_DIALOG_TITLE,
  ADD_PERSON_FIELD_HINT,
  ADD_PERSON_FIELD_LABEL,
  ADD_PERSON_LABEL,
  DIALOG_CANCEL,
  EMPTY_GROUPS,
  EMPTY_PEOPLE,
  EVERYONE_BLOCKS_REMOVE,
  GROUPS_SECTION_TITLE,
  LOCKED_FROM_CONFIG,
  PEOPLE_SECTION_TITLE,
  REMOVE_ACCESS_LABEL,
  savedOffForEveryone,
  savedOnForGroup,
  savedOnForPerson,
} from "./admin-access-copy";

function errorMessage(error: unknown, fallback: string): string {
  if (error instanceof APIError) return userFacingError(error, fallback);
  if (error instanceof Error && error.message) return error.message;
  return fallback;
}

function AccessEntryRow({
  entry,
  busy,
  onRemove,
}: {
  entry: AppAccessEntry;
  busy: boolean;
  onRemove: () => void;
}) {
  const locked = !entry.mutable;
  return (
    <li
      className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
      data-testid="admin-access-entry"
    >
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-foreground">{entry.label}</p>
        {locked ? (
          <p className="mt-0.5 text-xs text-muted-foreground">{LOCKED_FROM_CONFIG}</p>
        ) : null}
      </div>
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={locked || busy}
        onClick={onRemove}
      >
        {REMOVE_ACCESS_LABEL}
      </Button>
    </li>
  );
}

function AddAccessDialog({
  open,
  title,
  description,
  fieldLabel,
  fieldHint,
  inputType,
  placeholder,
  confirmLabel,
  busy,
  onOpenChange,
  onSubmit,
}: {
  open: boolean;
  title: string;
  description: string;
  fieldLabel: string;
  fieldHint: string;
  inputType: "email" | "text";
  placeholder: string;
  confirmLabel: string;
  busy: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (value: string) => Promise<void>;
}) {
  const [value, setValue] = useState("");
  const [formError, setFormError] = useState<string | null>(null);

  async function handleSubmit(event: FormEvent) {
    event.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      setFormError(`Enter ${fieldLabel.toLowerCase()}.`);
      return;
    }
    if (inputType === "email" && !trimmed.includes("@")) {
      setFormError("Enter a work email.");
      return;
    }
    if (inputType === "text" && !parseGroupSelector(trimmed).id) {
      setFormError("Enter a group id.");
      return;
    }
    setFormError(null);
    try {
      await onSubmit(trimmed);
      setValue("");
      onOpenChange(false);
    } catch (error) {
      setFormError(errorMessage(error, "Couldn't save that change."));
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) {
          setValue("");
          setFormError(null);
        }
        onOpenChange(next);
      }}
    >
      <DialogContent>
        <form onSubmit={handleSubmit} className="grid gap-4">
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>{description}</DialogDescription>
          </DialogHeader>
          <Field>
            <FieldLabel htmlFor="admin-access-input">{fieldLabel}</FieldLabel>
            <Input
              id="admin-access-input"
              type={inputType}
              autoComplete="off"
              value={value}
              onChange={(event) => setValue(event.target.value)}
              placeholder={placeholder}
            />
            <FieldDescription>{fieldHint}</FieldDescription>
            {formError ? (
              <p className="text-sm text-destructive">{formError}</p>
            ) : null}
          </Field>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
            >
              {DIALOG_CANCEL}
            </Button>
            <Button type="submit" loading={busy}>
              {confirmLabel}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

const SECTION_ANCHOR_CLASS =
  "scroll-mt-[var(--page-layout-anchor-offset)]";

function AccessSectionsPane({
  sections,
  activeId,
  onSelect,
}: {
  sections: { id: string; label: string }[];
  activeId: string | null;
  onSelect: (id: string) => void;
}) {
  return (
    <div data-testid="admin-app-toc">
      <NavList aria-label={ACCESS_SECTIONS_NAV_LABEL}>
        {sections.map((item) => (
          <NavListItem
            key={item.id}
            href={`#${item.id}`}
            active={activeId === item.id}
            current="location"
            onClick={(event) => {
              event.preventDefault();
              onSelect(item.id);
            }}
          >
            <NavListItemLabel>{item.label}</NavListItemLabel>
          </NavListItem>
        ))}
      </NavList>
    </div>
  );
}

export function AdminAppAccess({
  appName,
  appLabel,
  heading,
}: {
  appName: string;
  appLabel: string;
  heading: ReactNode;
}) {
  const membersQuery = useAppAuthorizationMembersQuery(appName);
  const resourceTypesQuery = useAuthorizationResourceTypesQuery();
  const addMutation = useAddAppAccessMutation(appName);
  const deleteMutation = useDeleteAppAccessMutation(appName);
  const [draftRule, setDraftRule] = useState<AppAccessRule | null>(null);
  const [groupOpen, setGroupOpen] = useState(false);
  const [personOpen, setPersonOpen] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  const members = membersQuery.data ?? [];
  const forbidden =
    membersQuery.isError && isAPIErrorStatus(membersQuery.error, 403);
  const loadError =
    membersQuery.isError && !forbidden
      ? errorMessage(membersQuery.error, "Couldn't load who can use this app.")
      : null;
  const hasDefaultRole = resourceTypeHasDefaultRole(
    resourceTypesQuery.data ?? [],
  );
  const inferred = inferAppAccessRule({ hasDefaultRole, members });
  const rule = draftRule ?? inferred;
  const { groups, people } = useMemo(
    () => partitionAccessEntries(members),
    [members],
  );
  const busy = addMutation.isPending || deleteMutation.isPending;
  const showWho = rule === "specific";
  const accessReady = !membersQuery.isPending && !resourceTypesQuery.isPending;

  const navSections = useMemo(() => {
    const sections: { id: string; label: string }[] = [
      { id: ACCESS_SECTION_IDS.who, label: ACCESS_WHO_NAV_LABEL },
    ];
    if (showWho && accessReady && !forbidden) {
      sections.push(
        { id: ACCESS_SECTION_IDS.groups, label: GROUPS_SECTION_TITLE },
        { id: ACCESS_SECTION_IDS.people, label: PEOPLE_SECTION_TITLE },
      );
    }
    return sections;
  }, [accessReady, forbidden, showWho]);

  const scrollRootRef = useRef<HTMLElement | null>(null);
  useLayoutEffect(() => {
    scrollRootRef.current = document.documentElement;
  }, []);

  const sectionsKey = navSections.map((item) => item.id).join(",");
  const getEntries = useCallback(() => {
    return navSections.flatMap((item) => {
      const el = document.getElementById(item.id);
      return el
        ? [{ id: item.id, top: el.getBoundingClientRect().top }]
        : [];
    });
  }, [navSections]);

  const tocActivationOffset = usePageLayoutAnchorOffsetPx();
  const { activeId, activate } = useScrollSpy({
    scrollRootRef,
    getEntries,
    sectionsKey,
    activationOffset: tocActivationOffset,
    forceLastAtBottom: true,
    enabled: navSections.length > 0,
    observeWindow: true,
  });

  const onNavSectionSelect = useCallback(
    (id: string) => {
      const el = document.getElementById(id);
      if (!el) return;
      activate(id);
      el.scrollIntoView({ behavior: "smooth", block: "start" });
      setMobileNavOpen(false);
    },
    [activate],
  );

  const accessPane = (
    <AccessSectionsPane
      sections={navSections}
      activeId={activeId}
      onSelect={onNavSectionSelect}
    />
  );
  const accessPaneMobile = (
    <AccessSectionsPane
      sections={navSections}
      activeId={activeId}
      onSelect={onNavSectionSelect}
    />
  );

  async function handleRuleChange(next: AppAccessRule) {
    if (!ruleChoiceEnabled(next, inferred)) return;
    setActionError(null);
    if (next === "specific") {
      setDraftRule("specific");
      return;
    }
    if (next === "no_one") {
      const removable = [...groups, ...people].filter((entry) => entry.mutable);
      try {
        for (const entry of removable) {
          const tuple = relationshipTupleForMember(appName, entry.member);
          if (tuple) await deleteMutation.mutateAsync(tuple);
        }
        setDraftRule(null);
        if (removable.length > 0 && removable.length === groups.length + people.length) {
          toast.success(savedOffForEveryone(appLabel));
        }
      } catch (error) {
        setActionError(errorMessage(error, "Couldn't update who can use this app."));
      }
    }
  }

  async function handleRemove(member: AppAuthorizationMember) {
    if (inferred === "everyone") return;
    const tuple = relationshipTupleForMember(appName, member);
    if (!tuple) return;
    setActionError(null);
    try {
      await deleteMutation.mutateAsync(tuple);
    } catch (error) {
      setActionError(errorMessage(error, "Couldn't remove access."));
    }
  }

  return (
    <PageLayout
      tracks="compact"
      pane={accessPane}
      paneMobile={
        <PageLayoutPaneMobileNav
          open={mobileNavOpen}
          onOpenChange={setMobileNavOpen}
          panelLabel={ACCESS_SECTIONS_NAV_LABEL}
        >
          {accessPaneMobile}
        </PageLayoutPaneMobileNav>
      }
    >
      {heading}
      <div className="space-y-12">
      <section aria-labelledby={ACCESS_SECTION_IDS.who}>
        <SectionHeader>
          <SectionHeaderContent>
            <SectionHeaderTitle
              id={ACCESS_SECTION_IDS.who}
              className={SECTION_ANCHOR_CLASS}
            >
              {ACCESS_RULE_HEADING(appLabel)}
            </SectionHeaderTitle>
          </SectionHeaderContent>
        </SectionHeader>
      {!accessReady ? (
        <p className="mt-3 flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading who can use this app…
        </p>
      ) : (
        <>
      <RadioGroup
        value={rule}
        onValueChange={(value) => void handleRuleChange(value as AppAccessRule)}
        className="mt-3 grid grid-cols-1 gap-2"
        aria-labelledby={ACCESS_SECTION_IDS.who}
        data-testid="admin-access-rule"
      >
        {(
          [
            ["everyone", ACCESS_RULE_CHOICES.everyone],
            ["specific", ACCESS_RULE_CHOICES.specific],
            ["no_one", ACCESS_RULE_CHOICES.noOne],
          ] as const
        ).map(([value, copy]) => {
          const inputId = `admin-access-${value}`;
          const enabled = ruleChoiceEnabled(value, inferred);
          return (
            <Label
              key={value}
              htmlFor={inputId}
              className={cn(choiceCardClassName, choiceCardHoverClassName)}
              data-testid={`admin-access-choice-${value}`}
            >
              <RadioGroupItem
                focusRing="none"
                value={value}
                id={inputId}
                disabled={!enabled || busy || forbidden}
                className={choiceCardRadioClassName}
              />
              <span className={choiceCardContentClassName}>
                <span data-choice-title className="text-sm font-medium text-foreground">
                  {copy.label}
                </span>
                <span data-choice-desc className="text-sm text-muted-foreground">
                  {copy.description}
                </span>
                {value === "everyone" && inferred === "everyone" ? (
                  <span className="text-sm text-muted-foreground">
                    {EVERYONE_BLOCKS_REMOVE}
                  </span>
                ) : null}
                {value === "everyone" && inferred !== "everyone" ? (
                  <span className="text-sm text-muted-foreground">
                    {LOCKED_FROM_CONFIG}
                  </span>
                ) : null}
              </span>
            </Label>
          );
        })}
      </RadioGroup>

      {forbidden ? (
        <p className="text-sm text-muted-foreground" data-testid="admin-access-denied">
          You need admin access to change who can use this app.
        </p>
      ) : null}

      {loadError ? (
        <p className="text-sm text-destructive">{loadError}</p>
      ) : null}

      {actionError ? (
        <p className="text-sm text-destructive">{actionError}</p>
      ) : null}
        </>
      )}
      </section>

      {showWho && !forbidden && !membersQuery.isPending ? (
        <>
          <section aria-labelledby="admin-access-groups">
            <SectionHeader>
              <SectionHeaderContent>
                <SectionHeaderTitle
                  id={ACCESS_SECTION_IDS.groups}
                  className={SECTION_ANCHOR_CLASS}
                >
                  {GROUPS_SECTION_TITLE}
                </SectionHeaderTitle>
              </SectionHeaderContent>
              <SectionHeaderActions>
                <Button
                  type="button"
                  size="sm"
                  onClick={() => setGroupOpen(true)}
                  disabled={busy}
                >
                  {ADD_GROUP_LABEL}
                </Button>
              </SectionHeaderActions>
            </SectionHeader>
            {groups.length === 0 ? (
              <p className="mt-3 text-sm text-muted-foreground">{EMPTY_GROUPS}</p>
            ) : (
              <ul className="mt-3 divide-y divide-border rounded-lg border border-border">
                {groups.map((entry, index) => (
                  <AccessEntryRow
                    key={`${entry.label}:${entry.role}:${index}`}
                    entry={entry}
                    busy={busy}
                    onRemove={() => void handleRemove(entry.member)}
                  />
                ))}
              </ul>
            )}
          </section>

          <section aria-labelledby="admin-access-people">
            <SectionHeader>
              <SectionHeaderContent>
                <SectionHeaderTitle
                  id={ACCESS_SECTION_IDS.people}
                  className={SECTION_ANCHOR_CLASS}
                >
                  {PEOPLE_SECTION_TITLE}
                </SectionHeaderTitle>
              </SectionHeaderContent>
              <SectionHeaderActions>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => setPersonOpen(true)}
                  disabled={busy}
                >
                  {ADD_PERSON_LABEL}
                </Button>
              </SectionHeaderActions>
            </SectionHeader>
            {people.length === 0 ? (
              <p className="mt-3 text-sm text-muted-foreground">{EMPTY_PEOPLE}</p>
            ) : (
              <ul className="mt-3 divide-y divide-border rounded-lg border border-border">
                {people.map((entry, index) => (
                  <AccessEntryRow
                    key={`${entry.label}:${entry.role}:${index}`}
                    entry={entry}
                    busy={busy}
                    onRemove={() => void handleRemove(entry.member)}
                  />
                ))}
              </ul>
            )}
          </section>
        </>
      ) : null}

      <AddAccessDialog
        open={groupOpen}
        title={ADD_GROUP_DIALOG_TITLE}
        description={ACCESS_RULE_CHOICES.specific.description}
        fieldLabel={ADD_GROUP_FIELD_LABEL}
        fieldHint={ADD_GROUP_FIELD_HINT}
        inputType="text"
        placeholder="eng"
        confirmLabel={ADD_GROUP_LABEL}
        busy={addMutation.isPending}
        onOpenChange={setGroupOpen}
        onSubmit={async (value) => {
          await addMutation.mutateAsync(groupRelationshipTuple(appName, value));
          setDraftRule(null);
          toast.success(
            savedOnForGroup(appLabel, parseGroupSelector(value).id || value),
          );
        }}
      />
      <AddAccessDialog
        open={personOpen}
        title={ADD_PERSON_DIALOG_TITLE}
        description={ACCESS_RULE_CHOICES.specific.description}
        fieldLabel={ADD_PERSON_FIELD_LABEL}
        fieldHint={ADD_PERSON_FIELD_HINT}
        inputType="email"
        placeholder="name@example.com"
        confirmLabel={ADD_PERSON_LABEL}
        busy={addMutation.isPending}
        onOpenChange={setPersonOpen}
        onSubmit={async (value) => {
          await addMutation.mutateAsync(personRelationshipTuple(appName, value));
          setDraftRule(null);
          toast.success(savedOnForPerson(appLabel, value));
        }}
      />
      </div>
    </PageLayout>
  );
}
