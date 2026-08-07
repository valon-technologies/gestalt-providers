/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { Plus } from "lucide-react";

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemSeparator,
  ItemTitle,
} from "@/components/ui/item";
import {
  PeoplePicker,
  type PeoplePickerPreset,
  type PersonOption,
} from "@/components/ui/people-picker";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/cn";

/** Sentinel Select value that opens the remove confirmation (not a real role). */
export const MEMBER_ACCESS_REMOVE_VALUE = "__remove__";

export type MemberAccessPerson = {
  id: string;
  name: string;
  email?: string;
  role: string;
  avatarUrl?: string;
  initials?: string;
  /** Static trailing label; no role Select. */
  owner?: boolean;
  /** Hide Remove; treat as immutable even when the surface is enabled. */
  locked?: boolean;
};

export type MemberAccessRole = {
  value: string;
  label: string;
};

/**
 * Directory invite chrome. Omit `invite` for roster-only surfaces
 * (e.g. service-account grants — not directory-addressable).
 * Composes `PeoplePicker` for the person; apps own `searchPeople`.
 */
export type MemberAccessInvite = {
  value: string;
  role: string;
  onValueChange: (
    value: string,
    option?: PersonOption | PeoplePickerPreset,
  ) => void;
  onRoleChange: (value: string) => void;
  onInvite: () => void;
  searchPeople: (query: string) => Promise<PersonOption[]>;
  presets?: PeoplePickerPreset[];
  placeholder?: string;
  searchPlaceholder?: string;
  allowCustomValue?: boolean;
  /** Values to omit from directory results (e.g. people already on the roster). */
  excludeValues?: string[];
};

export type MemberAccessProps = {
  people: MemberAccessPerson[];
  roles: MemberAccessRole[];
  /** When set, renders the invite row above the roster. */
  invite?: MemberAccessInvite | null;
  onRoleChange: (id: string, role: string) => void;
  onRemove: (id: string) => void;
  /** Disables invite controls (when present) and role Selects. */
  disabled?: boolean;
  className?: string;
};

function personInitials(person: MemberAccessPerson): string {
  if (person.initials?.trim()) return person.initials.trim();
  const parts = person.name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    const email = person.email?.trim();
    if (email) return email.slice(0, 2).toUpperCase();
    return "?";
  }
  if (parts.length === 1) return parts[0]!.slice(0, 2).toUpperCase();
  return `${parts[0]![0] ?? ""}${parts[parts.length - 1]![0] ?? ""}`.toUpperCase();
}

function MemberAccess({
  people,
  roles,
  invite = null,
  onRoleChange,
  onRemove,
  disabled = false,
  className,
}: MemberAccessProps) {
  const [pendingRemoveId, setPendingRemoveId] = React.useState<string | null>(null);

  function handleRoleSelect(id: string, value: string) {
    if (value === MEMBER_ACCESS_REMOVE_VALUE) {
      setPendingRemoveId(id);
      return;
    }
    onRoleChange(id, value);
  }

  const showInvite = invite != null;
  const canAdd = Boolean(invite?.value?.trim()) && !disabled;
  const pendingRemove =
    pendingRemoveId == null
      ? null
      : (people.find((person) => person.id === pendingRemoveId) ?? null);

  return (
    <div data-slot="member-access" className={cn("flex flex-col gap-4", className)}>
      {showInvite ? (
        <>
          <div className="flex flex-wrap items-center gap-2">
            <PeoplePicker
              className="min-w-[12rem] flex-1"
              value={invite.value}
              onValueChange={invite.onValueChange}
              searchPeople={invite.searchPeople}
              presets={invite.presets}
              placeholder={invite.placeholder ?? "Select person"}
              searchPlaceholder={invite.searchPlaceholder}
              allowCustomValue={invite.allowCustomValue}
              excludeValues={invite.excludeValues}
              disabled={disabled}
            />
            <Select
              value={invite.role}
              disabled={disabled}
              onValueChange={(value) => {
                if (value) invite.onRoleChange(value);
              }}
            >
              <SelectTrigger className="w-[8.5rem]" size="default" aria-label="Invite role">
                <SelectValue placeholder="Role" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {roles.map((role) => (
                    <SelectItem key={role.value} value={role.value}>
                      {role.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
            <Button
              type="button"
              size="icon"
              variant="outline"
              disabled={!canAdd}
              aria-label="Add member"
              onClick={invite.onInvite}
            >
              <Plus className="size-4" aria-hidden />
            </Button>
          </div>
          {people.length > 0 ? <Separator /> : null}
        </>
      ) : null}

      {people.length > 0 ? (
        <ItemGroup className="overflow-hidden rounded-lg border border-border">
          {people.map((person, index) => {
            const roleLocked = Boolean(person.owner || person.locked || disabled);
            return (
              <React.Fragment key={person.id}>
                {index > 0 ? <ItemSeparator /> : null}
                <Item
                  size="sm"
                  variant="default"
                  className="items-baseline rounded-none border-0"
                  role="listitem"
                >
                  <Avatar aria-hidden size="sm" variant="solid">
                    {person.avatarUrl ? (
                      <AvatarImage src={person.avatarUrl} alt="" />
                    ) : null}
                    <AvatarFallback>{personInitials(person)}</AvatarFallback>
                  </Avatar>
                  <ItemContent>
                    <ItemTitle className="truncate">{person.name}</ItemTitle>
                    {person.email ? (
                      <ItemDescription className="truncate text-xs">
                        {person.email}
                      </ItemDescription>
                    ) : null}
                  </ItemContent>
                  <ItemActions className="self-center">
                    {person.owner ? (
                      <span className="text-sm text-muted-foreground">Owner</span>
                    ) : (
                      <Select
                        value={person.role}
                        disabled={roleLocked}
                        onValueChange={(value) => {
                          if (value) handleRoleSelect(person.id, value);
                        }}
                      >
                        <SelectTrigger
                          className="w-[8.5rem]"
                          size="sm"
                          aria-label={`Role for ${person.name}`}
                        >
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectGroup>
                            {roles.map((role) => (
                              <SelectItem key={role.value} value={role.value}>
                                {role.label}
                              </SelectItem>
                            ))}
                            {!person.locked ? (
                              <SelectItem value={MEMBER_ACCESS_REMOVE_VALUE}>
                                Remove
                              </SelectItem>
                            ) : null}
                          </SelectGroup>
                        </SelectContent>
                      </Select>
                    )}
                  </ItemActions>
                </Item>
              </React.Fragment>
            );
          })}
        </ItemGroup>
      ) : null}

      <AlertDialog
        open={pendingRemoveId != null}
        onOpenChange={(open) => {
          if (!open) setPendingRemoveId(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {pendingRemove ? `Remove ${pendingRemove.name}?` : "Remove access?"}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {pendingRemove
                ? `${pendingRemove.name} will lose access. You can add them again later.`
                : "This person will lose access. You can add them again later."}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => {
                if (!pendingRemoveId) return;
                onRemove(pendingRemoveId);
                setPendingRemoveId(null);
              }}
            >
              Remove
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

export { MemberAccess };
