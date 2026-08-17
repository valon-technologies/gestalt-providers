import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  AvatarGroup,
  AvatarGroupCount,
  AvatarGroupItem,
} from "@/components/ui/avatar-group";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AccessGroupBadge } from "./access-group-badge";
import type { AppAccessEntry } from "./admin-access";
import { ACCESS_LIST_STATUS } from "./admin-access-copy";

const VISIBLE_PEOPLE = 5;

function rosterInitials(label: string): string {
  const trimmed = label.trim();
  if (!trimmed) return "?";
  if (trimmed.includes("@")) {
    const local = trimmed.split("@")[0] ?? trimmed;
    const parts = local.split(/[._-]+/).filter(Boolean);
    if (parts.length >= 2) {
      return `${parts[0]![0] ?? ""}${parts[1]![0] ?? ""}`.toUpperCase();
    }
    return local.slice(0, 2).toUpperCase();
  }
  const parts = trimmed.split(/\s+/).filter(Boolean);
  if (parts.length === 1) return parts[0]!.slice(0, 2).toUpperCase();
  return `${parts[0]![0] ?? ""}${parts[parts.length - 1]![0] ?? ""}`.toUpperCase();
}

export function AdminAccessStatus({
  status,
  groups,
  people,
}: {
  status: string;
  groups: AppAccessEntry[];
  people: AppAccessEntry[];
}) {
  const showRoster =
    status !== ACCESS_LIST_STATUS.everyone &&
    status !== ACCESS_LIST_STATUS.noOne &&
    (groups.length > 0 || people.length > 0);
  if (!showRoster) {
    const unavailable = status === ACCESS_LIST_STATUS.unavailable;
    return (
      <span
        className={
          unavailable
            ? "text-xs text-muted-foreground"
            : "text-sm text-muted-foreground"
        }
      >
        {status}
      </span>
    );
  }

  const visiblePeople = people.slice(0, VISIBLE_PEOPLE);
  const overflow = people.length - visiblePeople.length;
  const whoLabel = [
    ...groups.map((entry) => entry.label),
    people.length === 1 ? "1 person" : people.length > 0 ? `${people.length} people` : null,
  ]
    .filter(Boolean)
    .join(", ");

  return (
    <div
      data-no-row-click
      data-testid="admin-access-status"
      role="group"
      aria-label={whoLabel}
      className="flex items-center gap-1.5"
    >
      {groups.map((entry) => (
        <AccessGroupBadge
          key={entry.member.selectorValue ?? entry.label}
          label={entry.label}
          className="max-w-32"
        />
      ))}
      {visiblePeople.length > 0 ? (
        <TooltipProvider delayDuration={0}>
          <AvatarGroup variant="motion" aria-label="People">
            {visiblePeople.map((entry) => (
              <AvatarGroupItem
                key={entry.member.selectorValue ?? entry.label}
                tooltip={entry.label}
              >
                <Avatar size="sm">
                  <AvatarFallback>{rosterInitials(entry.label)}</AvatarFallback>
                </Avatar>
              </AvatarGroupItem>
            ))}
            {overflow > 0 ? (
              <AvatarGroupCount size="sm">
                +{overflow}
              </AvatarGroupCount>
            ) : null}
          </AvatarGroup>
        </TooltipProvider>
      ) : null}
    </div>
  );
}
