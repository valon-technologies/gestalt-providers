/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

const MS_PER_DAY = 86_400_000;

function startOfDay(d: Date): number {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
}

/** True when the wire value is a calendar all-day date (`YYYY-MM-DD`, no time). */
export function isAllDayWireDate(value: string): boolean {
  return Boolean(value) && !value.includes("T");
}

/**
 * Parse a wire date into a local `Date` for pickers / formatting.
 * All-day values are midnight local; datetimes use the Instant's local wall clock.
 */
export function parseAppDate(value?: string | null): Date | undefined {
  if (!value?.trim()) return undefined;
  if (isAllDayWireDate(value)) {
    const [y, m, d] = value.split("-").map(Number);
    if (!y || !m || !d) return undefined;
    const date = new Date(y, m - 1, d);
    return Number.isNaN(date.getTime()) ? undefined : date;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

function formatAllDayWire(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

/** Apply a calendar-day selection onto the prior wire value. */
export function applyCalendarDate(nextDay: Date | undefined, previousWire?: string): string {
  if (!nextDay || Number.isNaN(nextDay.getTime())) return "";

  const previous = previousWire?.trim() ? previousWire : "";
  if (!previous || isAllDayWireDate(previous)) {
    return formatAllDayWire(nextDay);
  }

  const prev = parseAppDate(previous);
  const hours = prev?.getHours() ?? 0;
  const minutes = prev?.getMinutes() ?? 0;
  const seconds = prev?.getSeconds() ?? 0;
  const ms = prev?.getMilliseconds() ?? 0;
  const merged = new Date(
    nextDay.getFullYear(),
    nextDay.getMonth(),
    nextDay.getDate(),
    hours,
    minutes,
    seconds,
    ms,
  );
  return merged.toISOString();
}

/**
 * Formats a calendar event start time as a human-readable relative string.
 *
 * | When | Output (separator = "at") |
 * |------|---------------------------|
 * | Today | "Today at 10:30 AM" |
 * | Yesterday | "Yesterday at 11:00 AM" |
 * | Tomorrow | "Tomorrow at 2:00 PM" |
 * | ±2–6 days | "Mon at 9:00 AM" |
 * | Same year | "Jun 20 at 11:00 AM" |
 * | Past year | "Jan 22, 2025 at 11:00 AM" |
 * | All-day | date part only, no time |
 *
 * @param startAt ISO date string ("YYYY-MM-DD" or full datetime)
 * @param options.separator string between date label and time (default "at")
 */
export function formatEventWhen(startAt: string, { separator = "at" }: { separator?: string } = {}): string {
  const date = parseAppDate(startAt);
  if (!date) return startAt;
  const allDay = isAllDayWireDate(startAt);
  const now = new Date();
  const dayDiff = Math.round((startOfDay(date) - startOfDay(now)) / MS_PER_DAY);

  let datePart: string;
  if (dayDiff === 0) datePart = "Today";
  else if (dayDiff === -1) datePart = "Yesterday";
  else if (dayDiff === 1) datePart = "Tomorrow";
  else if (dayDiff > -7 && dayDiff < 7) {
    datePart = date.toLocaleDateString(undefined, { weekday: "short" });
  } else {
    datePart = date.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      ...(date.getFullYear() !== now.getFullYear() ? { year: "numeric" } : {}),
    });
  }

  if (allDay) return datePart;
  const time = date.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
  return `${datePart} ${separator} ${time}`;
}
