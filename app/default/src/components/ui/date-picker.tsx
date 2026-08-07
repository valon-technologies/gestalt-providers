/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { endOfMonth, format, startOfMonth, subDays, subMonths } from "date-fns";
import { CalendarIcon } from "lucide-react";
import { type DateRange } from "react-day-picker";

import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import {
  resolvePopoverCollisionPadding,
  useViewportWidthPx,
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

/** Calendar grid constraints forwarded through the picker compositions. */
type CalendarConstraints = Pick<
  React.ComponentProps<typeof Calendar>,
  "captionLayout" | "startMonth" | "endMonth"
>;

type ControlSize = "sm" | "default" | "lg";

type PickerControlWidth = "full" | "intrinsic";

/** Field chrome shared by DatePicker / DateRangePicker (fields.md contract). */
type PickerFieldProps = {
  /**
   * How `Field` sizes direct children. `intrinsic` (default) — fixed trigger widths;
   * `full` — stretch to the field container (stacked forms).
   */
  controlWidth?: PickerControlWidth;
  /** Failed validation — `data-invalid` on Field, `aria-invalid` on the trigger. */
  invalid?: boolean;
  /** Helper copy below the control (`FieldDescription`). */
  description?: React.ReactNode;
  /** Validation message (`FieldError`); joins `aria-describedby` when set. */
  error?: React.ReactNode;
};

function usePickerFieldAdjunctIds(options: {
  error?: React.ReactNode;
  description?: React.ReactNode;
}) {
  const base = React.useId();
  return {
    errorId: options.error ? `${base}-error` : undefined,
    descriptionId: options.description ? `${base}-description` : undefined,
  };
}

function resolvePickerDescribedBy(
  ids: { errorId?: string; descriptionId?: string },
  options: { error?: React.ReactNode; description?: React.ReactNode },
) {
  const parts = [
    options.error && ids.errorId,
    options.description && ids.descriptionId,
  ].filter(Boolean) as string[];
  return parts.length ? parts.join(" ") : undefined;
}

/** Form-field padding — Input uses px-2 at every size; Button `lg` is px-8 for CTAs. */
const formTriggerPadding: Record<ControlSize, string> = {
  sm: "px-2",
  default: "px-2.5",
  lg: "px-2.5",
};

export type DatePickerProps = {
  id?: string;
  label?: React.ReactNode;
  value?: Date;
  defaultValue?: Date;
  onChange?: (date: Date | undefined) => void;
  placeholder?: string;
  disabled?: boolean;
  /** Close the popover when a day is selected (default true). */
  closeOnSelect?: boolean;
  /**
   * When true, re-clicking the selected day keeps it (DayPicker `required`).
   * Use for entity fields — accidental toggle-off must not clear + autosave null.
   * Pair with `clearable` for an explicit Clear control when empty is valid.
   */
  required?: boolean;
  /**
   * Show an explicit Clear control in the popover footer when a date is set.
   * Clears via the dedicated path even when `required` blocks calendar deselect.
   */
  clearable?: boolean;
  /** Label for the clearable footer control (default "Clear"). */
  clearLabel?: string;
  /**
   * Quick-select days beside the calendar (same rail chrome as DateRangePicker).
   * `true` → `buildDatePresets()` (Today); pass a custom list to replace defaults.
   */
  presets?: boolean | DatePreset[];
  /** Matches Select / Button control sizing (control-sizing.md). */
  size?: ControlSize;
  className?: string;
  triggerClassName?: string;
} & CalendarConstraints &
  PickerFieldProps;

export type DatePreset = {
  id: string;
  label: string;
  date: Date;
};

/**
 * Default quick days for single DatePicker. Keep short (Hick's law) — Today is
 * the common entity-field jump; Yesterday / custom points belong in a call-site
 * `presets` list.
 */
export function buildDatePresets(today: Date = new Date()): DatePreset[] {
  return [{ id: "today", label: "Today", date: today }];
}

/**
 * Controllable selection ownership: the parent owns selection when the `value`
 * key is present on props — including `value={undefined}` (cleared). Omitting
 * `value` means the picker owns internal state seeded by `defaultValue`.
 *
 * Do not use `value !== undefined` as the controlled check — that treats a
 * cleared controlled value as uncontrolled and shows stale internal state.
 */
export function resolvePickerValue<T>(options: {
  valuePropPresent: boolean;
  value: T | undefined;
  uncontrolled: T | undefined;
}): T | undefined {
  return options.valuePropPresent ? options.value : options.uncontrolled;
}

/** Range is complete when both endpoints are set (ready to close the popover). */
export function isCompleteDateRange(range: DateRange | undefined): boolean {
  return Boolean(range?.from && range?.to);
}

/** True when either endpoint is set (open-ended wire values count). */
export function hasDateRangeEndpoint(range: DateRange | undefined): boolean {
  return Boolean(range?.from || range?.to);
}

/** Trigger summary for a (possibly open-ended) range — never invents a missing end. */
export function formatDateRangeTriggerLabel(range: DateRange): string {
  if (range.from && range.to) {
    return `${format(range.from, "LLL dd")} – ${format(range.to, "LLL dd")}`;
  }
  if (range.from) return format(range.from, "LLL dd");
  if (range.to) return format(range.to, "LLL dd");
  return "";
}

/**
 * Calendar `month` seed for a range: prefer `from` so rolling presets
 * (Past 7/30 days) keep the start day on-screen; fall back to `to`.
 */
export function resolveDateRangeVisibleMonth(
  range: DateRange | undefined,
  fallback: Date = new Date(),
): Date {
  return range?.from ?? range?.to ?? fallback;
}

/**
 * Dual-month range panel min width (Calendar chrome). Used with popover
 * collision padding to decide when two months cannot fit in the viewport.
 */
export const DATE_RANGE_DUAL_MONTH_MIN_WIDTH_PX = 490;

/**
 * Tailwind `md` (768px) — Calendar months use `flex-col md:flex-row`. Below
 * this, mounting `numberOfMonths={2}` stacks two grids vertically and blows
 * the popover height; keep a single month until side-by-side layout applies.
 */
export const DATE_RANGE_SIDE_BY_SIDE_MONTHS_MIN_WIDTH_PX = 768;

/** Preset rail width (`sm:w-32`) — reserved when presets sit beside the calendar. */
export const DATE_RANGE_PRESET_COLUMN_WIDTH_PX = 128;

/**
 * Tailwind `sm` (640px): below this the preset rail stacks under the calendar
 * (`max-sm:flex-col`), so it must not consume horizontal dual-month budget.
 */
export const DATE_RANGE_PRESET_SIDE_BY_SIDE_MIN_WIDTH_PX = 640;

export type DateRangePreset = {
  id: string;
  label: string;
  range: DateRange;
};

/**
 * Default quick ranges (Calendar `RangeWithPresets` / ReUI pattern).
 * Pure builder — stories and DateRangePicker share one list.
 */
export function buildDateRangePresets(today: Date = new Date()): DateRangePreset[] {
  // Keep the default rail short (Hick's law). Point + rolling + current/previous
  // month cover filter chrome; year ranges and Yesterday are calendar picks or
  // custom `presets` lists — see guidelines/date-range-presets.md.
  return [
    { id: "today", label: "Today", range: { from: today, to: today } },
    {
      id: "last-7-days",
      label: "Past 7 days",
      range: { from: subDays(today, 6), to: today },
    },
    {
      id: "last-30-days",
      label: "Past 30 days",
      range: { from: subDays(today, 29), to: today },
    },
    {
      id: "month-to-date",
      label: "This month",
      range: { from: startOfMonth(today), to: today },
    },
    {
      id: "last-month",
      label: "Previous month",
      range: {
        from: startOfMonth(subMonths(today, 1)),
        to: endOfMonth(subMonths(today, 1)),
      },
    },
  ];
}

/**
 * Prefer the requested month count, but never mount a dual-month panel that is
 * wider than the calendar budget minus popover edge insets — Popover can shift,
 * not shrink, so layout ownership stays on DateRangePicker.
 *
 * Two width signals (do not conflate):
 * - `viewportWidthPx` — Calendar's `md:flex-row` media query (full window).
 * - `calendarBudgetWidthPx` — space left for the month grids after a side-by-side
 *   preset rail (defaults to the viewport). Fit math uses this; the md gate
 *   always uses the viewport so subtracting the rail cannot delay CSS side-by-side.
 */
export function resolveDateRangeMonthCount(options: {
  requested: number;
  viewportWidthPx: number;
  /** Horizontal budget for month grids (viewport minus side-by-side preset rail). */
  calendarBudgetWidthPx?: number;
  dualMonthMinWidthPx?: number;
  collisionPaddingPx?: number;
  /** Calendar `md:flex-row` floor — dual months below this stack vertically. */
  sideBySideMonthsMinWidthPx?: number;
}): number {
  const requested = Math.max(1, Math.floor(options.requested));
  if (requested <= 1) return 1;
  const dualMin = options.dualMonthMinWidthPx ?? DATE_RANGE_DUAL_MONTH_MIN_WIDTH_PX;
  const sideBySideMin =
    options.sideBySideMonthsMinWidthPx ?? DATE_RANGE_SIDE_BY_SIDE_MONTHS_MIN_WIDTH_PX;
  const calendarBudget =
    options.calendarBudgetWidthPx ?? options.viewportWidthPx;
  const pad =
    (options.collisionPaddingPx ?? resolvePopoverCollisionPadding(options.viewportWidthPx)) * 2;
  // Fit the month grids in the calendar budget; md gate uses the true viewport —
  // otherwise subtracting a preset rail makes `md:flex-row` appear "late".
  if (calendarBudget < dualMin + pad) return 1;
  if (options.viewportWidthPx < sideBySideMin) return 1;
  return requested;
}

/**
 * Single-date picker — Popover + Calendar composition (official shadcn Date Picker).
 * Optional `label` uses Field + FieldLabel (caption variant), same contract as other form controls.
 */
function DatePicker(props: DatePickerProps) {
  const {
    id,
    label,
    value,
    defaultValue,
    onChange,
    placeholder = "Pick a date",
    disabled,
    closeOnSelect = true,
    required = false,
    clearable = false,
    clearLabel = "Clear",
    presets,
    size = "default",
    controlWidth = "intrinsic",
    invalid,
    description,
    error,
    captionLayout,
    startMonth,
    endMonth,
    className,
    triggerClassName,
  } = props;
  const [open, setOpen] = React.useState(false);
  const [uncontrolled, setUncontrolled] = React.useState<Date | undefined>(defaultValue);
  const [month, setMonth] = React.useState<Date>(() => new Date());
  const valuePropPresent = "value" in props;
  const date = resolvePickerValue({ valuePropPresent, value, uncontrolled });
  const resolvedPresets =
    presets === true
      ? buildDatePresets()
      : Array.isArray(presets) && presets.length > 0
        ? presets
        : null;

  const setDate = (next: Date | undefined, opts?: { explicitClear?: boolean }) => {
    // Accidental calendar deselect is blocked when required; explicit Clear bypasses.
    if (required && next === undefined && !opts?.explicitClear) return;
    if (!valuePropPresent) setUncontrolled(next);
    onChange?.(next);
  };

  const applyPreset = (presetDate: Date) => {
    setDate(presetDate);
    setMonth(presetDate);
    if (closeOnSelect) setOpen(false);
  };

  const handleClear = () => {
    setDate(undefined, { explicitClear: true });
    setOpen(false);
  };

  const handleOpenChange = (nextOpen: boolean) => {
    if (nextOpen) setMonth(date ?? new Date());
    setOpen(nextOpen);
  };

  const autoId = React.useId();
  const triggerId = id ?? autoId;
  const canClear = clearable && Boolean(date);
  const { errorId, descriptionId } = usePickerFieldAdjunctIds({ error, description });
  const describedBy = resolvePickerDescribedBy(
    { errorId, descriptionId },
    { error, description },
  );

  return (
    <Field
      controlWidth={controlWidth}
      className={cn(controlWidth === "intrinsic" && "w-fit", className)}
      data-invalid={invalid || undefined}
      data-disabled={disabled || undefined}
    >
      {label ? <FieldLabel htmlFor={triggerId}>{label}</FieldLabel> : null}
      <Popover open={open} onOpenChange={handleOpenChange}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            size={size}
            id={triggerId}
            disabled={disabled}
            data-empty={!date}
            aria-invalid={invalid || undefined}
            aria-describedby={describedBy}
            className={cn(
              controlWidth === "full" ? "w-full" : "w-[212px]",
              "justify-between text-left font-normal data-[empty=true]:text-muted-foreground aria-[invalid=true]:border-destructive",
              formTriggerPadding[size],
              triggerClassName,
            )}
          >
            {date ? format(date, "PPP") : <span>{placeholder}</span>}
            <CalendarIcon aria-hidden data-icon="inline-end" />
          </Button>
        </PopoverTrigger>
        <PopoverContent
          className="flex w-auto max-h-(--radix-popover-content-available-height) flex-col overflow-hidden p-0"
          align="start"
        >
          <div className="min-h-0 flex-1 overflow-y-auto">
            <div className="flex max-sm:flex-col">
              {resolvedPresets ? (
                <div className="relative py-4 max-sm:order-1 max-sm:border-t sm:w-32">
                  <div className="h-full sm:border-e">
                    <div className="flex flex-col px-2">
                      {resolvedPresets.map((preset) => (
                        <Button
                          key={preset.id}
                          type="button"
                          className="w-full justify-start"
                          variant="ghost"
                          size="sm"
                          onClick={() => applyPreset(preset.date)}
                        >
                          {preset.label}
                        </Button>
                      ))}
                    </div>
                  </div>
                </div>
              ) : null}
              <Calendar
                mode="single"
                required={required || undefined}
                selected={date}
                month={month}
                onMonthChange={setMonth}
                captionLayout={captionLayout}
                startMonth={startMonth}
                endMonth={endMonth}
                onSelect={(next: Date | undefined) => {
                  setDate(next);
                  if (next) setMonth(next);
                  if (closeOnSelect && next) setOpen(false);
                }}
              />
            </div>
          </div>
          {canClear ? (
            <div className="flex shrink-0 items-center border-t border-border p-3">
              <Button type="button" variant="ghost" size={size} onClick={handleClear}>
                {clearLabel}
              </Button>
            </div>
          ) : null}
        </PopoverContent>
      </Popover>
      {description ? (
        <FieldDescription id={descriptionId}>{description}</FieldDescription>
      ) : null}
      {error ? <FieldError id={errorId}>{error}</FieldError> : null}
    </Field>
  );
}

export type DateRangePickerProps = {
  id?: string;
  label?: React.ReactNode;
  value?: DateRange;
  defaultValue?: DateRange;
  onChange?: (range: DateRange | undefined) => void;
  placeholder?: string;
  disabled?: boolean;
  numberOfMonths?: number;
  /**
   * `apply` (default) — calendar edits a draft; Apply commits via `onChange`.
   * Dismiss / Cancel discards the draft. Use for filters that reload data.
   * `live` — `onChange` on every selection (closeOnSelect still applies).
   */
  commitMode?: "apply" | "live";
  /** Only used when `commitMode="live"`. Close when both ends become set (default true). */
  closeOnSelect?: boolean;
  applyLabel?: string;
  cancelLabel?: string;
  /** Apply-mode clear control (DataTable date filters). Default "Clear filter". */
  clearLabel?: string;
  /**
   * Empty-trigger label tone. `muted` (default) for form placeholders
   * ("Pick a date") — active state shows dates only (FieldLabel names the field).
   * `foreground` = filter chip chrome: placeholder is the field title and stays
   * visible beside the dates (FacetedFilter pattern).
   */
  emptyLabelTone?: "muted" | "foreground";
  /**
   * Quick-select ranges beside the calendar (Calendar `RangeWithPresets` layout).
   * `true` → `buildDateRangePresets()`; pass a custom list to replace defaults.
   */
  presets?: boolean | DateRangePreset[];
  /** Matches Select / Button control sizing (control-sizing.md). */
  size?: ControlSize;
  className?: string;
  triggerClassName?: string;
} & CalendarConstraints &
  PickerFieldProps;

/**
 * Date-range picker — Popover + Calendar `mode="range"`.
 *
 * Default `commitMode="apply"` keeps draft selection inside the popover until
 * Apply, so parents that reload on `onChange` are not interrupted mid-pick.
 */
function DateRangePicker(props: DateRangePickerProps) {
  const {
    id,
    label,
    value,
    defaultValue,
    onChange,
    placeholder = "Pick a date",
    disabled,
    numberOfMonths = 2,
    commitMode = "apply",
    closeOnSelect = true,
    applyLabel = "Apply",
    cancelLabel = "Cancel",
    clearLabel = "Clear filter",
    emptyLabelTone = "muted",
    presets,
    size = "default",
    controlWidth = "intrinsic",
    invalid,
    description,
    error,
    captionLayout,
    startMonth,
    endMonth,
    className,
    triggerClassName,
  } = props;
  const [open, setOpen] = React.useState(false);
  const [uncontrolled, setUncontrolled] = React.useState<DateRange | undefined>(defaultValue);
  // Apply-mode session draft: seeded from committed on open, edited in the
  // calendar, committed only via Apply. `undefined` means cleared — never
  // coalesce to committed (that snaps the grid while Apply stays disabled).
  // Do not clear draft on dismiss: Radix Presence keeps the popover mounted
  // through the exit animation; clearing (or binding selection to `open`)
  // snaps the fading grid back to committed. Next open re-seeds instead.
  const [draft, setDraft] = React.useState<DateRange | undefined>(undefined);
  const [month, setMonth] = React.useState<Date>(() => new Date());
  const valuePropPresent = "value" in props;
  const committed = resolvePickerValue({ valuePropPresent, value, uncontrolled });
  const commitOnApply = commitMode === "apply";
  // Apply mode: calendar always binds to session draft (including during
  // dismiss animation). Live mode: calendar binds to committed. Trigger
  // always shows committed.
  const selectedRange = commitOnApply ? draft : committed;
  const resolvedPresets =
    presets === true
      ? buildDateRangePresets()
      : Array.isArray(presets) && presets.length > 0
        ? presets
        : null;
  const viewportWidthPx = useViewportWidthPx();
  // Preset rail only costs horizontal width when side-by-side (≥ sm); stacked
  // presets sit under the calendar and must not suppress dual-month.
  const presetRailWidthPx =
    resolvedPresets && viewportWidthPx >= DATE_RANGE_PRESET_SIDE_BY_SIDE_MIN_WIDTH_PX
      ? DATE_RANGE_PRESET_COLUMN_WIDTH_PX
      : 0;
  const visibleMonths = resolveDateRangeMonthCount({
    requested: numberOfMonths,
    viewportWidthPx,
    calendarBudgetWidthPx: viewportWidthPx - presetRailWidthPx,
  });

  const commitRange = (next: DateRange | undefined) => {
    if (!valuePropPresent) setUncontrolled(next);
    onChange?.(next);
  };

  const setLiveRange = (next: DateRange | undefined) => {
    commitRange(next);
    // Close only when this interaction *completes* the range — not when the
    // popover already showed a complete selection and the first click resets
    // or re-emits endpoints (would dismiss before the user picks a new `to`).
    if (
      closeOnSelect &&
      isCompleteDateRange(next) &&
      !isCompleteDateRange(committed)
    ) {
      setOpen(false);
    }
  };

  const applyPreset = (range: DateRange) => {
    if (commitOnApply) setDraft(range);
    else setLiveRange(range);
    setMonth(resolveDateRangeVisibleMonth(range));
  };

  const handleOpenChange = (nextOpen: boolean) => {
    if (nextOpen) {
      // Seed draft from committed only when opening — discard is "do not
      // commit", not "erase the session visual before Presence unmounts".
      if (commitOnApply) setDraft(committed);
      setMonth(resolveDateRangeVisibleMonth(committed));
      setOpen(true);
      return;
    }
    setOpen(false);
  };

  const handleApply = () => {
    if (!isCompleteDateRange(draft)) return;
    commitRange(draft);
    setOpen(false);
  };

  const handleCancel = () => {
    setOpen(false);
  };

  // Apply-mode clear commits empty (FacetedFilter "Clear filter") — Apply never
  // emits incomplete/empty, so this is the only path to drop a committed range.
  const handleClear = () => {
    commitRange(undefined);
    setDraft(undefined);
    setOpen(false);
  };

  const autoId = React.useId();
  const triggerId = id ?? autoId;
  const { errorId, descriptionId } = usePickerFieldAdjunctIds({ error, description });
  const describedBy = resolvePickerDescribedBy(
    { errorId, descriptionId },
    { error, description },
  );
  // Trigger always shows the committed value — draft stays in the popover until Apply.
  const triggerRange = committed;
  const triggerHasRange = hasDateRangeEndpoint(triggerRange);
  const triggerRangeLabel = triggerRange ? formatDateRangeTriggerLabel(triggerRange) : "";
  // Filter chip chrome: placeholder is the field title and stays beside dates.
  const filterChipChrome = emptyLabelTone === "foreground";
  const canClear =
    commitOnApply &&
    (hasDateRangeEndpoint(committed) || hasDateRangeEndpoint(draft));

  return (
    <Field
      controlWidth={controlWidth}
      className={cn(controlWidth === "intrinsic" && "w-fit", className)}
      data-invalid={invalid || undefined}
      data-disabled={disabled || undefined}
    >
      {label ? <FieldLabel htmlFor={triggerId}>{label}</FieldLabel> : null}
      <Popover open={open} onOpenChange={handleOpenChange}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            size={size}
            id={triggerId}
            disabled={disabled}
            data-empty={!triggerHasRange}
            aria-invalid={invalid || undefined}
            aria-describedby={describedBy}
            className={cn(
              // Fixed width; clip on the value span — not the trigger. Overflow on
              // the button synthesizes baseline from the box bottom (avatar.md).
              controlWidth === "full" ? "w-full" : "w-[280px]",
              "justify-between text-left font-normal aria-[invalid=true]:border-destructive",
              formTriggerPadding[size],
              emptyLabelTone === "muted" && "data-[empty=true]:text-muted-foreground",
              triggerClassName,
            )}
          >
            {filterChipChrome ? (
              // gap-1.5: same rhythm as Button — nesting would otherwise collapse
              // title / divider / range to zero gap (FacetedFilter chip spacing).
              <span className="flex min-w-0 flex-1 items-baseline gap-1.5 overflow-hidden">
                <span className="shrink-0">{placeholder}</span>
                {triggerHasRange ? (
                  <>
                    <span aria-hidden className="mx-0.5 h-4 w-px shrink-0 self-center bg-border" />
                    {/* min-w-0: flex items default to min-width:auto and block truncate (harden.md). */}
                    <span className="min-w-0 truncate">{triggerRangeLabel}</span>
                  </>
                ) : null}
              </span>
            ) : triggerHasRange ? (
              <span className="min-w-0 flex-1 overflow-hidden truncate">
                {triggerRangeLabel}
              </span>
            ) : (
              <span className="min-w-0 flex-1 overflow-hidden truncate">{placeholder}</span>
            )}
            <CalendarIcon aria-hidden data-icon="inline-end" />
          </Button>
        </PopoverTrigger>
        <PopoverContent
          className="flex w-auto max-h-(--radix-popover-content-available-height) flex-col overflow-hidden p-0"
          align="start"
        >
          <div className="min-h-0 flex-1 overflow-y-auto">
            <div className="flex max-sm:flex-col">
              {resolvedPresets ? (
                <div className="relative py-4 max-sm:order-1 max-sm:border-t sm:w-32">
                  <div className="h-full sm:border-e">
                    <div className="flex flex-col px-2">
                      {resolvedPresets.map((preset) => (
                        <Button
                          key={preset.id}
                          type="button"
                          className="w-full justify-start"
                          size="sm"
                          variant="ghost"
                          onClick={() => applyPreset(preset.range)}
                        >
                          {preset.label}
                        </Button>
                      ))}
                    </div>
                  </div>
                </div>
              ) : null}
              <Calendar
                mode="range"
                {...(resolvedPresets
                  ? { month, onMonthChange: setMonth }
                  : {
                      defaultMonth: resolveDateRangeVisibleMonth(
                        selectedRange ?? committed,
                      ),
                    })}
                selected={selectedRange}
                onSelect={commitOnApply ? setDraft : setLiveRange}
                numberOfMonths={visibleMonths}
                captionLayout={captionLayout}
                startMonth={startMonth}
                endMonth={endMonth}
              />
            </div>
          </div>
          {commitOnApply ? (
            <div className="flex shrink-0 items-center gap-2 border-t border-border p-3">
              {canClear ? (
                <Button
                  type="button"
                  variant="ghost"
                  size={size}
                  className="mr-auto"
                  onClick={handleClear}
                >
                  {clearLabel}
                </Button>
              ) : (
                <span className="mr-auto" />
              )}
              <Button type="button" variant="ghost" size={size} onClick={handleCancel}>
                {cancelLabel}
              </Button>
              <Button
                type="button"
                size={size}
                disabled={!isCompleteDateRange(draft)}
                onClick={handleApply}
              >
                {applyLabel}
              </Button>
            </div>
          ) : null}
        </PopoverContent>
      </Popover>
      {description ? (
        <FieldDescription id={descriptionId}>{description}</FieldDescription>
      ) : null}
      {error ? <FieldError id={errorId}>{error}</FieldError> : null}
    </Field>
  );
}

export { DatePicker, DateRangePicker };
export type { DateRange };
