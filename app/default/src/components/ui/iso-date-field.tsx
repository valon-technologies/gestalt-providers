/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import type { ReactNode } from "react";

import { DatePicker, type DatePickerProps, type DatePreset } from "@/components/ui/date-picker";
import { applyCalendarDate, parseAppDate } from "@/lib/date";

export type IsoDateFieldProps = {
  id?: string;
  label?: ReactNode;
  /** Wire value: ISO datetime or all-day `YYYY-MM-DD` (or empty). */
  value: string;
  onChange: (wire: string) => void;
  placeholder?: string;
  disabled?: boolean;
  /**
   * Default true — re-clicking the selected day must not clear the field
   * (autosave would persist null). Pair with `clearable` for intentional empty.
   */
  required?: boolean;
  /**
   * Default true — explicit Clear in the picker footer when a date is set.
   * Empty wire (`""`) remains a valid undated state.
   */
  clearable?: boolean;
  clearLabel?: string;
  presets?: boolean | DatePreset[];
  /** Matches sibling form controls (control-sizing.md). */
  size?: DatePickerProps["size"];
  /**
   * Default `full` — stacked form fields stretch to the container.
   * Use `intrinsic` only for compact toolbar pickers.
   */
  controlWidth?: DatePickerProps["controlWidth"];
  /** Failed validation — forwarded to DatePicker field chrome. */
  invalid?: boolean;
  description?: React.ReactNode;
  error?: React.ReactNode;
  className?: string;
  triggerClassName?: string;
};

export function IsoDateField({
  id,
  label,
  value,
  onChange,
  placeholder = "Pick a date",
  disabled,
  required = true,
  clearable = true,
  clearLabel,
  presets,
  size,
  controlWidth = "full",
  invalid,
  description,
  error,
  className,
  triggerClassName,
}: IsoDateFieldProps) {
  return (
    <DatePicker
      id={id}
      label={label}
      value={parseAppDate(value)}
      onChange={(date) => {
        if (!date) {
          if (required && !clearable) return;
          onChange("");
          return;
        }
        onChange(applyCalendarDate(date, value));
      }}
      placeholder={placeholder}
      disabled={disabled}
      required={required}
      clearable={clearable}
      clearLabel={clearLabel}
      presets={presets}
      size={size}
      controlWidth={controlWidth}
      invalid={invalid}
      description={description}
      error={error}
      className={className}
      triggerClassName={triggerClassName}
    />
  );
}
