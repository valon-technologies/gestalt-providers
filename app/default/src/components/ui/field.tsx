/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { Label, labelVariants } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";

// Field is the labeled-control unit: label + control + optional description/error.
// Invalid: data-invalid on Field + aria-invalid + aria-describedby → FieldError id
//   on the control.
// Disabled: native disabled on the control; optional data-disabled on Field for
// the label row — recolor (text-disabled-foreground), never opacity.
// Spec: guidelines/fields.md · RES-20260717-001.

function FieldSet({ className, ...props }: React.ComponentProps<"fieldset">) {
  return (
    <fieldset
      data-slot="field-set"
      className={cn(
        "flex flex-col gap-6",
        "has-[>[data-slot=checkbox-group]]:gap-3 has-[>[data-slot=radio-group]]:gap-3",
        className,
      )}
      {...props}
    />
  );
}

function FieldLegend({
  className,
  variant = "legend",
  ...props
}: React.ComponentProps<"legend"> & { variant?: "legend" | "label" }) {
  return (
    <legend
      data-slot="field-legend"
      data-variant={variant}
      className={cn(
        "mb-3 font-medium",
        "data-[variant=legend]:text-base",
        "data-[variant=label]:text-sm",
        className,
      )}
      {...props}
    />
  );
}

function FieldGroup({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="field-group"
      className={cn(
        "group/field-group @container/field-group flex w-full flex-col gap-7 data-[slot=checkbox-group]:gap-3 [&>[data-slot=field-group]]:gap-4",
        // Settings rows (horizontal Field + FieldContent): one shared label column
        // sized to the widest label so control columns line up across the stack.
        // Every direct child spans both columns so separators / vertical Fields /
        // checkbox rows don't collapse into a single track.
        "has-[[data-orientation=horizontal]_[data-slot=field-content]]:grid has-[[data-orientation=horizontal]_[data-slot=field-content]]:grid-cols-[max-content_minmax(0,1fr)] has-[[data-orientation=horizontal]_[data-slot=field-content]]:gap-x-4 has-[[data-orientation=horizontal]_[data-slot=field-content]]:gap-y-7 has-[[data-orientation=horizontal]_[data-slot=field-content]]:[&>*]:col-span-2",
        "has-[[data-orientation=responsive]_[data-slot=field-content]]:@md/field-group:grid has-[[data-orientation=responsive]_[data-slot=field-content]]:@md/field-group:grid-cols-[max-content_minmax(0,1fr)] has-[[data-orientation=responsive]_[data-slot=field-content]]:@md/field-group:gap-x-4 has-[[data-orientation=responsive]_[data-slot=field-content]]:@md/field-group:gap-y-7 has-[[data-orientation=responsive]_[data-slot=field-content]]:@md/field-group:[&>*]:col-span-2",
        className,
      )}
      {...props}
    />
  );
}

const fieldVariants = cva(
  "group/field flex w-full gap-1.5",
  {
    variants: {
      orientation: {
        vertical: "flex-col",
        horizontal: [
          // Checkbox/radio companion rows (no FieldContent): simple flex.
          "flex-row items-center",
          // Two horizontal anatomies (first-child slot owns the grid template):
          // 1) Label-first form rows (FieldLabel + FieldContent): join FieldGroup's
          //    shared max-content | 1fr columns via subgrid.
          // 2) Content-first preference rows (FieldContent + trailing control):
          //    self-contained 1fr | auto so Switch sits top-right even when Field
          //    is wrapped in FieldLabel (Choice Card) and cannot join the subgrid.
          "has-[>[data-slot=field-content]]:grid has-[>[data-slot=field-content]]:gap-x-4 has-[>[data-slot=field-content]]:gap-y-0",
          "has-[>[data-slot=field-label]:first-child]:col-span-2 has-[>[data-slot=field-label]:first-child]:grid-cols-subgrid has-[>[data-slot=field-label]:first-child]:items-baseline",
          "has-[>[data-slot=field-content]:first-child]:grid-cols-[minmax(0,1fr)_auto] has-[>[data-slot=field-content]:first-child]:items-start",
          "has-[>[data-slot=field-content]]:[&>[role=checkbox],[role=radio],[data-slot=switch]]:mt-px",
        ],
        responsive: [
          "flex-col",
          "@md/field-group:flex-row @md/field-group:items-center",
          "@md/field-group:has-[>[data-slot=field-content]]:grid @md/field-group:has-[>[data-slot=field-content]]:gap-x-4 @md/field-group:has-[>[data-slot=field-content]]:gap-y-0",
          "@md/field-group:has-[>[data-slot=field-label]:first-child]:col-span-2 @md/field-group:has-[>[data-slot=field-label]:first-child]:grid-cols-subgrid @md/field-group:has-[>[data-slot=field-label]:first-child]:items-baseline",
          "@md/field-group:has-[>[data-slot=field-content]:first-child]:grid-cols-[minmax(0,1fr)_auto] @md/field-group:has-[>[data-slot=field-content]:first-child]:items-start",
          "@md/field-group:has-[>[data-slot=field-content]]:[&>[role=checkbox],[role=radio],[data-slot=switch]]:mt-px",
        ],
      },
      /**
       * How direct children (label + control) size on the cross axis.
       * - `full` — stretch controls to the field width (default stacked forms).
       * - `intrinsic` — do not impose child width; controls own `w-*` utilities
       *   (DatePicker triggers, compact toolbar pickers). Omits `[&>*]:w-full`
       *   instead of counteracting with a parent `> *` width rule.
       */
      controlWidth: {
        full: "",
        intrinsic: "",
      },
    },
    compoundVariants: [
      {
        orientation: "vertical",
        controlWidth: "full",
        class: "[&>*]:w-full [&>.sr-only]:w-auto",
      },
      {
        orientation: "vertical",
        controlWidth: "intrinsic",
        class: "[&>.sr-only]:w-auto",
      },
      {
        orientation: "responsive",
        controlWidth: "full",
        class: "[&>*]:w-full [&>.sr-only]:w-auto @md/field-group:[&>*]:w-auto",
      },
      {
        orientation: "responsive",
        controlWidth: "intrinsic",
        class: "[&>.sr-only]:w-auto @md/field-group:[&>*]:w-auto",
      },
    ],
    defaultVariants: {
      orientation: "vertical",
      controlWidth: "full",
    },
  },
);

function Field({
  className,
  orientation = "vertical",
  controlWidth = "full",
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof fieldVariants>) {
  return (
    <div
      role="group"
      data-slot="field"
      data-orientation={orientation}
      data-control-width={controlWidth}
      className={cn(fieldVariants({ orientation, controlWidth }), className)}
      {...props}
    />
  );
}

function FieldContent({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="field-content"
      className={cn(
        "group/field-content flex min-w-0 flex-1 flex-col gap-1.5 leading-snug",
        className,
      )}
      {...props}
    />
  );
}

function FieldLabel({
  className,
  variant = "field",
  ...props
}: React.ComponentProps<typeof Label>) {
  // Captions default to Label `field`; pass variant="inline" for checkbox rows.
  // Choice-card chrome when wrapping a Field: rounded border + padding.
  // Checked wash is neutral (`bg-muted`), not accent — the Switch/Checkbox
  // control carries brand hue; the card plate stays quiet.
  return (
    <Label
      data-slot="field-label"
      variant={variant}
      className={cn(
        "group/field-label peer/field-label flex w-fit gap-2 leading-snug",
        "has-[>[data-slot=field]]:w-full has-[>[data-slot=field]]:flex-col has-[>[data-slot=field]]:rounded-md has-[>[data-slot=field]]:border [&>[data-slot=field]]:p-4",
        "has-data-[state=checked]:border-border has-data-[state=checked]:bg-muted",
        className,
      )}
      {...props}
    />
  );
}

function FieldTitle({ className, ...props }: React.ComponentProps<"div">) {
  // Readable name inside Choice Cards / settings rows — same size as
  // FieldDescription (`text-sm`), medium weight + foreground for hierarchy.
  // Not Label `field` (text-xs caption above inputs).
  return (
    <div
      data-slot="field-title"
      className={cn(
        labelVariants({ variant: "inline" }),
        "flex w-fit items-center gap-2 leading-snug",
        className,
      )}
      {...props}
    />
  );
}

function FieldDescription({ className, ...props }: React.ComponentProps<"p">) {
  return (
    <p
      data-slot="field-description"
      className={cn(
        "text-sm font-normal leading-normal text-muted-foreground group-data-[orientation=horizontal]/field:text-balance",
        "last:mt-0 nth-last-2:-mt-1 [[data-variant=legend]+&]:-mt-1.5",
        "[&>a]:underline [&>a]:underline-offset-4 [&>a:hover]:text-primary",
        className,
      )}
      {...props}
    />
  );
}

function FieldSeparator({
  children,
  className,
  ...props
}: React.ComponentProps<"div"> & { children?: React.ReactNode }) {
  return (
    <div
      data-slot="field-separator"
      data-content={!!children}
      className={cn(
        "relative -my-2 h-5 text-sm group-data-[variant=outline]/field-group:-mb-2",
        className,
      )}
      {...props}
    >
      <Separator className="absolute inset-0 top-1/2" />
      {children ? (
        <span
          className="relative mx-auto block w-fit bg-background px-2 text-muted-foreground"
          data-slot="field-separator-content"
        >
          {children}
        </span>
      ) : null}
    </div>
  );
}

function FieldError({
  className,
  children,
  errors,
  ...props
}: React.ComponentProps<"div"> & {
  errors?: Array<{ message?: string } | undefined>;
}) {
  let content: React.ReactNode = children ?? null;

  if (!content && errors?.length) {
    const uniqueErrors = [...new Map(errors.map((error) => [error?.message, error])).values()];
    if (uniqueErrors.length === 1) {
      content = uniqueErrors[0]?.message;
    } else {
      content = (
        <ul className="ml-4 flex list-disc flex-col gap-1">
          {uniqueErrors.map(
            (error, index) => error?.message && <li key={index}>{error.message}</li>,
          )}
        </ul>
      );
    }
  }

  if (!content) {
    return null;
  }

  return (
    <div
      role="alert"
      data-slot="field-error"
      className={cn("text-sm font-normal text-error-ink", className)}
      {...props}
    >
      {content}
    </div>
  );
}

export {
  Field,
  FieldLabel,
  FieldDescription,
  FieldError,
  FieldGroup,
  FieldLegend,
  FieldSeparator,
  FieldSet,
  FieldContent,
  FieldTitle,
  fieldVariants,
};
