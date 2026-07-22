import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { Label, labelVariants } from "@/components/ui/label";

/**
 * Gestalt console vendor of Valon Registry `field`.
 *
 * Ownership: Valon Registry (`valon-tools/apps/registry/ui/src/ui/field.tsx`).
 * Spec: guidelines/fields.md. FieldSeparator uses an inline hairline (Separator
 * not vendored — same chrome as Registry `bg-border` rule).
 */

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

const fieldVariants = cva("group/field flex w-full gap-1.5", {
  variants: {
    orientation: {
      vertical: ["flex-col [&>*]:w-full [&>.sr-only]:w-auto"],
      horizontal: [
        // Checkbox/radio companion rows (no FieldContent): simple flex.
        "flex-row items-center",
        // Settings rows (FieldContent present): join FieldGroup's shared columns.
        // Baseline-align label text with the control's text (not box-center /
        // top); description/error hang below inside FieldContent.
        // gap-x must match FieldGroup — a subgrid's own gap overrides the parent.
        "has-[>[data-slot=field-content]]:col-span-2 has-[>[data-slot=field-content]]:grid has-[>[data-slot=field-content]]:grid-cols-subgrid has-[>[data-slot=field-content]]:items-baseline has-[>[data-slot=field-content]]:gap-x-4 has-[>[data-slot=field-content]]:gap-y-0",
        "has-[>[data-slot=field-content]]:[&>[role=checkbox],[role=radio]]:mt-px",
      ],
      responsive: [
        "flex-col [&>*]:w-full [&>.sr-only]:w-auto",
        "@md/field-group:flex-row @md/field-group:items-center @md/field-group:[&>*]:w-auto",
        "@md/field-group:has-[>[data-slot=field-content]]:col-span-2 @md/field-group:has-[>[data-slot=field-content]]:grid @md/field-group:has-[>[data-slot=field-content]]:grid-cols-subgrid @md/field-group:has-[>[data-slot=field-content]]:items-baseline @md/field-group:has-[>[data-slot=field-content]]:gap-x-4 @md/field-group:has-[>[data-slot=field-content]]:gap-y-0",
        "@md/field-group:has-[>[data-slot=field-content]]:[&>[role=checkbox],[role=radio]]:mt-px",
      ],
    },
  },
  defaultVariants: {
    orientation: "vertical",
  },
});

function Field({
  className,
  orientation = "vertical",
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof fieldVariants>) {
  return (
    <div
      role="group"
      data-slot="field"
      data-orientation={orientation}
      className={cn(fieldVariants({ orientation }), className)}
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
  // Layout / checkbox-card chrome only — type + disabled/invalid live on Label.
  return (
    <Label
      data-slot="field-label"
      variant={variant}
      className={cn(
        "group/field-label peer/field-label flex w-fit gap-2 leading-snug",
        "has-[>[data-slot=field]]:w-full has-[>[data-slot=field]]:flex-col has-[>[data-slot=field]]:rounded-md has-[>[data-slot=field]]:border [&>[data-slot=field]]:p-4",
        "has-data-[state=checked]:border-accent-vivid has-data-[state=checked]:bg-accent dark:has-data-[state=checked]:bg-accent-subtle",
        className,
      )}
      {...props}
    />
  );
}

function FieldTitle({ className, ...props }: React.ComponentProps<"div">) {
  // Non-label title: same caption type/disabled contract as Label `field`.
  return (
    <div
      data-slot="field-title"
      className={cn(
        labelVariants({ variant: "field" }),
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
      <div
        role="separator"
        aria-hidden="true"
        className="absolute inset-x-0 top-1/2 h-px w-full shrink-0 bg-border"
      />
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
    const uniqueErrors = [
      ...new Map(errors.map((error) => [error?.message, error])).values(),
    ];
    if (uniqueErrors.length === 1) {
      content = uniqueErrors[0]?.message;
    } else {
      content = (
        <ul className="ml-4 flex list-disc flex-col gap-1">
          {uniqueErrors.map(
            (error, index) =>
              error?.message && <li key={index}>{error.message}</li>,
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
      className={cn("text-sm font-normal text-error-foreground", className)}
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
