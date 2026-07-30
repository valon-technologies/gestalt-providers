/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/button";
import { Input, type InputProps } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";

// InputGroup = one bordered control with leading/trailing (or block) addons.
// The shell owns border + focus ring + control height; InputGroupInput /
// Textarea are borderless fills. Field wraps the group for labels — do not
// invent a second label system. Spec: guidelines/fields.md · RES-20260721-005.
//
// Size: shell owns outer height (`h-control-*`, same box as a lone Input) and,
// for block-addon (`h-auto`) layouts, the control's min-height floor. The
// inner control fills with `h-full` only — never `min-h-control-*` on the
// fixed path (that token equals the shell outer box, so border would overflow).
// Addon/Text type scale follows `data-size` so sm/lg chrome matches the value.
// DOM order = visual order = tab order. Put inline-start / block-start addons
// *before* the control; inline-end / block-end *after*. Do not use CSS `order`
// to rearrange focusable chrome (that desyncs keyboard from the eye).
// Focus: group draws focus-ring geometry via :has(control:focus-visible) —
// same outline recipe as Calendar chrome (focus-ring.md). Inner control uses
// Input/Textarea chrome="group" so focus-ring is omitted at the source.
// Disabled: flat --disabled recolor on the shell (never opacity on addons).
// Color for addon text lives on Addon (muted → disabled); InputGroupText does
// not set its own muted color or it would win over the disabled recolor.
// Invalid: aria-invalid on the control → destructive shell border.
// Search + custom clear: type="search" with a trailing InputGroupButton for
// clear — shell hides WebKit's native cancel so consumers need no className
// override on InputGroupInput. Use InputGroupClearAddon when the clear affordance
// toggles — it reserves inline-end chrome so the shell width stays stable.

type InputGroupSize = NonNullable<InputProps["size"]>;

const InputGroupSizeContext = React.createContext<InputGroupSize>("default");
const InputGroupControlDisabledContext = React.createContext(false);
const InputGroupControlDisabledRegistrarContext = React.createContext<
  ((disabled: boolean) => void) | null
>(null);

const inputGroupVariants = cva(
  [
    // Baseline (not center): InputGroupText shares the value's alphabetic
    // baseline. Icon/button addons opt into self-center.
    "group/input-group relative flex w-full min-w-0 items-baseline rounded-md border border-input bg-background transition-[color,border-color] duration-select-out ease-out-quart",
    "has-[>textarea]:h-auto",

    // Addon align → padding / stacking on the control.
    "has-[>[data-align=inline-start]]:[&>[data-slot=input-group-control]]:pl-2",
    "has-[>[data-align=inline-end]]:[&>[data-slot=input-group-control]]:pr-2",
    "has-[>[data-align=block-start]]:h-auto has-[>[data-align=block-start]]:flex-col has-[>[data-align=block-start]]:items-stretch has-[>[data-align=block-start]]:[&>[data-slot=input-group-control]]:pb-3",
    "has-[>[data-align=block-end]]:h-auto has-[>[data-align=block-end]]:flex-col has-[>[data-align=block-end]]:items-stretch has-[>[data-align=block-end]]:[&>[data-slot=input-group-control]]:pt-3",

    // Focus on the control → ring on the shell (not ring-[3px] / border-ring).
    "has-[[data-slot=input-group-control]:focus-visible]:outline-3 has-[[data-slot=input-group-control]:focus-visible]:outline-offset-2 has-[[data-slot=input-group-control]:focus-visible]:outline-accent-solid",

    // Invalid / disabled — control owns aria-invalid + disabled; shell paints.
    "has-[[data-slot=input-group-control][aria-invalid=true]]:border-destructive",
    "has-[[data-slot=input-group-control]:disabled]:cursor-not-allowed has-[[data-slot=input-group-control]:disabled]:border-border has-[[data-slot=input-group-control]:disabled]:bg-disabled",

    // Trailing InputGroupButton + type=search → hide WebKit native clear (custom
    // clear owns the affordance; avoids duplicate × at the inline-end).
    "has-[>[data-align=inline-end]_[data-input-group-button]]:[&_[data-slot=input-group-control][type=search]::-webkit-search-cancel-button]:hidden",
  ],
  {
    variants: {
      size: {
        // Block shells drop to h-auto; pin the control's min-height here so
        // InputGroupInput can stay `h-full` only (no second height owner).
        sm: "h-control-sm has-[>[data-align=block-start]]:[&>[data-slot=input-group-control]]:min-h-control-sm has-[>[data-align=block-end]]:[&>[data-slot=input-group-control]]:min-h-control-sm",
        default:
          "h-control-default has-[>[data-align=block-start]]:[&>[data-slot=input-group-control]]:min-h-control-default has-[>[data-align=block-end]]:[&>[data-slot=input-group-control]]:min-h-control-default",
        lg: "h-control-lg has-[>[data-align=block-start]]:[&>[data-slot=input-group-control]]:min-h-control-lg has-[>[data-align=block-end]]:[&>[data-slot=input-group-control]]:min-h-control-lg",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

function InputGroup({
  className,
  size = "default",
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof inputGroupVariants>) {
  const [controlDisabled, setControlDisabled] = React.useState(false);

  return (
    <InputGroupSizeContext.Provider value={size ?? "default"}>
      <InputGroupControlDisabledContext.Provider value={controlDisabled}>
        <InputGroupControlDisabledRegistrarContext.Provider value={setControlDisabled}>
          <div
            data-slot="input-group"
            role="group"
            data-size={size ?? "default"}
            className={cn(inputGroupVariants({ size }), className)}
            {...props}
          />
        </InputGroupControlDisabledRegistrarContext.Provider>
      </InputGroupControlDisabledContext.Provider>
    </InputGroupSizeContext.Provider>
  );
}

const inputGroupAddonVariants = cva(
  // Content-sized (no h-full) so text addons participate in the shell's
  // items-baseline. Icon/button addons self-center in the control height.
  // Owns muted + disabled text color for children (including InputGroupText).
  // Type scale follows shell `data-size` (not a pinned text-control-default).
  "flex cursor-text items-center justify-center gap-2 font-normal text-muted-foreground select-none group-data-[size=sm]/input-group:text-control-sm group-data-[size=default]/input-group:text-control-default group-data-[size=lg]/input-group:text-control-lg group-has-[[data-slot=input-group-control]:disabled]/input-group:cursor-not-allowed group-has-[[data-slot=input-group-control]:disabled]/input-group:text-disabled-foreground [&>kbd]:rounded-sm [&>svg:not([class*='size-'])]:size-4",
  {
    variants: {
      align: {
        "inline-start":
          // Descendants (not `>button`) so Tooltip-wrapped InputGroupButton still
          // pulls to the edge — documented copy/password pattern.
          // Target `data-input-group-button` (not only data-slot): asChild
          // wrappers may stomp shared data-slot; this marker is InputGroup-owned.
          // No `order-*`: callers place start addons before the control in DOM.
          "pl-3 has-[[data-input-group-button]]:self-center has-[>svg]:self-center has-[[data-input-group-button]]:ml-[-0.45rem] has-[>kbd]:ml-[-0.35rem]",
        "inline-end":
          "pr-3 has-[[data-input-group-button]]:self-center has-[>svg]:self-center has-[[data-input-group-button]]:mr-[-0.45rem] has-[>kbd]:mr-[-0.35rem]",
        "block-start":
          "h-auto w-full justify-start px-3 pt-3 [.border-b]:pb-3",
        "block-end":
          "h-auto w-full justify-start px-3 pb-3 [.border-t]:pt-3",
      },
    },
    defaultVariants: {
      align: "inline-start",
    },
  },
);

function InputGroupAddon({
  className,
  align = "inline-start",
  onClick,
  ...props
}: React.ComponentProps<"div"> & VariantProps<typeof inputGroupAddonVariants>) {
  return (
    <div
      role="group"
      data-slot="input-group-addon"
      data-align={align}
      className={cn(inputGroupAddonVariants({ align }), className)}
      {...props}
      onClick={(e) => {
        onClick?.(e);
        if (e.defaultPrevented) return;
        if ((e.target as HTMLElement).closest("button")) {
          return;
        }
        const control = e.currentTarget.parentElement?.querySelector<
          HTMLInputElement | HTMLTextAreaElement
        >("[data-slot=input-group-control]");
        if (!control || control.disabled) return;
        // focusVisible so the shell's :focus-visible ring paints — plain
        // .focus() from a click would leave the field focused with no ring.
        // @ts-expect-error focusVisible is supported in Chromium; DOM lib lag.
        control.focus({ focusVisible: true });
      }}
    />
  );
}

function InputGroupButton({
  className,
  type = "button",
  variant = "ghost",
  size = "icon-xs",
  ...props
}: React.ComponentProps<typeof Button>) {
  return (
    <Button
      type={type}
      variant={variant}
      size={size}
      className={cn("shrink-0 shadow-none", className)}
      {...props}
      data-slot="input-group-button"
      // Layout target for Addon inset/centering. Distinct from data-slot so
      // asChild overlays (TooltipTrigger) cannot erase the addon contract.
      data-input-group-button=""
    />
  );
}

type InputGroupClearAddonProps = Omit<
  React.ComponentProps<typeof InputGroupButton>,
  | "aria-label"
  | "aria-hidden"
  | "tabIndex"
  | "disabled"
  | "onClick"
  | "onMouseDown"
  | "children"
> & {
  visible: boolean;
  onClear: () => void;
  "aria-label"?: string;
  children: React.ReactNode;
};

/** Trailing clear affordance that keeps inline-end chrome mounted for stable width. */
function InputGroupClearAddon({
  visible,
  onClear,
  "aria-label": ariaLabel = "Clear",
  className,
  children,
  ...props
}: InputGroupClearAddonProps) {
  const controlDisabled = React.useContext(InputGroupControlDisabledContext);
  const interactive = visible && !controlDisabled;

  return (
    <InputGroupAddon align="inline-end">
      <InputGroupButton
        {...props}
        disabled={!interactive}
        aria-label={interactive ? ariaLabel : undefined}
        aria-hidden={!interactive}
        tabIndex={interactive ? undefined : -1}
        className={cn(className, !visible && "pointer-events-none invisible")}
        onMouseDown={(event) => {
          event.preventDefault();
        }}
        onClick={(event) => {
          event.preventDefault();
          if (!visible) return;
          const control = event.currentTarget
            .closest("[data-slot=input-group]")
            ?.querySelector<HTMLInputElement | HTMLTextAreaElement>(
              "[data-slot=input-group-control]",
            );
          if (!control || control.disabled) return;
          onClear();
          // @ts-expect-error focusVisible is supported in Chromium; DOM lib lag.
          control.focus({ focusVisible: true });
        }}
      >
        {children}
      </InputGroupButton>
    </InputGroupAddon>
  );
}

function InputGroupText({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="input-group-text"
      className={cn(
        // Same type metrics as Input (no leading-none) so items-baseline on
        // the shell lines meta up with the value ink.
        "font-normal group-data-[size=sm]/input-group:text-control-sm group-data-[size=default]/input-group:text-control-default group-data-[size=lg]/input-group:text-control-lg [&_svg]:pointer-events-none [&_svg:not([class*='size-'])]:size-4 [&_svg]:inline [&_svg]:align-middle",
        className,
      )}
      {...props}
    />
  );
}

function InputGroupInput({
  className,
  size: sizeProp,
  ref,
  disabled,
  ...props
}: Omit<InputProps, "chrome"> & { ref?: React.Ref<HTMLInputElement> }) {
  const sizeCtx = React.useContext(InputGroupSizeContext);
  const registerControlDisabled = React.useContext(
    InputGroupControlDisabledRegistrarContext,
  );
  const size = sizeProp ?? sizeCtx;

  React.useLayoutEffect(() => {
    registerControlDisabled?.(!!disabled);
    return () => registerControlDisabled?.(false);
  }, [disabled, registerControlDisabled]);

  return (
    <Input
      ref={ref}
      data-slot="input-group-control"
      size={size}
      disabled={disabled}
      className={cn(
        // Fill the shell. `h-full!` beats Input's `h-control-*`. Block-addon
        // min-height is owned by the shell size variants — do not set a
        // control min-height token here or fixed shells overflow by border.
        "h-full! min-w-0 flex-1 rounded-none bg-transparent py-0",
        "disabled:bg-transparent",
        className,
      )}
      {...props}
      chrome="group"
    />
  );
}

function InputGroupTextarea({
  className,
  ref,
  disabled,
  ...props
}: Omit<React.ComponentProps<typeof Textarea>, "chrome">) {
  const size = React.useContext(InputGroupSizeContext);
  const registerControlDisabled = React.useContext(
    InputGroupControlDisabledRegistrarContext,
  );

  React.useLayoutEffect(() => {
    registerControlDisabled?.(!!disabled);
    return () => registerControlDisabled?.(false);
  }, [disabled, registerControlDisabled]);

  return (
    <Textarea
      ref={ref}
      data-slot="input-group-control"
      disabled={disabled}
      className={cn(
        "min-w-0 flex-1 resize-none rounded-none bg-transparent py-3",
        // Override Textarea's fixed text-sm so chrome + value share size.
        size === "sm" && "text-control-sm",
        size === "default" && "text-control-default",
        size === "lg" && "text-control-lg",
        "disabled:bg-transparent",
        className,
      )}
      {...props}
      chrome="group"
    />
  );
}

export {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupClearAddon,
  InputGroupText,
  InputGroupInput,
  InputGroupTextarea,
  inputGroupVariants,
  inputGroupAddonVariants,
};
