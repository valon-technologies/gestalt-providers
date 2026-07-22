"use client";

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { Button } from "@/components/ui/button";
import { SelectionCheck } from "@/components/ui/selection-check";

/**
 * Gestalt console vendor of Valon Registry `stepper`.
 *
 * Ownership: Valon Registry is the canonical source of truth
 * (`valon-tools/apps/registry/ui/src/ui/stepper.tsx`). Token adaptation only —
 * `cn` import path; Button / SelectionCheck / listItemInteraction resolve to
 * console vendors. Spec: `valon-tools/registry/guidelines/stepper.md`.
 */

// Multi-step *process navigation* (wizard chrome) — not TimelineSteps.
// Progress chrome (checks + connectors) is derived from the *active index*:
// going back rewinds completion. `activationMode` only gates which steps are
// clickable (jump = any; linear = up to furthest reached). Spec: guidelines/stepper.md.

type Orientation = "horizontal" | "vertical";
/** `jump` = any step clickable. `linear` = only up to furthest reached (no skip ahead). */
type ActivationMode = "jump" | "linear";
/** Visual progress relative to the active step — not a sticky visit history. */
type StepDataState = "active" | "completed" | "pending";
type StepperSize = "sm" | "default" | "lg";

interface StepperContextValue {
  orientation: Orientation;
  activationMode: ActivationMode;
  size: StepperSize;
  value: string;
  setValue: (next: string) => void;
  register: (value: string) => void;
  unregister: (value: string) => void;
  order: string[];
  isStepEnabled: (value: string) => boolean;
  getDataState: (value: string) => StepDataState;
  /** Rail grow + settle, in ms (0 under reduced motion). */
  getChromeDelayMs: () => number;
}

const StepperContext = React.createContext<StepperContextValue | null>(null);

function useStepper(consumer: string) {
  const ctx = React.useContext(StepperContext);
  if (!ctx) {
    throw new Error(`\`${consumer}\` must be used within \`Stepper\``);
  }
  return ctx;
}

interface StepItemContextValue {
  value: string;
  dataState: StepDataState;
  disabled: boolean;
  index: number;
}

const StepItemContext = React.createContext<StepItemContextValue | null>(null);

function useStepItem(consumer: string) {
  const ctx = React.useContext(StepItemContext);
  if (!ctx) {
    throw new Error(`\`${consumer}\` must be used within \`StepperItem\``);
  }
  return ctx;
}

/* -----------------------------------------------------------------------------
 * Stepper (root)
 * -------------------------------------------------------------------------- */

const stepperVariants = cva(
  // Incomplete rail (pending connectors + pending bullet rings) share one token.
  // Pad / rail / chrome-delay are one source of truth for geometry + paint lag.
  [
    "flex w-full gap-6",
    "[--stepper-rail-pending:var(--border)]",
    "[--stepper-trigger-pad:0.375rem]",
    // Slightly longer than --motion-move so the grow reads as progress.
    "[--stepper-rail-duration:var(--duration-250)]",
    // Extra beat after the rail arrives before destination ink / checks commit.
    "[--stepper-chrome-settle:var(--duration-75)]",
  ].join(" "),
  {
    variants: {
      orientation: {
        horizontal: "flex-col",
        vertical: "flex-col sm:flex-row sm:items-start",
      },
    },
    defaultVariants: {
      orientation: "horizontal",
    },
  },
);

function readCssDurationMs(value: string): number {
  const v = value.trim();
  if (!v) return 0;
  if (v.endsWith("ms")) return Number.parseFloat(v) || 0;
  if (v.endsWith("s")) return (Number.parseFloat(v) || 0) * 1000;
  return Number.parseFloat(v) || 0;
}

interface StepperProps
  extends Omit<React.ComponentProps<"div">, "defaultValue">,
    VariantProps<typeof stepperVariants> {
  value?: string;
  defaultValue?: string;
  onValueChange?: (value: string) => void;
  /** Non-linear click-any-step (`jump`, default) vs ordered unlock (`linear`). */
  activationMode?: ActivationMode;
  /** Indicator / connector scale. Default `default`. */
  size?: StepperSize;
}

function Stepper({
  className,
  orientation: orientationProp,
  activationMode = "jump",
  size = "default",
  value: valueProp,
  defaultValue,
  onValueChange,
  children,
  ...props
}: StepperProps) {
  const orientation = orientationProp ?? "horizontal";
  const resolvedSize = size ?? "default";
  const [uncontrolled, setUncontrolled] = React.useState(defaultValue ?? "");
  const isControlled = valueProp !== undefined;
  const value = isControlled ? valueProp : uncontrolled;

  const [order, setOrder] = React.useState<string[]>([]);
  // Linear unlock frontier — only advances; visuals still rewind with `value`.
  const [maxReached, setMaxReached] = React.useState(0);

  const setValue = React.useCallback(
    (next: string) => {
      if (!isControlled) setUncontrolled(next);
      onValueChange?.(next);
      setMaxReached((prev) => {
        const idx = order.indexOf(next);
        return idx > prev ? idx : prev;
      });
    },
    [isControlled, onValueChange, order],
  );

  // Seed / sync unlock frontier when order or controlled value settles.
  React.useLayoutEffect(() => {
    if (!value || order.length === 0) return;
    const idx = order.indexOf(value);
    if (idx < 0) return;
    setMaxReached((prev) => (idx > prev ? idx : prev));
  }, [value, order]);

  const register = React.useCallback((stepValue: string) => {
    setOrder((prev) => (prev.includes(stepValue) ? prev : [...prev, stepValue]));
  }, []);

  const unregister = React.useCallback((stepValue: string) => {
    setOrder((prev) => prev.filter((v) => v !== stepValue));
  }, []);

  const isStepEnabled = React.useCallback(
    (stepValue: string) => {
      if (activationMode === "jump") return true;
      const idx = order.indexOf(stepValue);
      if (idx === -1) return false;
      return idx <= maxReached;
    },
    [activationMode, order, maxReached],
  );

  const getDataState = React.useCallback(
    (stepValue: string): StepDataState => {
      const activeIndex = order.indexOf(value);
      const idx = order.indexOf(stepValue);
      if (idx === -1 || activeIndex === -1) {
        return stepValue === value ? "active" : "pending";
      }
      if (idx === activeIndex) return "active";
      if (idx < activeIndex) return "completed";
      return "pending";
    },
    [value, order],
  );

  const rootRef = React.useRef<HTMLDivElement>(null);

  const getChromeDelayMs = React.useCallback(() => {
    const node = rootRef.current;
    if (!node) return 0;
    // Read components — getPropertyValue leaves `calc(...)` unresolved on customs.
    const style = getComputedStyle(node);
    return (
      readCssDurationMs(style.getPropertyValue("--stepper-rail-duration")) +
      readCssDurationMs(style.getPropertyValue("--stepper-chrome-settle"))
    );
  }, []);

  const ctx = React.useMemo<StepperContextValue>(
    () => ({
      orientation,
      activationMode,
      size: resolvedSize,
      value,
      setValue,
      register,
      unregister,
      order,
      isStepEnabled,
      getDataState,
      getChromeDelayMs,
    }),
    [
      orientation,
      activationMode,
      resolvedSize,
      value,
      setValue,
      register,
      unregister,
      order,
      isStepEnabled,
      getDataState,
      getChromeDelayMs,
    ],
  );

  return (
    <StepperContext.Provider value={ctx}>
      <div
        ref={rootRef}
        data-slot="stepper"
        data-orientation={orientation}
        data-activation-mode={activationMode}
        data-size={resolvedSize}
        className={cn(stepperVariants({ orientation }), className)}
        {...props}
      >
        {children}
      </div>
    </StepperContext.Provider>
  );
}

/* -----------------------------------------------------------------------------
 * StepperList
 * -------------------------------------------------------------------------- */

const stepperListVariants = cva("flex w-full", {
  variants: {
    orientation: {
      horizontal: "flex-row items-start",
      vertical: "w-auto shrink-0 flex-col",
    },
  },
  defaultVariants: {
    orientation: "horizontal",
  },
});

function StepperList({ className, ...props }: React.ComponentProps<"ol">) {
  const { orientation } = useStepper("StepperList");
  return (
    <ol
      data-slot="stepper-list"
      data-orientation={orientation}
      className={cn(stepperListVariants({ orientation }), className)}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * StepperItem
 * -------------------------------------------------------------------------- */

const stepperItemVariants = cva("group/step relative flex", {
  variants: {
    orientation: {
      horizontal: "flex-1 flex-col items-center",
      vertical: "flex-row items-start gap-3 pb-8 last:pb-0",
    },
  },
  defaultVariants: {
    orientation: "horizontal",
  },
});

interface StepperItemProps extends React.ComponentProps<"li"> {
  value: string;
  disabled?: boolean;
}

function StepperItem({ className, value, disabled = false, children, ...props }: StepperItemProps) {
  const { orientation, register, unregister, order, getDataState, isStepEnabled } =
    useStepper("StepperItem");

  React.useLayoutEffect(() => {
    register(value);
    return () => unregister(value);
  }, [register, unregister, value]);

  const index = order.indexOf(value);
  const dataState = getDataState(value);
  const enabled = !disabled && isStepEnabled(value);

  const itemCtx = React.useMemo<StepItemContextValue>(
    () => ({
      value,
      dataState,
      disabled: !enabled,
      index: index === -1 ? 0 : index,
    }),
    [value, dataState, enabled, index],
  );

  return (
    <StepItemContext.Provider value={itemCtx}>
      <li
        data-slot="stepper-item"
        data-orientation={orientation}
        data-state={dataState}
        data-disabled={itemCtx.disabled || undefined}
        className={cn(stepperItemVariants({ orientation }), className)}
        {...props}
      >
        {children}
      </li>
    </StepItemContext.Provider>
  );
}

/* -----------------------------------------------------------------------------
 * StepperTrigger
 * -------------------------------------------------------------------------- */

const stepperTriggerVariants = cva(
  // One Neutral plate for the whole step (indicator + title + description) via
  // listItemInteraction — selectable-rows.md. Pad uses `--stepper-trigger-pad`
  // (same token the rail `top`/`left` offsets) so geometry stays centered.
  [
    "relative z-0 inline-flex items-center gap-2 rounded-md p-[var(--stepper-trigger-pad)] text-left outline-none",
    "transition-[color,background-color,border-color,opacity] duration-hover-out ease-out-quart hover:duration-hover-in",
    "focus-ring",
    listItemInteraction({ pointer: "css" }),
    "disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:bg-transparent disabled:active:bg-transparent",
  ].join(" "),
  {
    variants: {
      orientation: {
        horizontal: "flex-col",
        vertical: "flex-row",
      },
    },
    defaultVariants: {
      orientation: "horizontal",
    },
  },
);

interface StepperTriggerProps extends React.ComponentProps<"button"> {
  asChild?: boolean;
}

function StepperTrigger({
  className,
  asChild = false,
  children,
  onClick,
  ...props
}: StepperTriggerProps) {
  const { orientation, setValue } = useStepper("StepperTrigger");
  const { value, dataState, disabled } = useStepItem("StepperTrigger");
  const Comp = asChild ? Slot : "button";

  return (
    <Comp
      type={asChild ? undefined : "button"}
      data-slot="stepper-trigger"
      data-state={dataState}
      disabled={disabled}
      aria-current={dataState === "active" ? "step" : undefined}
      className={cn(stepperTriggerVariants({ orientation }), className)}
      {...props}
      onClick={(event: React.MouseEvent<HTMLButtonElement>) => {
        onClick?.(event);
        if (event.defaultPrevented || disabled) return;
        setValue(value);
      }}
    >
      {children}
    </Comp>
  );
}

/* -----------------------------------------------------------------------------
 * StepperIndicator
 * -------------------------------------------------------------------------- */

const stepperIndicatorVariants = cva(
  // Above the progress rail (separator z-[1]) so the bullet sits on the line end.
  "relative z-10 flex shrink-0 items-center justify-center rounded-full border font-display text-base font-normal italic leading-none transition-[color,background-color,border-color] duration-hover-out ease-out-quart [--stepper-indicator-size:2rem]",
  {
    variants: {
      size: {
        sm: "size-6 text-sm [--stepper-indicator-size:1.5rem] [&>svg]:size-3",
        default: "size-8 text-base [--stepper-indicator-size:2rem] [&>svg]:size-3.5",
        lg: "size-10 text-lg [--stepper-indicator-size:2.5rem] [&>svg]:size-4",
      },
      state: {
        // Active = ink fill + paper text (primary action surface).
        active: "border-primary bg-primary text-primary-foreground",
        // Completed = accent-solid fill (mid-gold — same as progress connectors).
        completed:
          "border-accent-solid bg-accent-solid text-accent-foreground",
        // Pending ring uses the same paint as pending connectors (--stepper-rail-pending).
        pending:
          "border-[color:var(--stepper-rail-pending)] bg-background text-muted-foreground",
      },
    },
    defaultVariants: {
      size: "default",
      state: "pending",
    },
  },
);

interface StepperIndicatorProps
  extends React.ComponentProps<"span">,
    VariantProps<typeof stepperIndicatorVariants> {
  /** Override the default number / check glyph. */
  children?: React.ReactNode;
}

function StepperIndicator({ className, size, children, ...props }: StepperIndicatorProps) {
  const { size: contextSize, getChromeDelayMs } = useStepper("StepperIndicator");
  const { dataState, index } = useStepItem("StepperIndicator");
  const resolvedSize = size ?? contextSize;
  const isCompleted = dataState === "completed";
  const checkSvg =
    resolvedSize === "sm" ? "size-3" : resolvedSize === "lg" ? "size-4" : "size-3.5";

  // Paint lags forward selection (pending → active waits for the rail). Rewind
  // onto a completed step snaps: ink + numeral now, check remounted unchecked
  // (no SelectionCheck exit wipe). Forward checks still lag on chrome delay.
  const prevDataState = React.useRef<StepDataState | null>(null);
  const [paintState, setPaintState] = React.useState<StepDataState>(dataState);
  const wasCompleted = React.useRef<boolean | null>(null);
  const [checkVisible, setCheckVisible] = React.useState(isCompleted);
  const [numberVisible, setNumberVisible] = React.useState(!isCompleted);
  const [checkEpoch, setCheckEpoch] = React.useState(0);

  React.useEffect(() => {
    const prev = prevDataState.current;
    if (prev === null) {
      prevDataState.current = dataState;
      setPaintState(dataState);
      return;
    }

    if (dataState === "active" && prev !== "active" && prev !== "completed") {
      // Forward / jump onto this step — hold pending until the edge arrives.
      prevDataState.current = dataState;
      setPaintState("pending");
      const delay = getChromeDelayMs();
      if (delay <= 0) {
        setPaintState("active");
        return;
      }
      const id = window.setTimeout(() => setPaintState("active"), delay);
      return () => window.clearTimeout(id);
    }

    prevDataState.current = dataState;
    setPaintState(dataState);
  }, [dataState, getChromeDelayMs]);

  React.useEffect(() => {
    if (wasCompleted.current === null) {
      wasCompleted.current = isCompleted;
      setCheckVisible(isCompleted);
      setNumberVisible(!isCompleted);
      return;
    }
    if (isCompleted && !wasCompleted.current) {
      wasCompleted.current = true;
      const delay = getChromeDelayMs();
      if (delay <= 0) {
        setCheckVisible(true);
        setNumberVisible(false);
        return;
      }
      const id = window.setTimeout(() => {
        setCheckVisible(true);
        setNumberVisible(false);
      }, delay);
      return () => window.clearTimeout(id);
    }
    if (!isCompleted && wasCompleted.current) {
      // Rewind: snap check off (remount skips exit wipe) + numeral + ink now.
      wasCompleted.current = false;
      setCheckVisible(false);
      setNumberVisible(true);
      setCheckEpoch((epoch) => epoch + 1);
    }
  }, [isCompleted, getChromeDelayMs]);

  return (
    <span
      data-slot="stepper-indicator"
      data-state={dataState}
      data-paint-state={paintState}
      data-size={resolvedSize}
      className={cn(stepperIndicatorVariants({ size: resolvedSize, state: paintState }), className)}
      {...props}
    >
      {children ?? (
        <>
          {/* Number stays mounted under the check so layout never shifts. */}
          <span
            className={cn("tabular-nums", !numberVisible && "invisible")}
            aria-hidden={!numberVisible}
          >
            {index + 1}
          </span>
          {/* Remount on rewind so unchecked is presence (no L→R exit wipe). */}
          <SelectionCheck
            key={checkEpoch}
            checked={checkVisible}
            density="condensed"
            tone="current"
            svgClassName={checkSvg}
            className="pointer-events-none absolute inset-0 m-auto"
          />
        </>
      )}
    </span>
  );
}

/* -----------------------------------------------------------------------------
 * StepperSeparator
 * -------------------------------------------------------------------------- */

const stepperSeparatorVariants = cva(
  // Destination-owned rail track. Fill child scales like a progress line
  // (`--stepper-rail-duration`). Pad offset keeps the line on the circle center.
  "pointer-events-none absolute z-[1] overflow-hidden bg-[var(--stepper-rail-pending)]",
  {
    variants: {
      orientation: {
        horizontal:
          "left-[calc(-50%+var(--stepper-indicator-size,2rem)*0.5)] top-[calc(var(--stepper-trigger-pad,0.375rem)+var(--stepper-indicator-size,2rem)*0.5)] h-[1.5px] w-[calc(100%-var(--stepper-indicator-size,2rem))] -translate-y-1/2 group-first/step:hidden",
        vertical:
          "left-[calc(var(--stepper-trigger-pad,0.375rem)+var(--stepper-indicator-size,2rem)*0.5)] top-[calc(var(--stepper-trigger-pad,0.375rem)+var(--stepper-indicator-size,2rem))] h-[calc(100%-var(--stepper-indicator-size,2rem)-var(--stepper-trigger-pad,0.375rem))] w-[1.5px] -translate-x-1/2 group-last/step:hidden",
      },
    },
    defaultVariants: {
      orientation: "horizontal",
    },
  },
);

const stepperSeparatorFillVariants = cva(
  // Clip grow = progress line (SelectionCheck uses the same clip-path pattern).
  // Prefer clip over scale-x — scale axis utilities are not always emitted here.
  [
    "size-full bg-accent-solid transition-[clip-path] ease-out-quart",
    "duration-[var(--stepper-rail-duration,var(--motion-move))]",
  ].join(" "),
  {
    variants: {
      orientation: {
        horizontal:
          "[clip-path:inset(0_100%_0_0)] data-[state=completed]:[clip-path:inset(0_0_0_0)]",
        vertical:
          "[clip-path:inset(100%_0_0_0)] data-[state=completed]:[clip-path:inset(0_0_0_0)]",
      },
    },
    defaultVariants: {
      orientation: "horizontal",
    },
  },
);

function StepperSeparator({ className, ...props }: React.ComponentProps<"div">) {
  const { orientation, size, value: activeValue, order } = useStepper("StepperSeparator");
  const { index } = useStepItem("StepperSeparator");
  const activeIndex = order.indexOf(activeValue);
  // Horizontal = segment into this step → fill when progress has reached here.
  // Vertical = outgoing stub into the padding below → fill when progress past here.
  const lineState =
    orientation === "horizontal"
      ? activeIndex >= index
        ? "completed"
        : "pending"
      : activeIndex > index
        ? "completed"
        : "pending";
  const sizeVar =
    size === "sm"
      ? "[--stepper-indicator-size:1.5rem]"
      : size === "lg"
        ? "[--stepper-indicator-size:2.5rem]"
        : "[--stepper-indicator-size:2rem]";

  return (
    <div
      data-slot="stepper-separator"
      data-orientation={orientation}
      data-state={lineState}
      aria-hidden="true"
      className={cn(sizeVar, stepperSeparatorVariants({ orientation }), className)}
      {...props}
    >
      <div
        data-slot="stepper-separator-fill"
        data-state={lineState}
        className={stepperSeparatorFillVariants({ orientation })}
      />
    </div>
  );
}

/* -----------------------------------------------------------------------------
 * Title / Description
 * -------------------------------------------------------------------------- */

function StepperTitle({ className, ...props }: React.ComponentProps<"span">) {
  const { dataState } = useStepItem("StepperTitle");
  return (
    <span
      data-slot="stepper-title"
      data-state={dataState}
      className={cn(
        "text-sm font-medium leading-none tracking-tight",
        dataState === "active" && "text-foreground",
        dataState === "completed" && "text-foreground",
        dataState === "pending" && "text-muted-foreground",
        className,
      )}
      {...props}
    />
  );
}

function StepperDescription({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="stepper-description"
      className={cn("text-muted-foreground text-xs text-balance", className)}
      {...props}
    />
  );
}

/* -----------------------------------------------------------------------------
 * StepperContent
 * -------------------------------------------------------------------------- */

interface StepperContentProps extends React.ComponentProps<"div"> {
  value: string;
  forceMount?: boolean;
}

function StepperContent({ className, value, forceMount = false, children, ...props }: StepperContentProps) {
  const { value: active, orientation } = useStepper("StepperContent");
  const isActive = active === value;
  if (!forceMount && !isActive) return null;

  return (
    <div
      data-slot="stepper-content"
      data-orientation={orientation}
      data-state={isActive ? "active" : "inactive"}
      hidden={!isActive}
      className={cn(
        "flex min-w-0 flex-1 flex-col gap-3 text-sm",
        orientation === "horizontal" && "w-full",
        className,
      )}
      {...props}
    >
      {children}
    </div>
  );
}

/* -----------------------------------------------------------------------------
 * Prev / Next
 * -------------------------------------------------------------------------- */

function StepperPrev({
  className,
  children = "Previous",
  onClick,
  disabled: disabledProp,
  ...props
}: React.ComponentProps<typeof Button>) {
  const { value, order, setValue } = useStepper("StepperPrev");
  const index = order.indexOf(value);
  const disabled = Boolean(disabledProp) || index <= 0;

  return (
    <Button
      type="button"
      variant="outline"
      data-slot="stepper-prev"
      className={className}
      {...props}
      disabled={disabled}
      onClick={(event) => {
        onClick?.(event);
        if (event.defaultPrevented || disabled) return;
        const prev = order[index - 1];
        if (prev) setValue(prev);
      }}
    >
      {children}
    </Button>
  );
}

function StepperNext({
  className,
  children = "Next",
  onClick,
  disabled: disabledProp,
  ...props
}: React.ComponentProps<typeof Button>) {
  const { value, order, setValue } = useStepper("StepperNext");
  const index = order.indexOf(value);
  const disabled = Boolean(disabledProp) || index < 0 || index >= order.length - 1;

  return (
    <Button
      type="button"
      data-slot="stepper-next"
      className={className}
      {...props}
      disabled={disabled}
      onClick={(event) => {
        onClick?.(event);
        if (event.defaultPrevented || disabled) return;
        const next = order[index + 1];
        if (next) setValue(next);
      }}
    >
      {children}
    </Button>
  );
}

export {
  Stepper,
  StepperList,
  StepperItem,
  StepperTrigger,
  StepperIndicator,
  StepperSeparator,
  StepperTitle,
  StepperDescription,
  StepperContent,
  StepperPrev,
  StepperNext,
  stepperVariants,
  stepperListVariants,
  stepperItemVariants,
  stepperTriggerVariants,
  stepperIndicatorVariants,
  stepperSeparatorVariants,
};
