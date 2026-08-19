/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import { listItemInteraction } from "@/lib/list-item-interaction";
import { Button } from "@/components/ui/button";
import {
  readStepRailTimingMs,
  stepRailCompletedChromeAccentClassName,
  stepRailCompletedChromeOutcomeClassName,
  stepRailIndicatorArriveStaggerSteps,
  stepRailLineState,
  stepRailLineStaggerSteps,
  stepRailAdvanceTransition,
  stepRailFromActiveIndex,
  stepRailRootClassName,
  StepRailIndicator,
  stepRailIndicatorVariants,
  StepRailSeparator,
  stepRailSeparatorTrackVariants,
  type StepRailCompletedChrome,
  type StepRailVisualState,
} from "@/components/ui/step-rail";

// Multi-step *process navigation* (wizard chrome) — not TimelineSteps.
// Progress chrome (checks + connectors) is derived from the *active index*:
// going back rewinds completion. `activationMode` only gates which steps are
// clickable (jump = any; linear = up to furthest reached). Spec: guidelines/stepper.md.

type Orientation = "horizontal" | "vertical";
/** `jump` = any step clickable. `linear` = only up to furthest reached (no skip ahead). */
type ActivationMode = "jump" | "linear";
/** Visual progress relative to the active step — not a sticky visit history. */
type StepDataState = StepRailVisualState;
type StepperSize = "sm" | "default" | "lg";

interface StepperContextValue {
  orientation: Orientation;
  activationMode: ActivationMode;
  size: StepperSize;
  interactive: boolean;
  value: string;
  setValue: (next: string) => void;
  register: (value: string) => void;
  unregister: (value: string) => void;
  order: string[];
  isStepEnabled: (value: string) => boolean;
  getDataState: (value: string) => StepDataState;
  /** Previous active index for this render (rail tail stagger). */
  fromActiveIndex: number;
  /** Rail grow + settle, in ms (0 under reduced motion). */
  getChromeDelayMs: (index: number, dataState: StepDataState) => number;
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
  [
    "flex w-full gap-6",
    stepRailRootClassName,
    "[--step-rail-trigger-pad:0.375rem]",
    // Legacy aliases — guidelines and overrides may still reference --stepper-*.
    "[--stepper-rail-pending:var(--step-rail-pending)]",
    "[--stepper-trigger-pad:var(--step-rail-trigger-pad)]",
    "[--stepper-rail-duration:var(--step-rail-duration)]",
    "[--stepper-chrome-settle:var(--step-rail-chrome-settle)]",
    "[--stepper-indicator-size:var(--step-rail-indicator-size)]",
  ].join(" "),
  {
    variants: {
      orientation: {
        horizontal: "flex-col",
        vertical: "flex-col sm:flex-row sm:items-start",
      },
      completedChrome: {
        accent: stepRailCompletedChromeAccentClassName,
        outcome: stepRailCompletedChromeOutcomeClassName,
      },
    },
    defaultVariants: {
      orientation: "horizontal",
      completedChrome: "outcome",
    },
  },
);

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
  /** When false, triggers are display-only (no click / hover plate). Default `true`. */
  interactive?: boolean;
  /** Completed disc/check/connector paint. Default `outcome`. Not TimelineSteps `success`. */
  completedChrome?: StepRailCompletedChrome;
}

function Stepper({
  className,
  orientation: orientationProp,
  activationMode = "jump",
  size = "default",
  interactive = true,
  completedChrome = "outcome",
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
  const transitionRef = React.useRef({ origin: null as number | null, current: null as number | null });
  const activeIndex = order.indexOf(value);
  transitionRef.current = stepRailAdvanceTransition(activeIndex, transitionRef.current);
  const fromActiveIndex = stepRailFromActiveIndex(transitionRef.current);

  const getChromeDelayMs = React.useCallback(
    (index: number, dataState: StepDataState) => {
      const { durationMs, settleMs } = readStepRailTimingMs(rootRef.current);
      const stagger = stepRailIndicatorArriveStaggerSteps(
        orientation,
        index,
        fromActiveIndex,
        activeIndex,
        dataState,
      );
      return stagger * durationMs + durationMs + settleMs;
    },
    [orientation, fromActiveIndex, activeIndex],
  );

  const ctx = React.useMemo<StepperContextValue>(
    () => ({
      orientation,
      activationMode,
      size: resolvedSize,
      interactive,
      value,
      setValue,
      register,
      unregister,
      order,
      isStepEnabled,
      getDataState,
      fromActiveIndex,
      getChromeDelayMs,
    }),
    [
      orientation,
      activationMode,
      resolvedSize,
      interactive,
      value,
      setValue,
      register,
      unregister,
      order,
      isStepEnabled,
      getDataState,
      fromActiveIndex,
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
        data-interactive={interactive || undefined}
        data-size={resolvedSize}
        data-completed-chrome={completedChrome}
        className={cn(
          stepperVariants({ orientation, completedChrome }),
          className,
        )}
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

// isolate: plate / rail / indicator z-index compare inside the step. Later
// items paint on top, so a destination-owned rail stays above a neighbor plate.
const stepperItemVariants = cva("group/step relative isolate flex", {
  variants: {
    orientation: {
      horizontal: "flex-1 flex-col items-stretch",
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
  [
    // No z-index: a stacking context here would trap the indicator below the
    // rail or paint this Neutral plate over destination-owned connectors.
    "relative inline-flex items-center gap-2 rounded-md outline-none",
    "p-[var(--step-rail-trigger-pad)]",
    "transition-[color,background-color,border-color,opacity] duration-hover-out ease-out-quart hover:duration-hover-in",
    "focus-ring",
  ].join(" "),
  {
    variants: {
      orientation: {
        // Fill the equal flex column so hover/hit area is constant across steps,
        // not hugging the label. Center wrapped titles under the indicator.
        horizontal: "flex w-full min-w-0 flex-col text-center",
        vertical: "flex-row text-left",
      },
      interactive: {
        true: listItemInteraction({ pointer: "css" }),
        false: "",
      },
    },
    defaultVariants: {
      orientation: "horizontal",
      interactive: true,
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
  const { orientation, interactive, setValue } = useStepper("StepperTrigger");
  const { value, dataState, disabled } = useStepItem("StepperTrigger");
  const triggerClassName = cn(
    stepperTriggerVariants({ orientation, interactive }),
    interactive &&
      "disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:bg-transparent disabled:active:bg-transparent",
    className,
  );

  if (!interactive) {
    return (
      <div
        data-slot="stepper-trigger"
        data-state={dataState}
        aria-current={dataState === "active" ? "step" : undefined}
        className={triggerClassName}
        {...(props as React.ComponentProps<"div">)}
      >
        {children}
      </div>
    );
  }

  const Comp = asChild ? Slot : "button";

  return (
    <Comp
      type={asChild ? undefined : "button"}
      data-slot="stepper-trigger"
      data-state={dataState}
      disabled={disabled}
      aria-current={dataState === "active" ? "step" : undefined}
      className={triggerClassName}
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
 * StepperIndicator / StepperSeparator (shared step-rail chrome)
 * -------------------------------------------------------------------------- */

const stepperIndicatorVariants = stepRailIndicatorVariants;

type StepperIndicatorProps = React.ComponentProps<typeof StepRailIndicator>;

function StepperIndicator({ className, size, children, ...props }: StepperIndicatorProps) {
  const { size: contextSize, getChromeDelayMs } = useStepper("StepperIndicator");
  const { dataState, index } = useStepItem("StepperIndicator");
  const resolveChromeDelayMs = React.useCallback(
    () => getChromeDelayMs(index, dataState),
    [getChromeDelayMs, index, dataState],
  );

  return (
    <StepRailIndicator
      data-slot="stepper-indicator"
      size={size ?? contextSize}
      state={dataState}
      index={index}
      getChromeDelayMs={resolveChromeDelayMs}
      className={className}
      {...props}
    >
      {children}
    </StepRailIndicator>
  );
}

const stepperSeparatorVariants = stepRailSeparatorTrackVariants;

function StepperSeparator({ className, ...props }: React.ComponentProps<"div">) {
  const {
    orientation,
    size,
    value: activeValue,
    order,
    fromActiveIndex,
  } = useStepper("StepperSeparator");
  const { index } = useStepItem("StepperSeparator");
  const activeIndex = order.indexOf(activeValue);
  const lineState = stepRailLineState(orientation, index, activeIndex);
  const layout = orientation === "horizontal" ? "stepper" : "stepper-vertical";
  const staggerSteps = stepRailLineStaggerSteps(
    orientation,
    index,
    fromActiveIndex,
    activeIndex,
  );

  return (
    <StepRailSeparator
      data-slot="stepper-separator"
      orientation={orientation}
      size={size}
      lineState={lineState}
      layout={layout}
      staggerSteps={staggerSteps}
      className={className}
      {...props}
    />
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
        "text-pretty text-sm font-medium leading-snug tracking-tight",
        // Horizontal trigger keeps items-center so the indicator stays a circle.
        // Titles wrap as full-column blocks via the item's data-orientation.
        "group-data-[orientation=horizontal]/step:block group-data-[orientation=horizontal]/step:w-full group-data-[orientation=horizontal]/step:min-w-0",
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
      className={cn(
        "text-muted-foreground text-xs text-balance",
        "group-data-[orientation=horizontal]/step:block group-data-[orientation=horizontal]/step:w-full group-data-[orientation=horizontal]/step:min-w-0",
        className,
      )}
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
