import {
  type ComponentType,
  type KeyboardEvent,
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { cn } from "@/lib/cn";

export type SegmentedControlOption<V extends string = string> = {
  value: V;
  label: string;
  icon?: ComponentType<{ className?: string }>;
};

const NEXT_KEYS = new Set(["ArrowRight", "ArrowDown"]);
const PREV_KEYS = new Set(["ArrowLeft", "ArrowUp"]);

const SIZE_STYLES = {
  xs: {
    container: "p-px",
    square: "size-5",
    labelled: "h-5 px-2",
    icon: "size-3.5",
    text: "text-xs",
  },
  sm: {
    container: "p-px",
    square: "size-7",
    labelled: "h-7 px-2.5",
    icon: "size-4",
    text: "text-xs",
  },
  default: {
    container: "p-px",
    square: "size-8",
    labelled: "h-8 px-3",
    icon: "size-4",
    text: "text-sm",
  },
} as const;

export type SegmentedControlProps<V extends string = string> = {
  options: ReadonlyArray<SegmentedControlOption<V>>;
  value: V;
  onValueChange: (value: V) => void;
  label: string;
  orientation?: "horizontal" | "vertical";
  showLabels?: boolean;
  size?: "xs" | "sm" | "default";
  className?: string;
};

type PillRect = { left: number; top: number; width: number; height: number };

const useIsomorphicLayoutEffect =
  typeof window !== "undefined" ? useLayoutEffect : useEffect;

/** Sliding-pill segmented switcher with arrow-key roving focus. */
export function SegmentedControl<V extends string>({
  options,
  value,
  onValueChange,
  label,
  orientation = "horizontal",
  showLabels = false,
  size = "default",
  className,
}: SegmentedControlProps<V>) {
  const containerRef = useRef<HTMLDivElement>(null);
  const buttonsRef = useRef<Array<HTMLButtonElement | null>>([]);

  const count = options.length;
  const activeIndex = Math.max(
    0,
    options.findIndex((option) => option.value === value),
  );
  const isVertical = orientation === "vertical";

  const [animate, setAnimate] = useState(false);
  useEffect(() => setAnimate(true), []);

  const [pill, setPill] = useState<PillRect | null>(null);
  const measure = useCallback(() => {
    const btn = buttonsRef.current[activeIndex];
    if (!btn) return;
    const next: PillRect = {
      left: btn.offsetLeft,
      top: btn.offsetTop,
      width: btn.offsetWidth,
      height: btn.offsetHeight,
    };
    setPill((prev) =>
      prev &&
      prev.left === next.left &&
      prev.top === next.top &&
      prev.width === next.width &&
      prev.height === next.height
        ? prev
        : next,
    );
  }, [activeIndex]);

  useIsomorphicLayoutEffect(() => {
    measure();
  }, [measure, count, isVertical, showLabels, size]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || typeof ResizeObserver === "undefined") return undefined;
    const ro = new ResizeObserver(() => measure());
    ro.observe(el);
    return () => ro.disconnect();
  }, [measure]);

  function focusOption(index: number) {
    onValueChange(options[index].value);
    buttonsRef.current[index]?.focus();
  }

  function onKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    let next = activeIndex;
    if (NEXT_KEYS.has(event.key)) next = (activeIndex + 1) % count;
    else if (PREV_KEYS.has(event.key)) next = (activeIndex - 1 + count) % count;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = count - 1;
    else return;
    event.preventDefault();
    focusOption(next);
  }

  const styles = SIZE_STYLES[size];

  return (
    <div
      ref={containerRef}
      role="radiogroup"
      aria-label={label}
      onKeyDown={onKeyDown}
      className={cn(
        "relative inline-flex rounded-lg border border-alpha bg-base-100 dark:bg-surface-raised",
        styles.container,
        isVertical ? "flex-col" : "flex-row",
        className,
      )}
    >
      <span
        aria-hidden
        style={
          pill
            ? { left: pill.left, top: pill.top, width: pill.width, height: pill.height }
            : { opacity: 0 }
        }
        className={cn(
          "pointer-events-none absolute rounded-md bg-base-white shadow-card dark:bg-surface",
          animate &&
            "transition-[left,top,width,height] duration-overshoot ease-out-back-soft",
        )}
      />
      {options.map((option, index) => {
        const Icon = option.icon;
        const checked = option.value === value;
        return (
          <button
            key={option.value}
            ref={(node) => {
              buttonsRef.current[index] = node;
            }}
            type="button"
            role="radio"
            aria-checked={checked}
            aria-label={option.label}
            title={showLabels ? undefined : option.label}
            tabIndex={checked ? 0 : -1}
            onClick={() => onValueChange(option.value)}
            className={cn(
              "focus-ring relative z-10 inline-flex items-center justify-center gap-1.5 rounded-md font-medium text-muted transition-[color,background-color] duration-hover-out ease-out-quart hover:duration-hover-in hover:text-primary aria-checked:text-primary",
              !checked && "hover:bg-alpha-5 active:bg-alpha-10",
              styles.text,
              showLabels ? styles.labelled : styles.square,
              isVertical && showLabels && "w-full",
            )}
          >
            {Icon ? <Icon className={cn(styles.icon, "shrink-0")} /> : null}
            {showLabels ? <span>{option.label}</span> : null}
          </button>
        );
      })}
    </div>
  );
}
