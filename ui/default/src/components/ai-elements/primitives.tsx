"use client";

// Minimal local stand-ins for the shadcn/ui primitives that Vercel AI
// Elements composes (button, badge, collapsible, alert). The chat components
// in this directory are adapted from https://github.com/vercel/ai-elements
// (Apache-2.0); these primitives keep that code working without adopting the
// Radix/shadcn stack — see ./LICENSE.
import { cn } from "@/lib/utils";
import type { ComponentProps, HTMLAttributes, ReactNode } from "react";
import { createContext, useContext, useState } from "react";

type ButtonVariant = "default" | "secondary" | "outline" | "ghost" | "destructive";
type ButtonSize = "default" | "sm" | "icon" | "icon-sm";

const buttonVariants: Record<ButtonVariant, string> = {
  default: "bg-brand text-background hover:bg-brand/90",
  secondary: "bg-surface-raised text-foreground hover:bg-alpha-10",
  outline: "border border-alpha bg-background hover:bg-alpha-5 hover:border-alpha-strong",
  ghost: "text-muted hover:bg-alpha-5 hover:text-primary",
  destructive: "bg-danger text-background hover:bg-danger/90",
};

const buttonSizes: Record<ButtonSize, string> = {
  default: "h-9 px-3.5 text-sm",
  sm: "h-8 px-3 text-sm",
  icon: "size-9",
  "icon-sm": "size-7",
};

export type ButtonProps = ComponentProps<"button"> & {
  variant?: ButtonVariant;
  size?: ButtonSize;
};

export const Button = ({
  className,
  variant = "default",
  size = "default",
  ...props
}: ButtonProps) => (
  <button
    className={cn(
      "inline-flex shrink-0 items-center justify-center gap-1.5 whitespace-nowrap rounded-md font-medium outline-none transition-colors duration-150",
      "focus-visible:ring-2 focus-visible:ring-brand/50 disabled:pointer-events-none disabled:opacity-60",
      buttonVariants[variant],
      buttonSizes[size],
      className,
    )}
    {...props}
  />
);

export type BadgeProps = HTMLAttributes<HTMLSpanElement> & {
  variant?: "default" | "secondary" | "outline";
};

export const Badge = ({ className, variant = "default", ...props }: BadgeProps) => (
  <span
    className={cn(
      "inline-flex w-fit items-center gap-1 rounded-md border px-2 py-0.5 text-xs font-medium",
      variant === "default" && "border-transparent bg-brand text-background",
      variant === "secondary" && "border-transparent bg-alpha-5 text-secondary",
      variant === "outline" && "border-alpha text-secondary",
      className,
    )}
    {...props}
  />
);

interface CollapsibleContextValue {
  open: boolean;
  setOpen: (open: boolean) => void;
}

const CollapsibleContext = createContext<CollapsibleContextValue | null>(null);

function useCollapsible(component: string) {
  const context = useContext(CollapsibleContext);
  if (!context) {
    throw new Error(`${component} must be used within Collapsible`);
  }
  return context;
}

export type CollapsibleProps = HTMLAttributes<HTMLDivElement> & {
  open?: boolean;
  defaultOpen?: boolean;
  onOpenChange?: (open: boolean) => void;
};

export const Collapsible = ({
  className,
  open: openProp,
  defaultOpen = false,
  onOpenChange,
  children,
  ...props
}: CollapsibleProps) => {
  const [uncontrolledOpen, setUncontrolledOpen] = useState(defaultOpen);
  const open = openProp ?? uncontrolledOpen;
  const setOpen = (next: boolean) => {
    if (openProp === undefined) setUncontrolledOpen(next);
    onOpenChange?.(next);
  };

  return (
    <CollapsibleContext.Provider value={{ open, setOpen }}>
      <div data-state={open ? "open" : "closed"} className={className} {...props}>
        {children}
      </div>
    </CollapsibleContext.Provider>
  );
};

export type CollapsibleTriggerProps = ComponentProps<"button">;

export const CollapsibleTrigger = ({
  className,
  onClick,
  ...props
}: CollapsibleTriggerProps) => {
  const { open, setOpen } = useCollapsible("CollapsibleTrigger");
  return (
    <button
      type="button"
      aria-expanded={open}
      data-state={open ? "open" : "closed"}
      className={className}
      onClick={(event) => {
        onClick?.(event);
        if (!event.defaultPrevented) setOpen(!open);
      }}
      {...props}
    />
  );
};

export type CollapsibleContentProps = HTMLAttributes<HTMLDivElement>;

export const CollapsibleContent = ({
  className,
  children,
  ...props
}: CollapsibleContentProps) => {
  const { open } = useCollapsible("CollapsibleContent");
  if (!open) return null;
  return (
    <div data-state="open" className={className} {...props}>
      {children}
    </div>
  );
};

export type AlertProps = HTMLAttributes<HTMLDivElement>;

export const Alert = ({ className, ...props }: AlertProps) => (
  <div
    role="alert"
    className={cn(
      "w-full rounded-lg border border-alpha bg-surface px-4 py-3 text-sm text-foreground",
      className,
    )}
    {...props}
  />
);

export type AlertDescriptionProps = HTMLAttributes<HTMLDivElement>;

export const AlertDescription = ({ className, ...props }: AlertDescriptionProps) => (
  <div className={cn("text-sm text-secondary [&_p]:leading-relaxed", className)} {...props} />
);

export type ShimmerProps = {
  children: ReactNode;
  className?: string;
};

// CSS-only replacement for ai-elements' motion-based <Shimmer>: the moving
// highlight is a background-position keyframe (see --animate-shimmer in
// globals.css) over text painted via background-clip.
export const Shimmer = ({ children, className }: ShimmerProps) => (
  <span
    className={cn(
      "animate-shimmer inline-block bg-clip-text text-transparent",
      "bg-[linear-gradient(90deg,color-mix(in_oklab,var(--alpha-dark)_60%,transparent)_40%,var(--foreground)_50%,color-mix(in_oklab,var(--alpha-dark)_60%,transparent)_60%)]",
      "bg-[length:200%_100%]",
      className,
    )}
  >
    {children}
  </span>
);
