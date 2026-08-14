/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

type AvatarGroupVariant = "motion" | "static";

type AvatarGroupContextValue = {
  variant: AvatarGroupVariant;
};

const AvatarGroupContext = React.createContext<AvatarGroupContextValue>({
  variant: "motion",
});

function useAvatarGroup() {
  return React.useContext(AvatarGroupContext);
}

export type AvatarGroupProps = React.ComponentPropsWithoutRef<"div"> & {
  /**
   * `motion` — stack expands on group hover, item hover lifts. `static` —
   * overlap only.
   */
  variant?: AvatarGroupVariant;
};

/**
 * Overlapping avatar stack. Compose with `Avatar` children inside
 * `AvatarGroupItem` (optional tooltip) and optional `AvatarGroupCount`.
 */
const AvatarGroup = React.forwardRef<HTMLDivElement, AvatarGroupProps>(
  ({ className, variant = "motion", ...props }, ref) => (
    <AvatarGroupContext.Provider value={{ variant }}>
      <div
        ref={ref}
        data-slot="avatar-group"
        data-variant={variant}
        role="group"
        className={cn("group/avatar-group flex items-center", className)}
        {...props}
      />
    </AvatarGroupContext.Provider>
  ),
);
AvatarGroup.displayName = "AvatarGroup";

const avatarGroupItemRingVariants = cva("[&_[data-slot=avatar]]:ring-2", {
  variants: {
    avatarRing: {
      background: "[&_[data-slot=avatar]]:ring-background",
      accent: "[&_[data-slot=avatar]]:ring-accent-vivid",
    },
  },
  defaultVariants: { avatarRing: "background" },
});

export type AvatarGroupItemProps = React.ComponentPropsWithoutRef<"span"> &
  VariantProps<typeof avatarGroupItemRingVariants> & {
    /** Full name (or label) shown in a tooltip on hover. */
    tooltip?: React.ReactNode;
  };

const AvatarGroupItem = React.forwardRef<HTMLSpanElement, AvatarGroupItemProps>(
  ({ className, tooltip, avatarRing, children, ...props }, ref) => {
    const { variant } = useAvatarGroup();
    const motion = variant === "motion";

    const item = (
      <span
        ref={ref}
        data-slot="avatar-group-item"
        className={cn(
          "relative inline-flex shrink-0",
          "[&:not(:first-child)]:-ml-2",
          motion &&
            "translate-y-0 scale-100 transition-[translate,scale,margin-left] duration-overshoot ease-out-back motion-reduce:transition-none group-hover/avatar-group:[&:not(:first-child)]:-ml-1 hover:z-10 hover:-translate-y-0.5 hover:scale-[1.04]",
          avatarGroupItemRingVariants({ avatarRing }),
          className,
        )}
        {...props}
      >
        {children}
      </span>
    );

    if (tooltip == null || tooltip === false) {
      return item;
    }

    return (
      <Tooltip>
        <TooltipTrigger asChild>{item}</TooltipTrigger>
        <TooltipContent sideOffset={2}>{tooltip}</TooltipContent>
      </Tooltip>
    );
  },
);
AvatarGroupItem.displayName = "AvatarGroupItem";

const avatarGroupCountVariants = cva(
  "relative flex shrink-0 items-center justify-center rounded-full bg-muted-strong font-medium text-muted-foreground ring-2 ring-background",
  {
    variants: {
      size: {
        sm: "size-6 text-[0.625rem]",
        default: "size-7 text-xs",
        lg: "size-8 text-sm",
        xl: "size-10 text-sm",
      },
    },
    defaultVariants: { size: "default" },
  },
);

export type AvatarGroupCountProps = React.ComponentPropsWithoutRef<"div"> &
  VariantProps<typeof avatarGroupCountVariants>;

/** Overflow chip (`+N`) sized to match Avatar sm/default/lg/xl. */
const AvatarGroupCount = React.forwardRef<HTMLDivElement, AvatarGroupCountProps>(
  ({ className, size, ...props }, ref) => {
    const { variant } = useAvatarGroup();
    const motion = variant === "motion";

    return (
      <div
        ref={ref}
        data-slot="avatar-group-count"
        className={cn(
          avatarGroupCountVariants({ size }),
          "[&:not(:first-child)]:-ml-2",
          motion &&
            "transition-[margin-left] duration-overshoot ease-out-back motion-reduce:transition-none group-hover/avatar-group:[&:not(:first-child)]:-ml-1",
          className,
        )}
        {...props}
      />
    );
  },
);
AvatarGroupCount.displayName = "AvatarGroupCount";

export {
  AvatarGroup,
  AvatarGroupItem,
  AvatarGroupCount,
  avatarGroupCountVariants,
};
export type { AvatarGroupVariant };
