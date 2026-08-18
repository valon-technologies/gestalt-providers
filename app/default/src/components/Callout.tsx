import type { ComponentProps } from "react";
import type { VariantProps } from "class-variance-authority";

import {
  alertLayoutVariants,
  alertSurfaceVariants,
} from "@/components/ui/alert";
import { cn } from "@/lib/cn";

type CalloutProps = Omit<ComponentProps<"div">, "role"> & {
  variant?: VariantProps<typeof alertSurfaceVariants>["variant"];
};

/**
 * Persistent in-page guidance that reuses Alert wash and stacked layout.
 * Not a live region — status flashes belong on `Alert`.
 *
 * Compose with `AlertTitle` / `AlertDescription` / `AlertActions` and a
 * leading `>svg` the same way as Alert.
 */
export function Callout({
  className,
  variant = "default",
  children,
  ...divProps
}: CalloutProps) {
  return (
    <div
      data-slot="callout"
      data-layout="default"
      data-variant={variant ?? "default"}
      className={cn(alertSurfaceVariants({ variant }), className)}
      {...divProps}
    >
      <div data-slot="alert-content" className="w-full">
        <div
          data-slot="alert-layout"
          className={alertLayoutVariants({ layout: "default" })}
        >
          {children}
        </div>
      </div>
    </div>
  );
}
