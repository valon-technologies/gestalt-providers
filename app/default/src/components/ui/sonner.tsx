/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import type * as React from "react";
import { Toaster as Sonner } from "sonner";

type ToasterProps = React.ComponentProps<typeof Sonner>;

const Toaster = ({ ...props }: ToasterProps) => {
  return (
    <Sonner
      className="toaster group"
      // Disable Sonner's built-in HSL rich palette — status wash comes from
      // theme tokens (--success / --success-foreground), same as Alert.
      richColors={false}
      toastOptions={{
        classNames: {
          toast:
            "group toast !duration-toast !ease-out-back group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-lg",
          // Status toasts tint the whole surface in their hue so importance reads at
          // a glance — green = success, yellow = warning, red = error, blue = info. Icon,
          // title, and description all take the hue; the description gets a per-type
          // [data-description] override because its neutral-gray default dips below AA
          // (~4.2:1) on the light tints. Border dropped so each tint is one clean fill.
          // loading/plain toasts stay neutral. !important beats sonner's [data-styled]
          // surface + the shared neutral .toast classes, like the existing !duration.
          success:
            "!bg-success !text-success-foreground !border-transparent [&_[data-description]]:!text-success-foreground [&_svg]:!text-success-foreground",
          warning:
            "!bg-warning !text-warning-foreground !border-transparent [&_[data-description]]:!text-warning-foreground [&_svg]:!text-warning-foreground",
          error:
            "!bg-error !text-error-foreground !border-transparent [&_[data-description]]:!text-error-foreground [&_svg]:!text-error-foreground",
          info: "!bg-info !text-info-foreground !border-transparent [&_[data-description]]:!text-info-foreground [&_svg]:!text-info-foreground",
          description: "group-[.toast]:text-muted-foreground",
          actionButton: "group-[.toast]:bg-primary group-[.toast]:text-primary-foreground",
          cancelButton: "group-[.toast]:bg-muted group-[.toast]:text-muted-foreground",
        },
      }}
      {...props}
    />
  );
};

export { Toaster };
