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
      // theme tokens, same as Badge / Alert.
      richColors={false}
      toastOptions={{
        classNames: {
          toast:
            "group toast !duration-toast !ease-out-back group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-lg",
          // Use --badge-* status washes, not --success / --error. Tenants often
          // remap --success to a solid brand green (legacy grove) without a
          // matching light --success-foreground — that yields dark-on-dark
          // toasts. Badge tokens stay pale wash + on-tint ink (Registry color.md).
          // !important beats sonner's [data-styled] surface + shared .toast neutrals.
          success:
            "!bg-badge-success !text-badge-success-foreground !border-transparent [&_[data-description]]:!text-badge-success-foreground [&_svg]:!text-badge-success-foreground",
          warning:
            "!bg-badge-warning !text-badge-warning-foreground !border-transparent [&_[data-description]]:!text-badge-warning-foreground [&_svg]:!text-badge-warning-foreground",
          error:
            "!bg-badge-destructive !text-badge-destructive-foreground !border-transparent [&_[data-description]]:!text-badge-destructive-foreground [&_svg]:!text-badge-destructive-foreground",
          info: "!bg-badge-info !text-badge-info-foreground !border-transparent [&_[data-description]]:!text-badge-info-foreground [&_svg]:!text-badge-info-foreground",
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
