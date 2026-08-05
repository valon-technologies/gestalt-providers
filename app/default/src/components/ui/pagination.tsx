/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import {
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  MoreHorizontal,
} from "lucide-react";

import { cn } from "@/lib/cn";
import { Button } from "@/components/ui/button";

function Pagination({ className, ...props }: React.ComponentProps<"nav">) {
  return (
    <nav
      role="navigation"
      aria-label="pagination"
      data-slot="pagination"
      className={cn("mx-auto flex w-full justify-center", className)}
      {...props}
    />
  );
}

function PaginationContent({ className, ...props }: React.ComponentProps<"ul">) {
  return (
    <ul
      data-slot="pagination-content"
      className={cn("flex flex-row items-center gap-1", className)}
      {...props}
    />
  );
}

function PaginationItem({ ...props }: React.ComponentProps<"li">) {
  return <li data-slot="pagination-item" {...props} />;
}

type PaginationLinkProps = {
  isActive?: boolean;
  asChild?: boolean;
} & Pick<React.ComponentProps<typeof Button>, "size"> &
  React.ComponentProps<"a">;

function PaginationLink({
  className,
  isActive,
  size = "icon",
  asChild = false,
  children,
  ...props
}: PaginationLinkProps) {
  return (
    <Button
      asChild
      variant={isActive ? "outline" : "ghost"}
      size={size}
      className={cn("cursor-pointer", className)}
    >
      {asChild && React.isValidElement(children) ? (
        React.cloneElement(
          children as React.ReactElement<Record<string, unknown>>,
          {
            "aria-current": isActive ? "page" : undefined,
            "data-slot": "pagination-link",
            "data-active": isActive,
            ...props,
          },
        )
      ) : (
        <a
          aria-current={isActive ? "page" : undefined}
          data-slot="pagination-link"
          data-active={isActive}
          {...props}
        >
          {children}
        </a>
      )}
    </Button>
  );
}

function PaginationPrevious({
  className,
  size = "default",
  children,
  ...props
}: React.ComponentProps<typeof PaginationLink>) {
  return (
    <PaginationLink
      aria-label="Go to previous page"
      size={size}
      className={cn("gap-1 px-2.5", className)}
      {...props}
    >
      {children ?? (
        <>
          <ChevronLeft />
          <span>Previous</span>
        </>
      )}
    </PaginationLink>
  );
}

function PaginationNext({
  className,
  size = "default",
  children,
  ...props
}: React.ComponentProps<typeof PaginationLink>) {
  return (
    <PaginationLink
      aria-label="Go to next page"
      size={size}
      className={cn("gap-1 px-2.5", className)}
      {...props}
    >
      {children ?? (
        <>
          <span>Next</span>
          <ChevronRight />
        </>
      )}
    </PaginationLink>
  );
}

function PaginationFirst({
  className,
  ...props
}: React.ComponentProps<typeof PaginationLink>) {
  return (
    <PaginationLink
      aria-label="Go to first page"
      className={cn("[&_svg]:size-4", className)}
      {...props}
    >
      <ChevronsLeft />
    </PaginationLink>
  );
}

function PaginationLast({
  className,
  ...props
}: React.ComponentProps<typeof PaginationLink>) {
  return (
    <PaginationLink
      aria-label="Go to last page"
      className={cn("[&_svg]:size-4", className)}
      {...props}
    >
      <ChevronsRight />
    </PaginationLink>
  );
}

function PaginationEllipsis({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      aria-hidden
      data-slot="pagination-ellipsis"
      className={cn(
        "text-muted-foreground/70 flex size-control-default items-center justify-center",
        className,
      )}
      {...props}
    >
      <MoreHorizontal className="size-4" />
      <span className="sr-only">More pages</span>
    </span>
  );
}

export {
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationFirst,
  PaginationItem,
  PaginationLast,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
};
