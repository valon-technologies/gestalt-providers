import type { HTMLAttributes, ReactNode } from "react";

export function SectionHeader({
  className = "",
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={`flex w-full flex-col gap-3 sm:flex-row sm:items-baseline sm:justify-between ${className}`}
      {...props}
    />
  );
}

export function SectionHeaderContent({
  className = "",
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return <div className={`flex min-w-0 flex-col gap-1 ${className}`} {...props} />;
}

export function SectionHeaderTitle({
  className = "",
  ...props
}: HTMLAttributes<HTMLHeadingElement>) {
  return (
    <h2
      className={`text-xl font-heading font-normal text-primary ${className}`}
      {...props}
    />
  );
}

export function SectionHeaderDescription({
  className = "",
  ...props
}: HTMLAttributes<HTMLParagraphElement>) {
  return <p className={`text-sm text-muted ${className}`} {...props} />;
}

export function SectionHeaderActions({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={`flex shrink-0 items-center gap-2 ${className}`}>
      {children}
    </div>
  );
}
