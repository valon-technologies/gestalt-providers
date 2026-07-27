import type { HTMLAttributes } from "react";

export function RegistryCode({
  className = "",
  ...props
}: HTMLAttributes<HTMLElement>) {
  return (
    <code
      className={`inline-block max-w-full break-all rounded-sm border border-border bg-muted px-[0.25em] py-[0.12em] font-mono text-[0.9em] text-foreground ${className}`}
      {...props}
    />
  );
}
