import type { ElementType, ReactNode } from "react";

// Pages and the nav share one stable, app-owned content column. width="full"
// is the explicit opt-out for surfaces that own the whole viewport; tenant
// themes do not change layout density.
export type ContainerWidth = "content" | "full";

export default function Container({
  as: Tag = "div",
  width = "content",
  className,
  children,
}: {
  as?: ElementType;
  width?: ContainerWidth;
  className?: string;
  children: ReactNode;
}) {
  const widthClassName =
    width === "full" ? "w-full" : "mx-auto w-full max-w-7xl px-6";
  return (
    <Tag className={className ? `${widthClassName} ${className}` : widthClassName}>
      {children}
    </Tag>
  );
}
