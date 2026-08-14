import type { ElementType, ReactNode } from "react";

// Pages and the nav share one stable, app-owned content column. width="full"
// is the explicit opt-out for surfaces that own the whole viewport; tenant
// themes do not change layout density.
//
// `py-16` (4rem) is the page inset under AppTopBar. Sticky Pane gap in
// globals.css uses the same 4rem so rails share this seam. Do not re-pad
// individual routes.
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
    width === "full" ? "w-full py-16" : "mx-auto w-full max-w-7xl px-6 py-16";
  return (
    <Tag className={className ? `${widthClassName} ${className}` : widthClassName}>
      {children}
    </Tag>
  );
}
