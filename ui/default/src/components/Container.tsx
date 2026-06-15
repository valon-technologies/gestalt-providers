import type { ElementType, ReactNode } from "react";

// Pages and the nav share one centered content column whose max width is the
// theme's --content-max-width token. width="full" is the explicit opt-out for
// surfaces that own the whole viewport (e.g. /agents).
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
    width === "full" ? "w-full" : "mx-auto w-full max-w-content px-6";
  return (
    <Tag className={className ? `${widthClassName} ${className}` : widthClassName}>
      {children}
    </Tag>
  );
}
