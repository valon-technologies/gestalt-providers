import type { ElementType, ReactNode } from "react";

// Pages and the nav share one centered content column whose max width is the
// theme's --content-max-width token.
export default function Container({
  as: Tag = "div",
  className,
  children,
}: {
  as?: ElementType;
  className?: string;
  children: ReactNode;
}) {
  const widthClassName = "mx-auto w-full max-w-content px-6";
  return (
    <Tag className={className ? `${widthClassName} ${className}` : widthClassName}>
      {children}
    </Tag>
  );
}
