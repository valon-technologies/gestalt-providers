
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

export type HeaderChromeScale = {
  contentGapY: Record<string, string>;
  title: Record<string, string>;
  description: Record<string, string>;
  icon: Record<string, string>;
  /** Top padding on the text stack when an icon is stacked above (section headers). */
  iconStackPadding?: Record<string, string>;
};

export type HeaderChromeTitleMode =
  | { kind: "page" }
  | { kind: "section"; defaultTag?: "h2" | "h3" };

export type HeaderChromeConfig = {
  slotPrefix: string;
  rootElement: "header" | "div";
  alignBetweenItems: string;
  /** Complete Tailwind literals for center align (slot selectors must be static for @source). */
  alignCenterClasses: string;
  /** Complete `:has([data-slot=…-content][data-size=…])` gap-y literals for stacked row rhythm. */
  stackedRowGapY: readonly string[];
  defaultSize: string;
  scale: HeaderChromeScale;
  title: HeaderChromeTitleMode;
};

const TITLE_INTERACTIVE_CLASSNAME =
  "cursor-pointer border-0 bg-transparent p-0 text-left font-[inherit] text-inherit no-underline hover:text-inherit focus-ring rounded-sm";

/** Stable role marker on factory Icon components — survives re-exports; not `child.type` alone. */
export const HEADER_CHROME_ICON_ROLE = "header-chrome-icon" as const;

export type HeaderChromeIconProps = Omit<React.ComponentProps<"div">, "aria-hidden"> & {
  "data-header-chrome-icon"?: boolean;
};

export type HeaderChromeIconComponent = React.FC<HeaderChromeIconProps> & {
  readonly headerChromeRole?: typeof HEADER_CHROME_ICON_ROLE;
};

export function isHeaderChromeIconChild(child: React.ReactNode): boolean {
  if (!React.isValidElement(child)) return false;
  const props = child.props as HeaderChromeIconProps;
  if (props["data-header-chrome-icon"] === true) return true;
  const type = child.type as HeaderChromeIconComponent;
  if (typeof type === "function" && type.headerChromeRole === HEADER_CHROME_ICON_ROLE) {
    return true;
  }
  return false;
}

export function createHeaderChrome<Root extends "header" | "div">(
  config: HeaderChromeConfig & { rootElement: Root },
) {
  const {
    slotPrefix,
    rootElement,
    alignBetweenItems,
    alignCenterClasses,
    stackedRowGapY,
    defaultSize,
    scale,
    title: titleMode,
  } = config;

  const rootSlot = slotPrefix;
  const contentSlot = `${slotPrefix}-content`;
  const iconSlot = `${slotPrefix}-icon`;
  const titleSlot = `${slotPrefix}-title`;
  const descriptionSlot = `${slotPrefix}-description`;
  const actionsSlot = `${slotPrefix}-actions`;

  const headerVariants = cva(
    ["flex w-full gap-x-4 gap-y-2", ...stackedRowGapY],
    {
      variants: {
        align: {
          between: cn("flex-col sm:flex-row sm:justify-between", alignBetweenItems),
          center: cn("flex-col items-center text-center", alignCenterClasses),
        },
      },
      defaultVariants: { align: "between" },
    },
  );

  const titleVariants = cva("font-normal text-balance text-foreground", {
    variants: { size: scale.title },
    defaultVariants: { size: defaultSize },
  });

  type HeaderSize = keyof typeof scale.title & string;
  const ScaleContext = React.createContext<HeaderSize>(defaultSize as HeaderSize);

  const contentVariants = cva("flex min-w-0 flex-col", {
    variants: { size: scale.contentGapY },
    defaultVariants: { size: defaultSize },
  });

  const iconVariants = cva(
    "flex shrink-0 items-center text-muted-foreground [&_svg]:pointer-events-none [&_svg]:shrink-0",
    { variants: { size: scale.icon }, defaultVariants: { size: defaultSize } },
  );

  const descriptionVariants = cva("max-w-xl text-balance text-muted-foreground", {
    variants: { size: scale.description },
    defaultVariants: { size: defaultSize },
  });

  type HeaderAlign = NonNullable<VariantProps<typeof headerVariants>["align"]>;
  type HeaderProps = React.ComponentProps<Root> & { align?: HeaderAlign };

  const AlignContext = React.createContext<HeaderAlign>("between");

  function Header({ className, align, ...props }: HeaderProps) {
    const resolvedAlign = align ?? "between";
    return (
      <AlignContext.Provider value={resolvedAlign}>
        {React.createElement(rootElement, {
          "data-slot": rootSlot,
          className: cn(headerVariants({ align: resolvedAlign }), className),
          ...props,
        })}
      </AlignContext.Provider>
    );
  }

  function Icon({ className, ...props }: HeaderChromeIconProps) {
    const size = React.useContext(ScaleContext);
    return (
      <div
        data-slot={iconSlot}
        data-header-chrome-icon
        data-size={size}
        className={cn(iconVariants({ size }), className)}
        aria-hidden="true"
        {...props}
      />
    );
  }
  Object.assign(Icon, { headerChromeRole: HEADER_CHROME_ICON_ROLE });

  function Content({
    className,
    size = defaultSize,
    children,
    ...props
  }: React.ComponentProps<"div"> & VariantProps<typeof contentVariants>) {
    const resolved = (size ?? defaultSize) as HeaderSize;
    const gapClasses = contentVariants({ size: resolved });
    const align = React.useContext(AlignContext);

    let body = children;
    let sectionHasIcon = false;
    if (titleMode.kind === "section") {
      const icons: React.ReactNode[] = [];
      const rest: React.ReactNode[] = [];
      for (const child of React.Children.toArray(children)) {
        if (isHeaderChromeIconChild(child)) {
          icons.push(child);
        } else {
          rest.push(child);
        }
      }
      if (icons.length > 0) {
        sectionHasIcon = true;
        const iconStackPadding = scale.iconStackPadding?.[resolved];
        // Icon is absolutely positioned so the in-flow text stack supplies the
        // flex-item baseline for `items-baseline` on the header row.
        body = (
          <div className="relative flex min-w-0 flex-col">
            <div
              className={cn(
                "absolute top-0 flex",
                align === "center" ? "left-1/2 -translate-x-1/2" : "left-0",
              )}
            >
              {icons}
            </div>
            <div className={cn("flex min-w-0 flex-col", gapClasses, iconStackPadding)}>{rest}</div>
          </div>
        );
      }
    }

    return (
      <ScaleContext.Provider value={resolved}>
        <div
          data-slot={contentSlot}
          data-size={resolved}
          className={cn(
            sectionHasIcon ? "min-w-0" : gapClasses,
            className,
          )}
          {...props}
        >
          {body}
        </div>
      </ScaleContext.Provider>
    );
  }

  function PageTitle({ className, size: sizeProp, href, onNavigate, children, ...props }: Omit<React.ComponentProps<"h1">, "onClick"> & VariantProps<typeof titleVariants> & { href?: string; onNavigate?: () => void }) {
    const scaleSize = React.useContext(ScaleContext);
    const size = sizeProp ?? scaleSize;
    let body = children;
    if (href) body = <a href={href} className={TITLE_INTERACTIVE_CLASSNAME}>{children}</a>;
    else if (onNavigate) body = <button type="button" className={TITLE_INTERACTIVE_CLASSNAME} onClick={onNavigate}>{children}</button>;
    return <h1 data-slot={titleSlot} data-size={size} className={cn(titleVariants({ size }), className)} {...props}>{body}</h1>;
  }

  function SectionTitle({ className, size: sizeProp, as: Comp = titleMode.kind === "section" ? (titleMode.defaultTag ?? "h2") : "h2", ...props }: Omit<React.ComponentProps<"h2">, "as"> & VariantProps<typeof titleVariants> & { as?: "h2" | "h3" }) {
    const scaleSize = React.useContext(ScaleContext);
    const size = sizeProp ?? scaleSize;
    return <Comp data-slot={titleSlot} data-size={size} className={cn(titleVariants({ size }), className)} {...props} />;
  }

  const Title = titleMode.kind === "page" ? PageTitle : SectionTitle;

  function Description({ className, ...props }: React.ComponentProps<"p">) {
    const size = React.useContext(ScaleContext);
    return <p data-slot={descriptionSlot} data-size={size} className={cn(descriptionVariants({ size }), className)} {...props} />;
  }

  function Actions({ className, ...props }: React.ComponentProps<"div">) {
    return <div data-slot={actionsSlot} className={cn("flex shrink-0 items-center gap-2", className)} {...props} />;
  }

  return { ScaleContext, AlignContext, Header, Content, Icon, Title, Description, Actions, headerVariants, contentVariants, iconVariants, titleVariants, descriptionVariants };
}
