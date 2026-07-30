
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import {
  createHeaderChrome,
  createHeaderChromeScale,
  type HeaderChromeTierTable,
} from "./header-chrome";

type PageHeaderSize = "sm" | "default" | "md" | "lg" | "xl" | "entity";

/**
 * Canonical page-header tier table. Keep the complete Tailwind literals here;
 * `createHeaderChromeScale` derives all cva maps and row selectors from it.
 */
const PAGE_HEADER_TIERS = {
  sm: {
    contentGapY: "gap-y-1.5",
    stackedRowGapY:
      "[&:has([data-slot=page-header-content][data-size=sm])]:gap-y-1.5",
    title: "font-sans text-heading-md",
    description: "text-xs",
    icon: "[&_svg:not([class*='size-'])]:size-5",
  },
  default: {
    contentGapY: "gap-y-2",
    stackedRowGapY:
      "[&:has([data-slot=page-header-content][data-size=default])]:gap-y-2",
    title: "font-display text-heading-lg tracking-heading",
    description: "text-sm",
    icon: "[&_svg:not([class*='size-'])]:size-6",
  },
  md: {
    contentGapY: "gap-y-2.5",
    stackedRowGapY:
      "[&:has([data-slot=page-header-content][data-size=md])]:gap-y-2.5",
    title: "font-display text-heading-xl tracking-display",
    description: "text-base",
    icon: "[&_svg:not([class*='size-'])]:size-8",
  },
  lg: {
    contentGapY: "gap-y-3",
    stackedRowGapY:
      "[&:has([data-slot=page-header-content][data-size=lg])]:gap-y-3",
    title: "font-display text-display-sm tracking-display",
    description: "text-body-lg",
    icon: "[&_svg:not([class*='size-'])]:size-10",
  },
  xl: {
    contentGapY: "gap-y-5",
    stackedRowGapY:
      "[&:has([data-slot=page-header-content][data-size=xl])]:gap-y-5",
    title: "font-display text-display-xl tracking-display-tight",
    description: "text-heading-lg",
    icon: "[&_svg:not([class*='size-'])]:size-12",
  },
  entity: {
    contentGapY: "gap-y-3",
    stackedRowGapY:
      "[&:has([data-slot=page-header-content][data-size=entity])]:gap-y-3",
    title: "font-display text-display-sm tracking-display",
    description: "text-body-lg",
    icon: "[&_svg:not([class*='size-'])]:size-10",
  },
} as const satisfies HeaderChromeTierTable<PageHeaderSize>;

const PAGE_HEADER_ALIGN_CENTER =
  "[&_[data-slot=page-header-content]]:items-center [&_[data-slot=page-header-actions]]:justify-center";

const PAGE_HEADER_SCALE = createHeaderChromeScale(PAGE_HEADER_TIERS);

const {
  Header: PageHeader,
  Content: PageHeaderContent,
  Icon: PageHeaderIcon,
  Title: PageHeaderTitle,
  Description: PageHeaderDescription,
  Actions: PageHeaderActions,
  headerVariants: pageHeaderVariants,
  contentVariants: pageHeaderContentVariants,
  iconVariants: pageHeaderIconVariants,
  titleVariants: pageHeaderTitleVariants,
  descriptionVariants: pageHeaderDescriptionVariants,
} = createHeaderChrome<"header", PageHeaderSize>({
  slotPrefix: "page-header",
  rootElement: "header",
  alignBetweenItems: "sm:items-end",
  alignCenterClasses: PAGE_HEADER_ALIGN_CENTER,
  defaultSize: "default",
  title: { kind: "page" },
  ...PAGE_HEADER_SCALE,
});

export {
  PageHeader,
  PageHeaderContent,
  PageHeaderIcon,
  PageHeaderTitle,
  PageHeaderDescription,
  PageHeaderActions,
  pageHeaderVariants,
  pageHeaderContentVariants,
  pageHeaderIconVariants,
  pageHeaderTitleVariants,
  pageHeaderDescriptionVariants,
};
