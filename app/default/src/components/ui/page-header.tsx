
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { createHeaderChrome } from "./header-chrome";

/** Static Tailwind literals — must stay complete strings for `@source` emission. */
const PAGE_HEADER_STACKED_ROW_GAP_Y = [
  "[&:has([data-slot=page-header-content][data-size=sm])]:gap-y-1.5",
  "[&:has([data-slot=page-header-content][data-size=default])]:gap-y-2",
  "[&:has([data-slot=page-header-content][data-size=md])]:gap-y-2.5",
  "[&:has([data-slot=page-header-content][data-size=lg])]:gap-y-3",
  "[&:has([data-slot=page-header-content][data-size=xl])]:gap-y-5",
  "[&:has([data-slot=page-header-content][data-size=entity])]:gap-y-3",
] as const;

const PAGE_HEADER_ALIGN_CENTER =
  "[&_[data-slot=page-header-content]]:items-center [&_[data-slot=page-header-actions]]:justify-center";

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
} = createHeaderChrome({
  slotPrefix: "page-header",
  rootElement: "header",
  alignBetweenItems: "sm:items-end",
  alignCenterClasses: PAGE_HEADER_ALIGN_CENTER,
  stackedRowGapY: PAGE_HEADER_STACKED_ROW_GAP_Y,
  defaultSize: "default",
  title: { kind: "page" },
  scale: {
    contentGapY: { sm: "gap-y-1.5", default: "gap-y-2", md: "gap-y-2.5", lg: "gap-y-3", xl: "gap-y-5", entity: "gap-y-3" },
    title: {
      sm: "font-sans text-heading-md",
      default: "font-display text-heading-lg tracking-heading",
      md: "font-display text-heading-xl tracking-display",
      lg: "font-display text-display-sm tracking-display",
      xl: "font-display text-display-xl tracking-display-tight",
      entity: "font-display text-display-sm tracking-display",
    },
    description: { sm: "text-xs", default: "text-sm", md: "text-base", lg: "text-body-lg", xl: "text-heading-lg", entity: "text-body-lg" },
    icon: {
      sm: "[&_svg:not([class*='size-'])]:size-5",
      default: "[&_svg:not([class*='size-'])]:size-6",
      md: "[&_svg:not([class*='size-'])]:size-8",
      lg: "[&_svg:not([class*='size-'])]:size-10",
      xl: "[&_svg:not([class*='size-'])]:size-12",
      entity: "[&_svg:not([class*='size-'])]:size-10",
    },
  },
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
