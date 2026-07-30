
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import {
  createHeaderChrome,
  createHeaderChromeScale,
  type HeaderChromeTierTable,
} from "./header-chrome";

type SectionHeaderSize = "sm" | "default" | "lg" | "md";

const SECTION_HEADER_ALIGN_CENTER =
  "[&_[data-slot=section-header-content]]:items-center [&_[data-slot=section-header-actions]]:justify-center";

/**
 * Canonical section-header tier table. `iconStackPadding` must equal the SVG
 * box height plus `contentGapY` for each tier.
 */
const SECTION_HEADER_TIERS = {
  sm: {
    contentGapY: "gap-y-1.5",
    stackedRowGapY:
      "[&:has([data-slot=section-header-content][data-size=sm])]:gap-y-1.5",
    title: "font-sans text-heading-sm",
    description: "text-xs",
    icon: "[&_svg:not([class*='size-'])]:size-4",
    iconStackPadding: "pt-5.5",
  },
  default: {
    contentGapY: "gap-y-2.5",
    stackedRowGapY:
      "[&:has([data-slot=section-header-content][data-size=default])]:gap-y-2.5",
    title: "font-display text-heading-xl tracking-display",
    description: "text-base",
    icon: "[&_svg:not([class*='size-'])]:size-8",
    iconStackPadding: "pt-10.5",
  },
  lg: {
    contentGapY: "gap-y-2",
    stackedRowGapY:
      "[&:has([data-slot=section-header-content][data-size=lg])]:gap-y-2",
    title: "font-display text-heading-lg tracking-heading",
    description: "text-sm",
    icon: "[&_svg:not([class*='size-'])]:size-6",
    iconStackPadding: "pt-8",
  },
  md: {
    contentGapY: "gap-y-2.5",
    stackedRowGapY:
      "[&:has([data-slot=section-header-content][data-size=md])]:gap-y-2.5",
    title: "font-display text-heading-xl tracking-display",
    description: "text-base",
    icon: "[&_svg:not([class*='size-'])]:size-8",
    iconStackPadding: "pt-10.5",
  },
} as const satisfies HeaderChromeTierTable<SectionHeaderSize>;

const SECTION_HEADER_SCALE = createHeaderChromeScale(SECTION_HEADER_TIERS);

type SectionHeaderIconStack = {
  readonly [K in SectionHeaderSize]: {
    readonly svg: string;
    readonly gapY: string;
    readonly textPad: string;
  };
};

/** Compatibility projection for the existing icon-rhythm contract test/API. */
export const SECTION_HEADER_ICON_STACK = Object.fromEntries(
  Object.entries(SECTION_HEADER_TIERS).map(([size, tier]) => [
    size,
    {
      svg: tier.icon,
      gapY: tier.contentGapY,
      textPad: tier.iconStackPadding,
    },
  ]),
) as SectionHeaderIconStack;

const {
  Header: SectionHeader,
  Content: SectionHeaderContent,
  Icon: SectionHeaderIcon,
  Title: SectionHeaderTitle,
  Description: SectionHeaderDescription,
  Actions: SectionHeaderActions,
  headerVariants: sectionHeaderVariants,
  contentVariants: sectionHeaderContentVariants,
  iconVariants: sectionHeaderIconVariants,
  titleVariants: sectionHeaderTitleVariants,
  descriptionVariants: sectionHeaderDescriptionVariants,
} = createHeaderChrome<"div", SectionHeaderSize>({
  slotPrefix: "section-header",
  rootElement: "div",
  alignBetweenItems: "sm:items-baseline",
  alignCenterClasses: SECTION_HEADER_ALIGN_CENTER,
  defaultSize: "default",
  title: { kind: "section", defaultTag: "h2" },
  ...SECTION_HEADER_SCALE,
});

export {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderIcon,
  SectionHeaderTitle,
  SectionHeaderDescription,
  SectionHeaderActions,
  sectionHeaderVariants,
  sectionHeaderContentVariants,
  sectionHeaderIconVariants,
  sectionHeaderTitleVariants,
  sectionHeaderDescriptionVariants,
};
