
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import {
  createHeaderChrome,
  createHeaderChromeScale,
  type HeaderChromeTierTable,
} from "./header-chrome";

type SectionHeaderSize = "xs" | "sm" | "default" | "lg" | "md";

const SECTION_HEADER_ALIGN_CENTER =
  "[&_[data-slot=section-header-content]]:items-center [&_[data-slot=section-header-actions]]:justify-center";

/**
 * Canonical section-header tier table. `iconStackPadding` must equal the SVG
 * box height plus `contentGapY` for each tier.
 *
 * `xs` is one step below Heading SM (`text-base`) for nested section titles
 * inside a dialog or other sm header. Heading SM ships 18px type on a 24px
 * line; beside control-sm (32px) that extra leading makes `items-baseline`
 * look vertically centered. `leading-none` keeps the title box on the glyph
 * line so actions sit on the title baseline.
 */
const SECTION_HEADER_TIERS = {
  xs: {
    contentGapY: "gap-y-1.5",
    stackedRowGapY:
      "[&:has([data-slot=section-header-content][data-size=xs])]:gap-y-1.5",
    title: "font-sans text-base leading-none",
    description: "text-xs",
    icon: "[&_svg:not([class*='size-'])]:size-4",
    iconStackPadding: "pt-5.5",
  },
  sm: {
    contentGapY: "gap-y-1.5",
    stackedRowGapY:
      "[&:has([data-slot=section-header-content][data-size=sm])]:gap-y-1.5",
    title: "font-sans text-heading-sm leading-none",
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
const SECTION_HEADER_STACKED_ROW_GAP_Y = SECTION_HEADER_SCALE.stackedRowGapY;

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
  stackedRowGapY: SECTION_HEADER_STACKED_ROW_GAP_Y,
  defaultSize: "default",
  title: { kind: "section", defaultTag: "h2" },
  scale: SECTION_HEADER_SCALE.scale,
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
