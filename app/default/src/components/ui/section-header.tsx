
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import { createHeaderChrome } from "./header-chrome";

/** Static Tailwind literals — must stay complete strings for `@source` emission. */
const SECTION_HEADER_STACKED_ROW_GAP_Y = [
  "[&:has([data-slot=section-header-content][data-size=sm])]:gap-y-1.5",
  "[&:has([data-slot=section-header-content][data-size=default])]:gap-y-2.5",
  "[&:has([data-slot=section-header-content][data-size=md])]:gap-y-2.5",
] as const;

const SECTION_HEADER_ALIGN_CENTER =
  "[&_[data-slot=section-header-content]]:items-center [&_[data-slot=section-header-actions]]:justify-center";

/**
 * Per-tier icon stack rhythm — single source of truth.
 * `textPad` must equal svg box height + `gapY` (e.g. sm: 16px + 6px → pt-5.5 / 22px).
 */
export const SECTION_HEADER_ICON_STACK = {
  sm: {
    svg: "[&_svg:not([class*='size-'])]:size-4",
    gapY: "gap-y-1.5",
    textPad: "pt-5.5",
  },
  default: {
    svg: "[&_svg:not([class*='size-'])]:size-8",
    gapY: "gap-y-2.5",
    textPad: "pt-10.5",
  },
  md: {
    svg: "[&_svg:not([class*='size-'])]:size-8",
    gapY: "gap-y-2.5",
    textPad: "pt-10.5",
  },
} as const;

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
} = createHeaderChrome({
  slotPrefix: "section-header",
  rootElement: "div",
  alignBetweenItems: "sm:items-baseline",
  alignCenterClasses: SECTION_HEADER_ALIGN_CENTER,
  stackedRowGapY: SECTION_HEADER_STACKED_ROW_GAP_Y,
  defaultSize: "default",
  title: { kind: "section", defaultTag: "h2" },
  scale: {
    contentGapY: {
      sm: SECTION_HEADER_ICON_STACK.sm.gapY,
      default: SECTION_HEADER_ICON_STACK.default.gapY,
      md: SECTION_HEADER_ICON_STACK.md.gapY,
    },
    title: {
      sm: "font-sans text-heading-sm",
      default: "font-display text-heading-xl tracking-display",
      md: "font-display text-heading-xl tracking-display",
    },
    description: { sm: "text-xs", default: "text-base", md: "text-base" },
    icon: {
      sm: SECTION_HEADER_ICON_STACK.sm.svg,
      default: SECTION_HEADER_ICON_STACK.default.svg,
      md: SECTION_HEADER_ICON_STACK.md.svg,
    },
    iconStackPadding: {
      sm: SECTION_HEADER_ICON_STACK.sm.textPad,
      default: SECTION_HEADER_ICON_STACK.default.textPad,
      md: SECTION_HEADER_ICON_STACK.md.textPad,
    },
  },
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
