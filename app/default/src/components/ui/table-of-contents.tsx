import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `table-of-contents`.
 *
 * Ownership: Valon Registry
 * (`valon-tools/apps/registry/ui/src/ui/table-of-contents.tsx`).
 * Pair with `useScrollSpy` for active-section tracking.
 *
 * NOTE: `kind: "separator"` is a console forward-port until Registry lands the
 * same item union — keep this file in sync when Registry ships separators.
 */

export type TableOfContentsLinkItem = {
  /** Omitted or `"link"` — navigates to a section. */
  kind?: "link";
  id: string;
  title: string;
  /** Heading depth; 1 = top-level. Indentation uses depth 2 / 3+. */
  depth: number;
};

export type TableOfContentsSeparatorItem = {
  kind: "separator";
  /** Stable React key only — not a scroll target. */
  id: string;
};

export type TableOfContentsItem =
  | TableOfContentsLinkItem
  | TableOfContentsSeparatorItem;

export function isTableOfContentsLink(
  item: TableOfContentsItem,
): item is TableOfContentsLinkItem {
  return item.kind !== "separator";
}

export type TableOfContentsProps = {
  items: TableOfContentsItem[];
  activeId?: string | null;
  onItemSelect?: (id: string) => void;
  /** Accessible name for the nav. Default "On this page". */
  label?: string;
  className?: string;
  listClassName?: string;
  /**
   * When set, TOC owns a definite-height scrollport (height + max-height).
   * Scrollport padding is the inset contract: `ps`/`py` keep item washes off the
   * clip edges; `pe` keeps overlay scrollbar thumbs off fills
   * (`scrollbar-gutter` alone is ignored by overlay scrollbars on macOS).
   */
  maxHeight?: string;
};

function depthPaddingClass(depth: number): string {
  if (depth <= 1) return "pl-2";
  if (depth === 2) return "pl-4";
  return "pl-8";
}

/**
 * In-page table of contents list. Presentational only — pair with
 * `useScrollSpy` / `pickActiveSection` for active-section tracking.
 *
 * Active styles follow List Item surface tokens (wash fill + vivid left rail);
 * hover/press snap (no color transition) per motion-tokens.
 *
 * Separators (`kind: "separator"`) are decorative horizontal rules between
 * groups — not scroll targets and not announced to AT.
 */
function TableOfContents({
  items,
  activeId = null,
  onItemSelect,
  label = "On this page",
  className,
  listClassName,
  maxHeight,
}: TableOfContentsProps) {
  if (items.length === 0) return null;

  const list = (
    <ul className={cn("space-y-0.5", listClassName)}>
      {items.map((item) => {
        if (item.kind === "separator") {
          return (
            <li
              key={item.id}
              role="separator"
              aria-hidden
              className="my-2 list-none px-2"
            >
              <div className="h-px bg-border" />
            </li>
          );
        }

        const isActive = item.id === activeId;
        return (
          <li key={item.id}>
            <button
              type="button"
              onClick={() => onItemSelect?.(item.id)}
              aria-current={isActive ? "true" : undefined}
              className={cn(
                "block w-full truncate border-l-2 py-1 pr-2 text-left text-sm focus-ring",
                depthPaddingClass(item.depth),
                isActive
                  ? [
                      // Flush left rail: no start radius so the vivid border reads as a continuous edge.
                      "rounded-r-sm border-accent-vivid bg-accent-wash font-medium text-foreground",
                      "hover:bg-accent-wash-hover active:bg-accent-wash-pressed",
                    ]
                  : [
                      "rounded-sm border-transparent text-muted-foreground",
                      "hover:bg-list-item-hover hover:text-foreground",
                      "active:bg-list-item-pressed active:text-foreground",
                    ],
              )}
              title={item.title}
            >
              {item.title}
            </button>
          </li>
        );
      })}
    </ul>
  );

  return (
    <nav
      aria-label={label}
      data-slot="table-of-contents"
      className={cn(
        maxHeight && "flex min-h-0 flex-col overflow-hidden",
        className,
      )}
      // `height` (not only max-height) so flex-1 scroll children get a definite
      // block size — otherwise overflow clips without scrolling.
      style={maxHeight ? { height: maxHeight, maxHeight } : undefined}
    >
      {maxHeight ? (
        // Scrollport owns item inset: py/ps keep washes off clip edges; pe-* is
        // the overlay-scrollbar gutter (thumbs paint in the padding band).
        // ps matches pe so the vivid left rail + wash aren't flush to the column.
        <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain py-1 pe-2 ps-1">
          {list}
        </div>
      ) : (
        list
      )}
    </nav>
  );
}

export { TableOfContents };
