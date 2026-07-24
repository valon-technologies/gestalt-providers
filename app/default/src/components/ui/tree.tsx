"use client";


/**
 * Gestalt console vendor of Valon Registry `tree`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/tree.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import type { ItemInstance, TreeInstance } from "@headless-tree/core";
import { ChevronDownIcon, MinusIcon, PlusIcon } from "lucide-react";
import { cva, type VariantProps } from "class-variance-authority";

import { listItemInteraction } from "@/lib/list-item-interaction";
import { cn } from "@/lib/cn";

type ToggleIconType = "chevron" | "plus-minus";

export type TreeSize = "sm" | "default" | "lg";

/** Vertical rhythm between rows and inside row labels — orthogonal to `size` (glyph scale). */
export type TreeDensity = "default" | "condensed";

/** Default per-level indent (px) when callers omit `indent` on `Tree`. */
export const TREE_INDENT_BY_SIZE: Record<TreeSize, number> = {
  sm: 16,
  default: 20,
  lg: 24,
};

/** ReUI c-tree-2 vertical indent guides — 1px line centered in each indent column. */
export const treeIndentGuidesClassName =
  "relative before:pointer-events-none before:absolute before:inset-0 before:-z-10 before:content-[''] before:bg-[repeating-linear-gradient(to_right,transparent_0,transparent_calc(var(--tree-indent)/2-0.5px),var(--border)_calc(var(--tree-indent)/2-0.5px),var(--border)_calc(var(--tree-indent)/2+0.5px),transparent_calc(var(--tree-indent)/2+0.5px),transparent_var(--tree-indent))]";

const treeGutterVariants = cva(
  "flex shrink-0 items-center justify-center w-[var(--tree-indent)]",
  {
    variants: {
      size: {
        sm: "",
        default: "",
        lg: "",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

const treeToggleIconVariants = cva(
  "pointer-events-none shrink-0 text-muted-foreground",
  {
    variants: {
      size: {
        sm: "size-3",
        default: "size-3.5",
        lg: "size-4",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

const treeItemLabelVariants = cva(
  [
    "bg-background flex w-full min-w-0 items-center rounded-md",
    listItemInteraction({ pointer: "css-group" }),
  ].join(" "),
  {
    variants: {
      size: {
        sm: "gap-1 pe-1.5 text-xs",
        default: "gap-1.5 pe-2 text-sm",
        lg: "gap-2 pe-2.5 text-base",
      },
      density: {
        default: "",
        condensed: "",
      },
    },
    compoundVariants: [
      { size: "sm", density: "default", class: "py-1" },
      { size: "default", density: "default", class: "py-1.5" },
      { size: "lg", density: "default", class: "py-2" },
      { size: "sm", density: "condensed", class: "py-0.5" },
      { size: "default", density: "condensed", class: "py-0.5" },
      { size: "lg", density: "condensed", class: "py-1" },
    ],
    defaultVariants: {
      size: "default",
      density: "default",
    },
  },
);

const treeContainerVariants = cva("flex flex-col", {
  variants: {
    size: {
      sm: "",
      default: "",
      lg: "",
    },
    density: {
      default: "",
      condensed: "gap-0",
    },
  },
  compoundVariants: [
    { density: "default", size: "sm", class: "gap-px" },
    { density: "default", size: "default", class: "gap-0.5" },
    { density: "default", size: "lg", class: "gap-0.5" },
  ],
  defaultVariants: {
    size: "default",
    density: "default",
  },
});

const treeLeafGutterVariants = treeGutterVariants;

/** Lucide stroke width for +/- and chevron toggles. */
const TREE_TOGGLE_STROKE_WIDTH = 3;

type TreeContextValue<T = unknown> = {
  indent: number;
  size: TreeSize;
  density: TreeDensity;
  /** Folder expand via whole row (`row`) or +/- gutter only (`toggle`). */
  expandActivation: "row" | "toggle";
  currentItem?: ItemInstance<T>;
  itemOnClick?: React.MouseEventHandler<HTMLElement>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  tree?: TreeInstance<any>;
  toggleIconType?: ToggleIconType;
};

const TreeContext = React.createContext<TreeContextValue>({
  indent: TREE_INDENT_BY_SIZE.default,
  size: "default",
  density: "default",
  expandActivation: "row",
  toggleIconType: "plus-minus",
});

function useTreeContext<T = unknown>() {
  return React.useContext(TreeContext) as TreeContextValue<T>;
}

type TreeProps = React.HTMLAttributes<HTMLDivElement> & {
  indent?: number;
  size?: TreeSize;
  /** Tighter row padding and inter-row gap without shrinking glyphs (`size`). */
  density?: TreeDensity;
  /** Vertical indent guides in the gutter (ReUI c-tree-2). Row labels stay opaque. */
  showIndentGuides?: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  tree?: TreeInstance<any>;
  toggleIconType?: ToggleIconType;
  /** `toggle` keeps row inert so nested controls (e.g. Checkbox) stay valid HTML. */
  expandActivation?: "row" | "toggle";
};

function Tree({
  indent,
  size = "default",
  density = "default",
  showIndentGuides = false,
  tree,
  className,
  toggleIconType = "plus-minus",
  expandActivation = "row",
  style,
  ...props
}: TreeProps) {
  const resolvedIndent = indent ?? TREE_INDENT_BY_SIZE[size];
  const containerProps =
    tree && typeof tree.getContainerProps === "function"
      ? tree.getContainerProps()
      : {};
  const { style: containerStyle, ...containerRest } = containerProps;

  return (
    <TreeContext.Provider
      value={{ indent: resolvedIndent, size, density, tree, toggleIconType, expandActivation }}
    >
      <div
        data-slot="tree"
        data-size={size}
        data-density={density}
        style={
          {
            ...style,
            ...containerStyle,
            "--tree-indent": `${resolvedIndent}px`,
          } as React.CSSProperties
        }
        className={cn(
          treeContainerVariants({ size, density }),
          showIndentGuides && treeIndentGuidesClassName,
          className,
        )}
        {...props}
        {...containerRest}
      />
    </TreeContext.Provider>
  );
}

type TreeItemProps<T = unknown> = Omit<
  React.HTMLAttributes<HTMLDivElement>,
  "children"
> & {
  item: ItemInstance<T>;
  children?: React.ReactNode;
};

function TreeItem<T = unknown>({
  item,
  className,
  children,
  style,
  onClick,
  ...props
}: TreeItemProps<T>) {
  const parentContext = useTreeContext<T>();
  const { indent, expandActivation } = parentContext;
  const itemProps =
    typeof item.getProps === "function" ? item.getProps() : {};
  const {
    style: itemStyle,
    onClick: itemOnClick,
    ...itemRest
  } = itemProps;

  return (
    <TreeContext.Provider
      value={{
        ...parentContext,
        currentItem: item as ItemInstance<unknown>,
        itemOnClick,
      }}
    >
      <div
        data-slot="tree-item"
        style={
          {
            ...style,
            ...itemStyle,
            "--tree-padding": `${item.getItemMeta().level * indent}px`,
          } as React.CSSProperties
        }
        className={cn(
          "group z-10 w-full ps-(--tree-padding) text-left outline-hidden select-none focus:z-20 focus-ring disabled:pointer-events-none disabled:opacity-50",
          className,
        )}
        data-focus={
          typeof item.isFocused === "function"
            ? item.isFocused() || false
            : undefined
        }
        data-folder={
          typeof item.isFolder === "function"
            ? item.isFolder() || false
            : undefined
        }
        data-selected={
          typeof item.isSelected === "function"
            ? item.isSelected() || false
            : undefined
        }
        onClick={
          expandActivation === "row"
            ? (onClick ?? itemOnClick)
            : onClick
        }
        {...itemRest}
        {...props}
      >
        {children}
      </div>
    </TreeContext.Provider>
  );
}

type TreeItemLabelProps<T = unknown> = React.HTMLAttributes<HTMLSpanElement> &
  VariantProps<typeof treeItemLabelVariants> & {
    item?: ItemInstance<T>;
  };

function TreeItemLabel<T = unknown>({
  item: propItem,
  size: sizeProp,
  density: densityProp,
  children,
  className,
  ...props
}: TreeItemLabelProps<T>) {
  const {
    currentItem,
    toggleIconType,
    size: contextSize,
    density: contextDensity,
    expandActivation,
    itemOnClick,
  } = useTreeContext<T>();
  const item = propItem ?? currentItem;
  const size = sizeProp ?? contextSize;
  const density = densityProp ?? contextDensity;

  if (!item) {
    return null;
  }

  const toggleIconClassName = treeToggleIconVariants({ size });

  const renderToggleIcon = () =>
    toggleIconType === "plus-minus" ? (
      item.isExpanded() ? (
        <MinusIcon
          className={toggleIconClassName}
          strokeWidth={TREE_TOGGLE_STROKE_WIDTH}
        />
      ) : (
        <PlusIcon
          className={toggleIconClassName}
          strokeWidth={TREE_TOGGLE_STROKE_WIDTH}
        />
      )
    ) : (
      <ChevronDownIcon
        className={cn(
          toggleIconClassName,
          "in-aria-[expanded=false]:-rotate-90",
        )}
        strokeWidth={TREE_TOGGLE_STROKE_WIDTH}
      />
    );

  const folderGutter =
    expandActivation === "toggle" ? (
      <button
        type="button"
        data-slot="tree-item-toggle"
        className={cn(treeGutterVariants({ size }), "focus-ring rounded-md")}
        aria-label={item.isExpanded() ? "Collapse" : "Expand"}
        onPointerDown={(event) => {
          // Keep nested toggles from activating an ancestor <label> (choice cards).
          event.preventDefault();
          event.stopPropagation();
        }}
        onClick={(event) => {
          event.stopPropagation();
          itemOnClick?.(event);
        }}
      >
        {renderToggleIcon()}
      </button>
    ) : (
      <span aria-hidden className={treeGutterVariants({ size })}>
        {renderToggleIcon()}
      </span>
    );

  return (
    <span
      data-slot="tree-item-label"
      className={cn(treeItemLabelVariants({ size, density }), className)}
      {...props}
    >
      {item.isFolder() ? (
        folderGutter
      ) : (
        // Same width as the folder toggle gutter — do not also add `ps-*` on leaves
        // or nested checkbox columns double-indent (L2→L3 step ≠ L1→L2).
        <span aria-hidden className={treeLeafGutterVariants({ size })} />
      )}
      {children ??
        (typeof item.getItemName === "function" ? item.getItemName() : null)}
    </span>
  );
}

export { Tree, TreeItem, TreeItemLabel, treeItemLabelVariants };
