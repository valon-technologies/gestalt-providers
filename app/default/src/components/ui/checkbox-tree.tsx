"use client";


/**
 * Gestalt console vendor of Valon Registry `checkbox-tree`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/checkbox-tree.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import { hotkeysCoreFeature, syncDataLoaderFeature } from "@headless-tree/core";
import { useTree } from "@headless-tree/react";

import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Tree, TreeItem, TreeItemLabel, TREE_INDENT_BY_SIZE, type TreeDensity } from "@/components/ui/tree";
import { cn } from "@/lib/cn";

const CHECKBOX_TREE_ROOT_ID = "__checkbox-tree-root__";

/**
 * Hierarchical multi-select on a collapsible Headless Tree spine (ReUI c-tree-4
 * chrome + Flexnative checkbox-07 selection). Leaf ids are the source of truth;
 * parent checked / indeterminate is always derived from descendants.
 */
export type CheckboxTreeNode = {
  id: string;
  label: string;
  children?: CheckboxTreeNode[];
};

export type CheckboxTreeCheckState = boolean | "indeterminate";

export type CheckboxTreeFlatItem = {
  id: string;
  label: string;
  children?: string[];
};

/** Depth-first leaf ids under `node` (the node itself when it has no children). */
export function getCheckboxTreeLeafIds(node: CheckboxTreeNode): string[] {
  return node.children?.length
    ? node.children.flatMap(getCheckboxTreeLeafIds)
    : [node.id];
}

export function getCheckboxTreeLeafIdsFromFlat(
  item: CheckboxTreeFlatItem,
  items: Readonly<Record<string, CheckboxTreeFlatItem>>,
): string[] {
  return item.children?.length
    ? item.children.flatMap((childId) =>
        getCheckboxTreeLeafIdsFromFlat(items[childId]!, items),
      )
    : [item.id];
}

/** Derive parent/leaf Checkbox `checked` from the leaf-id selection set. */
export function getCheckboxTreeCheckState(
  node: CheckboxTreeNode,
  checkedLeafIds: ReadonlySet<string>,
): CheckboxTreeCheckState {
  const leaves = getCheckboxTreeLeafIds(node);
  return getCheckboxTreeCheckStateFromLeaves(leaves, checkedLeafIds);
}

export function getCheckboxTreeCheckStateFromFlat(
  item: CheckboxTreeFlatItem,
  items: Readonly<Record<string, CheckboxTreeFlatItem>>,
  checkedLeafIds: ReadonlySet<string>,
): CheckboxTreeCheckState {
  const leaves = getCheckboxTreeLeafIdsFromFlat(item, items);
  return getCheckboxTreeCheckStateFromLeaves(leaves, checkedLeafIds);
}

function getCheckboxTreeCheckStateFromLeaves(
  leaves: readonly string[],
  checkedLeafIds: ReadonlySet<string>,
): CheckboxTreeCheckState {
  if (leaves.length === 0) return false;
  let n = 0;
  for (const id of leaves) {
    if (checkedLeafIds.has(id)) n += 1;
  }
  if (n === 0) return false;
  if (n === leaves.length) return true;
  return "indeterminate";
}

/** Immutable toggle of one or more leaf ids in the selection set. */
export function setCheckboxTreeLeaves(
  checkedLeafIds: ReadonlySet<string>,
  leafIds: readonly string[],
  next: boolean,
): Set<string> {
  const draft = new Set(checkedLeafIds);
  for (const id of leafIds) {
    if (next) draft.add(id);
    else draft.delete(id);
  }
  return draft;
}

/**
 * Controllable leaf selection — same ownership contract as DatePicker.
 * Prop presence (`"value" in props`) decides controlled mode; a present but
 * empty/cleared `value` must win over internal state.
 */
export function resolveCheckboxTreeSelection(options: {
  valuePropPresent: boolean;
  value: readonly string[] | undefined;
  uncontrolled: ReadonlySet<string>;
}): Set<string> {
  return options.valuePropPresent
    ? new Set(options.value ?? [])
    : new Set(options.uncontrolled);
}

/** Flat index for Headless Tree dataLoader + selection helpers. */
export function indexCheckboxTree(nodes: readonly CheckboxTreeNode[]): {
  rootIds: string[];
  items: Record<string, CheckboxTreeFlatItem>;
} {
  const items: Record<string, CheckboxTreeFlatItem> = {};
  const walk = (node: CheckboxTreeNode) => {
    items[node.id] = {
      id: node.id,
      label: node.label,
      children: node.children?.map((child) => child.id),
    };
    node.children?.forEach(walk);
  };
  nodes.forEach(walk);
  return { rootIds: nodes.map((node) => node.id), items };
}

export type CheckboxTreeProps = {
  tree: readonly CheckboxTreeNode[];
  /** Controlled selected leaf ids. */
  value?: readonly string[];
  /** Uncontrolled initial leaf ids. */
  defaultValue?: readonly string[];
  onValueChange?: (value: string[]) => void;
  /** Branch ids expanded on first render. Defaults to none — parents with checked leaves auto-expand. */
  defaultExpanded?: readonly string[];
  /** Per-level indent in px. Defaults from `size` (`TREE_INDENT_BY_SIZE`). */
  indent?: number;
  className?: string;
  /** Row + checkbox scale — `sm` / `default` / `lg` (control-sizing.md). */
  size?: React.ComponentProps<typeof Checkbox>["size"];
  /** Tighter vertical rhythm without shrinking glyphs — forwarded to `Tree`. */
  density?: TreeDensity;
};

const checkboxTreeRowGap: Record<
  NonNullable<CheckboxTreeProps["size"]>,
  string
> = {
  sm: "gap-1.5",
  default: "gap-2",
  lg: "gap-2.5",
};

const CheckboxTree = React.forwardRef<HTMLDivElement, CheckboxTreeProps>(
  (props, ref) => {
    const {
      tree,
      value,
      defaultValue,
      onValueChange,
      defaultExpanded,
      indent: indentProp,
      className,
      size: sizeProp = "default",
      density = "default",
    } = props;
    const size = sizeProp ?? "default";
    const indent = indentProp ?? TREE_INDENT_BY_SIZE[size];
    const idPrefix = React.useId();
    const valuePropPresent = "value" in props;
    const [uncontrolled, setUncontrolled] = React.useState(
      () => new Set(defaultValue ?? []),
    );
    const checkedLeafIds = resolveCheckboxTreeSelection({
      valuePropPresent,
      value,
      uncontrolled,
    });

    const { rootIds, items } = React.useMemo(
      () => indexCheckboxTree(tree),
      [tree],
    );

    const loaderItems = React.useMemo((): Record<string, CheckboxTreeFlatItem> => ({
        [CHECKBOX_TREE_ROOT_ID]: {
          id: CHECKBOX_TREE_ROOT_ID,
          label: "",
          children: rootIds,
        },
        ...items,
      }),
      [items, rootIds],
    );

    const initialExpandedItems = React.useMemo(
      () => [...(defaultExpanded ?? [])],
      // Headless Tree reads expandedItems only from initialState on mount.
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [],
    );

    const headlessTree = useTree<CheckboxTreeFlatItem>({
      initialState: { expandedItems: initialExpandedItems },
      indent,
      rootItemId: CHECKBOX_TREE_ROOT_ID,
      getItemName: (item) => item.getItemData().label,
      isItemFolder: (item) => (item.getItemData()?.children?.length ?? 0) > 0,
      dataLoader: {
        getItem: (itemId) => loaderItems[itemId],
        getChildren: (itemId) => loaderItems[itemId]?.children ?? [],
      },
      features: [syncDataLoaderFeature, hotkeysCoreFeature],
    });

    // Leaf→folder transitions (e.g. lazy-loaded children) do not rebuild the
    // visible item list unless we explicitly refresh the tree cache.
    React.useLayoutEffect(() => {
      headlessTree.rebuildTree();
    }, [loaderItems, headlessTree]);

    // Open branches when lazy-loaded children arrive for an already-checked app.
    React.useLayoutEffect(() => {
      for (const id of Object.keys(loaderItems)) {
        if (id === CHECKBOX_TREE_ROOT_ID) continue;
        const data = loaderItems[id]!;
        if (!data.children?.length) continue;
        const leaves = getCheckboxTreeLeafIdsFromFlat(data, loaderItems);
        if (!leaves.some((leaf) => checkedLeafIds.has(leaf))) continue;
        const item = headlessTree.getItemInstance(id);
        if (item.isFolder() && !item.isExpanded()) {
          item.expand();
        }
      }
      // Only when tree structure changes — not on every selection change, so
      // manual collapse is not immediately undone.
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [loaderItems, headlessTree]);

    const selectionRef = React.useRef(checkedLeafIds);
    const valueRef = React.useRef(value);
    valueRef.current = value;
    const controlledValueKey = valuePropPresent
      ? (value ?? []).join("\0")
      : null;
    React.useLayoutEffect(() => {
      if (valuePropPresent) {
        selectionRef.current = new Set(valueRef.current ?? []);
      }
    }, [valuePropPresent, controlledValueKey]);

    const onToggle = (leafIds: readonly string[], next: boolean) => {
      const draft = setCheckboxTreeLeaves(selectionRef.current, leafIds, next);
      selectionRef.current = draft;
      if (next) {
        for (const id of Object.keys(loaderItems)) {
          if (id === CHECKBOX_TREE_ROOT_ID) continue;
          const data = loaderItems[id]!;
          if (!data.children?.length) continue;
          const leaves = getCheckboxTreeLeafIdsFromFlat(data, loaderItems);
          if (!leafIds.some((leaf) => leaves.includes(leaf))) continue;
          const item = headlessTree.getItemInstance(id);
          if (item.isFolder() && !item.isExpanded()) {
            item.expand();
          }
        }
      }
      if (!valuePropPresent) setUncontrolled(draft);
      onValueChange?.([...draft]);
    };

    return (
      <div
        ref={ref}
        data-slot="checkbox-tree"
        className={cn("w-full max-w-xs", className)}
      >
        <Tree
          indent={indent}
          size={size}
          density={density}
          expandActivation="toggle"
          showIndentGuides
          tree={headlessTree}
          toggleIconType="plus-minus"
        >
          {headlessTree.getItems().map((item) => {
            const id = item.getId();
            if (id === CHECKBOX_TREE_ROOT_ID) return null;

            const data = loaderItems[id]!;
            const leaves = getCheckboxTreeLeafIdsFromFlat(data, loaderItems);
            const state = getCheckboxTreeCheckStateFromFlat(
              data,
              loaderItems,
              checkedLeafIds,
            );
            const inputId = `${idPrefix}-${id}`;

            return (
              <TreeItem key={id} item={item}>
                <TreeItemLabel>
                  <span
                    className={cn(
                      "flex min-w-0 items-center",
                      checkboxTreeRowGap[size],
                    )}
                  >
                    <Checkbox
                      id={inputId}
                      size={size}
                      checked={state}
                      onCheckedChange={(next) =>
                        onToggle(leaves, next === true)
                      }
                      onPointerDown={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                      }}
                      onClick={(event) => event.stopPropagation()}
                    />
                    <Label
                      htmlFor={inputId}
                      variant="inline"
                      className="truncate font-normal"
                      onPointerDown={(event) => event.stopPropagation()}
                    >
                      {item.getItemName()}
                    </Label>
                  </span>
                </TreeItemLabel>
              </TreeItem>
            );
          })}
        </Tree>
      </div>
    );
  },
);
CheckboxTree.displayName = "CheckboxTree";

export { CheckboxTree };
