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
import { FileIcon, FolderIcon } from "lucide-react";

import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/cn";

/**
 * Hierarchical multi-select built on Checkbox. The selection set is **leaf ids
 * only** — parent checked / indeterminate is always derived from descendants
 * (RES-20260721-004 / Flexnative Nested tree). Not a WAI-ARIA `role="tree"`
 * explorer; for deep keyboard trees prefer Headless Tree / React Aria Tree.
 */
export type CheckboxTreeNode = {
  id: string;
  label: string;
  children?: CheckboxTreeNode[];
};

export type CheckboxTreeCheckState = boolean | "indeterminate";

/** Depth-first leaf ids under `node` (the node itself when it has no children). */
export function getCheckboxTreeLeafIds(node: CheckboxTreeNode): string[] {
  return node.children?.length
    ? node.children.flatMap(getCheckboxTreeLeafIds)
    : [node.id];
}

/** Derive parent/leaf Checkbox `checked` from the leaf-id selection set. */
export function getCheckboxTreeCheckState(
  node: CheckboxTreeNode,
  checkedLeafIds: ReadonlySet<string>,
): CheckboxTreeCheckState {
  const leaves = getCheckboxTreeLeafIds(node);
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

export type CheckboxTreeProps = {
  tree: readonly CheckboxTreeNode[];
  /** Controlled selected leaf ids. */
  value?: readonly string[];
  /** Uncontrolled initial leaf ids. */
  defaultValue?: readonly string[];
  onValueChange?: (value: string[]) => void;
  /** Folder / file icons beside each label (Nested-tree demo default). */
  showIcons?: boolean;
  className?: string;
  size?: React.ComponentProps<typeof Checkbox>["size"];
};

function CheckboxTreeItem({
  node,
  checkedLeafIds,
  onToggle,
  depth,
  idPrefix,
  showIcons,
  size,
}: Readonly<{
  node: CheckboxTreeNode;
  checkedLeafIds: ReadonlySet<string>;
  onToggle: (leafIds: readonly string[], next: boolean) => void;
  depth: number;
  idPrefix: string;
  showIcons: boolean;
  size: React.ComponentProps<typeof Checkbox>["size"];
}>) {
  const leaves = getCheckboxTreeLeafIds(node);
  const state = getCheckboxTreeCheckState(node, checkedLeafIds);
  const inputId = `${idPrefix}-${node.id}`;
  const isFolder = Boolean(node.children?.length);

  return (
    <div className="flex flex-col gap-2.5">
      <div
        className="flex items-center gap-2.5"
        style={{ paddingLeft: depth * 20 }}
      >
        <Checkbox
          id={inputId}
          size={size}
          checked={state}
          onCheckedChange={(value) => onToggle(leaves, value === true)}
        />
        {showIcons ? (
          isFolder ? (
            <FolderIcon
              aria-hidden
              className="size-4 shrink-0 text-muted-foreground"
            />
          ) : (
            <FileIcon
              aria-hidden
              className="size-4 shrink-0 text-muted-foreground"
            />
          )
        ) : null}
        <Label htmlFor={inputId} variant="inline" className="font-normal">
          {node.label}
        </Label>
      </div>
      {node.children?.map((child) => (
        <CheckboxTreeItem
          key={child.id}
          node={child}
          checkedLeafIds={checkedLeafIds}
          onToggle={onToggle}
          depth={depth + 1}
          idPrefix={idPrefix}
          showIcons={showIcons}
          size={size}
        />
      ))}
    </div>
  );
}

const CheckboxTree = React.forwardRef<HTMLDivElement, CheckboxTreeProps>(
  (props, ref) => {
    const {
      tree,
      value,
      defaultValue,
      onValueChange,
      showIcons = true,
      className,
      size = "default",
    } = props;
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

    // Latest leaf set for composing rapid toggles before React re-renders
    // (or before a controlled parent writes `value` back). Sync only when the
    // controlled leaf-id key changes — not when the parent passes a new array
    // reference with the same ids — so optimistic drafts are not wiped.
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
      if (!valuePropPresent) setUncontrolled(draft);
      onValueChange?.([...draft]);
    };

    return (
      <div
        ref={ref}
        data-slot="checkbox-tree"
        className={cn("flex w-full max-w-xs flex-col gap-2.5", className)}
      >
        {tree.map((node) => (
          <CheckboxTreeItem
            key={node.id}
            node={node}
            checkedLeafIds={checkedLeafIds}
            onToggle={onToggle}
            depth={0}
            idPrefix={idPrefix}
            showIcons={showIcons}
            size={size}
          />
        ))}
      </div>
    );
  },
);
CheckboxTree.displayName = "CheckboxTree";

export { CheckboxTree };
