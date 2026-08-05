/** Prev/next position within an ordered sibling list (detail pagination). */
export function siblingNavigation(
  ids: readonly string[],
  currentId: string,
): {
  index: number;
  total: number;
  previousId: string | null;
  nextId: string | null;
} {
  const index = ids.indexOf(currentId);
  return {
    index,
    total: ids.length,
    previousId: index > 0 ? (ids[index - 1] ?? null) : null,
    nextId:
      index >= 0 && index < ids.length - 1 ? (ids[index + 1] ?? null) : null,
  };
}
