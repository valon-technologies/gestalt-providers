export function formatRegistryTime(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export function shortenSourceRef(sourceRef?: string): string {
  const ref = sourceRef?.trim();
  if (!ref) return "";
  return ref.length > 7 ? ref.slice(0, 7) : ref;
}

export function isActiveRegistryRollout(state?: string): boolean {
  return state === "enrolling" || state === "restarting";
}
