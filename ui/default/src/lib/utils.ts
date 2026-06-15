import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

// shadcn-convention class combiner, used by the vendored ai-elements
// components (and any future registry pulls expecting `@/lib/utils`).
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
