import { type ReactNode } from "react";

/** Wrap `**label**` segments in `<strong>` for Registry typeset weight. */
export function RecipeEmphasis({ text }: { text: string }): ReactNode {
  const parts = text.split("**");
  if (parts.length === 1) return text;
  return parts.map((part, index) =>
    index % 2 === 1 ? <strong key={index}>{part}</strong> : part,
  );
}
