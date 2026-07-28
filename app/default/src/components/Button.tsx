import {
  Button as RegistryButton,
  type ButtonProps as RegistryButtonProps,
} from "@/components/ui/button";

/**
 * Console Button — thin adapter over Registry `ui/button`.
 *
 * Legacy call sites used `primary` / `danger`; Registry uses `default` /
 * `destructive`. Prefer importing `{ Button }` from `@/components/ui/button`
 * for new code.
 */

type LegacyVariant = "primary" | "secondary" | "danger";

type ButtonProps = Omit<RegistryButtonProps, "variant"> & {
  variant?: LegacyVariant | RegistryButtonProps["variant"];
};

function mapVariant(
  variant: ButtonProps["variant"],
): RegistryButtonProps["variant"] {
  if (variant === "primary" || variant == null) return "default";
  if (variant === "danger") return "destructive";
  return variant;
}

export default function Button({
  variant = "primary",
  ...props
}: ButtonProps) {
  return <RegistryButton variant={mapVariant(variant)} {...props} />;
}
