import { ButtonHTMLAttributes } from "react";

type Variant = "primary" | "secondary" | "danger";

const variantStyles: Record<Variant, string> = {
  primary:
    "bg-primary text-primary-foreground hover:bg-primary/90",
  secondary:
    "bg-accent text-accent-foreground hover:bg-accent/80",
  danger:
    "bg-destructive text-destructive-foreground hover:bg-destructive/90",
};

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
}

export default function Button({
  variant = "primary",
  className = "",
  disabled,
  children,
  ...props
}: ButtonProps) {
  return (
    <button
      className={`rounded-md px-6 py-2.5 text-sm font-medium transition-all duration-150 ease-out disabled:opacity-50 disabled:cursor-not-allowed active:translate-y-px ${variantStyles[variant]} ${className}`}
      disabled={disabled}
      {...props}
    >
      {children}
    </button>
  );
}
