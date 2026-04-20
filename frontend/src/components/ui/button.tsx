import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "inline-flex items-center justify-center rounded-xl border text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus-ring)] disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        default:
          "border-transparent bg-[linear-gradient(145deg,var(--btn-primary-from),var(--btn-primary-to))] text-white shadow-sm hover:brightness-105",
        secondary:
          "border-transparent bg-[linear-gradient(145deg,var(--btn-secondary-from),var(--btn-secondary-to))] text-white shadow-sm hover:brightness-105",
        outline:
          "border-[var(--border-strong)] bg-[var(--surface)] text-[var(--ink-2)] hover:bg-[var(--surface-2)] hover:text-[var(--ink)]",
        ghost:
          "border-transparent bg-transparent text-[var(--ink-3)] hover:bg-[var(--surface-2)] hover:text-[var(--ink)]",
      },
      size: {
        default: "h-10 px-4 py-2",
        sm: "h-8 rounded-lg px-3 text-xs",
        lg: "h-11 px-5 text-sm",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, ...props }, ref) => {
    return <button className={cn(buttonVariants({ variant, size, className }))} ref={ref} {...props} />;
  },
);

Button.displayName = "Button";
