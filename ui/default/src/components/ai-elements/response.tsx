"use client";

// Adapted from Vercel AI Elements `message.tsx` (MessageResponse) —
// https://github.com/vercel/ai-elements, Apache-2.0; see ./LICENSE.
// Divergences: code blocks render through the project's vitesse ShikiCode
// (dual-theme CSS vars, .doc-code shell) instead of @streamdown/code, and
// the math/mermaid/cjk plugins are omitted.
import ShikiCode from "@/components/ShikiCode";
import { cn } from "@/lib/utils";
import type { ComponentProps, HTMLAttributes, ReactNode } from "react";
import { isValidElement, memo } from "react";
import { Streamdown } from "streamdown";

function nodeText(node: ReactNode): string {
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(nodeText).join("");
  if (isValidElement(node)) {
    return nodeText((node.props as { children?: ReactNode }).children);
  }
  return "";
}

// Block code arrives as <pre><code class="language-x">…</code></pre>; the
// override re-routes it through ShikiCode. Inline code keeps Streamdown's
// default rendering.
const CodePre = (props: HTMLAttributes<HTMLPreElement>) => {
  const code = Array.isArray(props.children) ? props.children[0] : props.children;
  let language = "";
  if (isValidElement(code)) {
    const className = (code.props as { className?: string }).className ?? "";
    language = /language-([\w-]+)/.exec(className)?.[1] ?? "";
  }
  return (
    <div className="doc-code my-3 text-sm">
      <ShikiCode language={language} text={nodeText(code).replace(/\n$/, "")} />
    </div>
  );
};

const components = { pre: CodePre };

export type ResponseProps = ComponentProps<typeof Streamdown>;

export const Response = memo(
  ({ className, ...props }: ResponseProps) => (
    <Streamdown
      className={cn(
        "size-full text-sm leading-6 [&>*:first-child]:mt-0 [&>*:last-child]:mb-0",
        className,
      )}
      components={components}
      {...props}
    />
  ),
  (prevProps, nextProps) => prevProps.children === nextProps.children,
);

Response.displayName = "Response";
