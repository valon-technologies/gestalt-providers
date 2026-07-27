/**
 * Home-shell Oxlint plugin — console UI invariants for app/default.
 *
 * Keep this package local to gestalt-providers. Do not path-depend on
 * toolshed's private oxlint-gestalt (different token model + overwrite-surface
 * rules that do not apply here).
 */

const BRAND_TEXT_ON_SELECTED =
  /(?:^|[\s"'`])(?:data-active|data-\[active\]|data-\[selected\]|data-\[state=active\]):text-(?:brand|gold-\d+)(?=$|[\s"'`/])/;

const REMEDIATION =
  "Selected chrome uses accent fill + ink foreground (bg-accent-subtle / text-accent-foreground) — not text-brand or text-gold-*. See src/components/ui/PORTING.md.";

function isInsideCnCall(node) {
  let cur = node.parent;
  while (cur) {
    if (
      cur.type === "CallExpression" &&
      cur.callee?.type === "Identifier" &&
      cur.callee.name === "cn"
    ) {
      return true;
    }
    cur = cur.parent;
  }
  return false;
}

function staticStringsFromCnCall(node) {
  const parts = [];
  for (const arg of node.arguments ?? []) {
    if (arg.type === "Literal" && typeof arg.value === "string") {
      parts.push(arg.value);
      continue;
    }
    if (arg.type === "TemplateLiteral") {
      for (const el of arg.quasis ?? []) {
        const raw = el.value?.raw ?? "";
        if (raw) parts.push(raw);
      }
    }
  }
  return parts.join(" ");
}

function classStringVisitors(context, check) {
  return {
    Literal(node) {
      if (typeof node.value !== "string") return;
      if (isInsideCnCall(node)) return;
      const message = check(node.value);
      if (message) context.report({ node, message });
    },
    TemplateElement(node) {
      if (isInsideCnCall(node)) return;
      const raw = node.value?.raw ?? "";
      if (!raw) return;
      const message = check(raw);
      if (message) context.report({ node, message });
    },
    CallExpression(node) {
      if (node.callee?.type !== "Identifier" || node.callee.name !== "cn") {
        return;
      }
      const merged = staticStringsFromCnCall(node);
      if (!merged) return;
      const message = check(merged);
      if (message) context.report({ node, message });
    },
  };
}

const plugin = {
  meta: {
    name: "home",
  },
  rules: {
    /**
     * Accent roles color the FILL; on-accent text stays ink.
     * Forbidden: data-active:text-brand / text-gold-* on selected chrome.
     */
    "no-brand-text-on-selected": {
      create(context) {
        return classStringVisitors(context, (text) =>
          BRAND_TEXT_ON_SELECTED.test(text) ? REMEDIATION : null,
        );
      },
    },
  },
};

export default plugin;
