import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "input-group.tsx"),
  "utf8",
);
const INPUT_SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "input.tsx"),
  "utf8",
);
const TEXTAREA_SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "textarea.tsx"),
  "utf8",
);

/**
 * InputGroup shell owns control height (`h-control-*` via size). The inner
 * control must fill with `h-full!` and must not be the second height owner.
 * Addon owns muted→disabled text color; InputGroupText must not pin
 * `text-muted-foreground` or it wins over the disabled recolor.
 */
describe("InputGroup height and disabled-text contracts", () => {
  test("shell size variants set h-control-*", () => {
    expect(SOURCE).toContain("h-control-sm");
    expect(SOURCE).toContain("h-control-default");
    expect(SOURCE).toContain("h-control-lg");
    expect(SOURCE).toMatch(/sm:\s*"h-control-sm/);
    expect(SOURCE).toMatch(/default:\s*\n?\s*"h-control-default/);
    expect(SOURCE).toMatch(/lg:\s*"h-control-lg/);
  });

  test("InputGroupInput fills shell height instead of re-applying h-control", () => {
    expect(SOURCE).toContain("h-full!");
    expect(SOURCE).toContain("InputGroupSizeContext");
  });

  test("InputGroupText does not pin muted color over addon disabled recolor", () => {
    const textBlock = SOURCE.slice(
      SOURCE.indexOf("function InputGroupText"),
      SOURCE.indexOf("function InputGroupInput"),
    );
    expect(textBlock).not.toContain("text-muted-foreground");
    expect(SOURCE).toContain(
      "group-has-[[data-slot=input-group-control]:disabled]/input-group:text-disabled-foreground",
    );
  });

  test("addon click focuses control with focusVisible for shell ring", () => {
    expect(SOURCE).toContain("focus({ focusVisible: true })");
  });

  test("button edge spacing targets input-group-button marker (Tooltip-safe)", () => {
    expect(SOURCE).toContain(
      "has-[[data-input-group-button]]:ml-[-0.45rem]",
    );
    expect(SOURCE).toContain(
      "has-[[data-input-group-button]]:mr-[-0.45rem]",
    );
    expect(SOURCE).toContain('data-input-group-button=""');
    expect(SOURCE).not.toContain("has-[>button]:ml-");
    expect(SOURCE).not.toContain("has-[>button]:mr-");
    expect(SOURCE).not.toContain("has-[[data-slot=input-group-button]]:ml-");
  });

  test("addon composes caller onClick after focus handler ownership", () => {
    const addon = SOURCE.slice(
      SOURCE.indexOf("function InputGroupAddon"),
      SOURCE.indexOf("function InputGroupButton"),
    );
    expect(addon).toContain("onClick?.(e)");
    expect(addon).toContain("if (e.defaultPrevented) return");
    // Own onClick must win over {...props} — props spread before onClick.
    const propsSpread = addon.indexOf("{...props}");
    const onClickAssign = addon.indexOf("onClick={(e)");
    expect(propsSpread).toBeGreaterThan(-1);
    expect(onClickAssign).toBeGreaterThan(propsSpread);
  });

  test("InputGroupTextarea includes min-w-0 like InputGroupInput", () => {
    const block = SOURCE.slice(SOURCE.indexOf("function InputGroupTextarea"));
    expect(block).toContain("min-w-0");
  });

  test("disabled control flips addon cursor and skips focus", () => {
    expect(SOURCE).toContain(
      "group-has-[[data-slot=input-group-control]:disabled]/input-group:cursor-not-allowed",
    );
    expect(SOURCE).toContain("control.disabled");
  });

  test("no CSS order on inline addons — DOM order is tab order", () => {
    expect(SOURCE).not.toContain("order-first");
    expect(SOURCE).not.toContain("order-last");
  });

  test("addon and text type scale follow shell data-size", () => {
    expect(SOURCE).toContain(
      "group-data-[size=sm]/input-group:text-control-sm",
    );
    expect(SOURCE).toContain(
      "group-data-[size=lg]/input-group:text-control-lg",
    );
  });

  test("shell owns block-layout control min-height; InputGroupInput does not", () => {
    const inputBlock = SOURCE.slice(
      SOURCE.indexOf("function InputGroupInput"),
      SOURCE.indexOf("function InputGroupTextarea"),
    );
    expect(inputBlock).not.toContain("min-h-control-");
    expect(SOURCE).toContain(
      "[&>[data-slot=input-group-control]]:min-h-control-sm",
    );
    expect(SOURCE).toContain(
      "[&>[data-slot=input-group-control]]:min-h-control-default",
    );
    expect(SOURCE).toContain(
      "[&>[data-slot=input-group-control]]:min-h-control-lg",
    );
    expect(SOURCE).toContain("has-[>[data-align=block-start]]");
    expect(SOURCE).toContain("has-[>[data-align=block-end]]");
  });

  test("InputGroupTextarea reads size context for type scale", () => {
    const block = SOURCE.slice(SOURCE.indexOf("function InputGroupTextarea"));
    expect(block).toContain("InputGroupSizeContext");
    expect(block).toContain("text-control-lg");
  });

  test("InputGroupInput and Textarea accept ref as a prop and pass it through", () => {
    const inputBlock = SOURCE.slice(
      SOURCE.indexOf("function InputGroupInput"),
      SOURCE.indexOf("function InputGroupTextarea"),
    );
    const textareaBlock = SOURCE.slice(SOURCE.indexOf("function InputGroupTextarea"));
    // React 19: ref is a normal prop — do not wrap these in forwardRef.
    expect(inputBlock).not.toContain("forwardRef");
    expect(textareaBlock).not.toContain("forwardRef");
    expect(inputBlock).toMatch(/\{\s*className,\s*size: sizeProp,\s*ref,/);
    expect(textareaBlock).toMatch(/\{\s*className,\s*ref,/);
    expect(inputBlock).toContain("ref={ref}");
    expect(textareaBlock).toContain("ref={ref}");
  });

  test("shell uses items-baseline so InputGroupText shares the value baseline", () => {
    expect(SOURCE).toContain("items-baseline");
    expect(SOURCE).not.toMatch(/group\/input-group[^"]*items-center/);
  });

  test("shell hides native search clear when inline-end button is present", () => {
    expect(SOURCE).toContain(
      "has-[>[data-align=inline-end]_[data-input-group-button]]:[&_[data-slot=input-group-control][type=search]::-webkit-search-cancel-button]:hidden",
    );
  });

  test("InputGroupInput publishes control disabled state to addons", () => {
    const shell = SOURCE.slice(
      SOURCE.indexOf("function InputGroup("),
      SOURCE.indexOf("const inputGroupAddonVariants"),
    );
    const block = SOURCE.slice(
      SOURCE.indexOf("function InputGroupInput"),
      SOURCE.indexOf("function InputGroupTextarea"),
    );
    expect(shell).toContain("InputGroupControlDisabledContext.Provider");
    expect(shell).toContain("InputGroupControlDisabledRegistrarContext.Provider");
    expect(block).toContain("registerControlDisabled?.(!!disabled)");
    expect(block).toContain("useLayoutEffect");
    expect(block).toContain("disabled={disabled}");
  });

  test("InputGroupClearAddon reserves inline-end chrome while hidden", () => {
    const typeBlock = SOURCE.slice(
      SOURCE.indexOf("type InputGroupClearAddonProps"),
      SOURCE.indexOf("function InputGroupClearAddon"),
    );
    const block = SOURCE.slice(
      SOURCE.indexOf("function InputGroupClearAddon"),
      SOURCE.indexOf("function InputGroupText"),
    );
    expect(typeBlock).toContain('"disabled"');
    expect(block).toContain('align="inline-end"');
    expect(block).toContain("pointer-events-none invisible");
    expect(block).toContain("InputGroupControlDisabledContext");
    expect(block).toContain("const interactive = visible && !controlDisabled");
    expect(block).toContain("disabled={!interactive}");
    expect(block).toContain("aria-hidden={!interactive}");
    expect(block).toContain("tabIndex={interactive ? undefined : -1}");
    expect(block).toContain("event.preventDefault()");
    expect(block).toContain('if (!visible) return');
    expect(block).toContain('if (!control || control.disabled) return');
    expect(block).toContain("onClear();");
    expect(block).toContain("control.focus({ focusVisible: true })");
    const disabledGuard = block.indexOf('if (!control || control.disabled) return');
    const onClearCall = block.indexOf("onClear();");
    expect(disabledGuard).toBeGreaterThan(-1);
    expect(onClearCall).toBeGreaterThan(disabledGuard);
    const propsSpread = block.indexOf("{...props}");
    const disabledAssign = block.indexOf("disabled={!interactive}");
    const classNameAssign = block.indexOf("className={cn(");
    expect(propsSpread).toBeGreaterThan(-1);
    expect(disabledAssign).toBeGreaterThan(propsSpread);
    expect(classNameAssign).toBeGreaterThan(propsSpread);
  });
});

/**
 * Shell owns focus-ring via :has(control:focus-visible). Inner controls must
 * omit Input/Textarea focus-ring at the source — not via outline-none override
 * (`.focus-ring:focus-visible` beats `focus-visible:outline-none`).
 */
describe("InputGroup focus-ring contracts", () => {
  test("InputGroupInput omits chrome from public API and pins group after spread", () => {
    const inputBlock = SOURCE.slice(
      SOURCE.indexOf("function InputGroupInput"),
      SOURCE.indexOf("function InputGroupTextarea"),
    );
    expect(inputBlock).toContain('Omit<InputProps, "chrome">');
    expect(inputBlock).toContain('chrome="group"');
    expect(inputBlock).not.toContain("focus-visible:outline-none");
    const spread = inputBlock.indexOf("{...props}");
    const chrome = inputBlock.indexOf('chrome="group"');
    expect(spread).toBeGreaterThan(-1);
    expect(chrome).toBeGreaterThan(spread);
  });

  test("group chrome suppresses UA outline at the Input/Textarea source", () => {
    expect(INPUT_SOURCE).toMatch(/group:\s*"[^"]*focus-visible:outline-none/);
    expect(TEXTAREA_SOURCE).toMatch(/group:\s*"[^"]*focus-visible:outline-none/);
    const inputBlock = SOURCE.slice(
      SOURCE.indexOf("function InputGroupInput"),
      SOURCE.indexOf("function InputGroupTextarea"),
    );
    const textareaBlock = SOURCE.slice(SOURCE.indexOf("function InputGroupTextarea"));
    expect(inputBlock).not.toContain("focus-visible:outline-none");
    expect(textareaBlock).not.toContain("focus-visible:outline-none");
  });

  test("InputGroupTextarea omits chrome from public API and pins group after spread", () => {
    const block = SOURCE.slice(SOURCE.indexOf("function InputGroupTextarea"));
    expect(block).toContain('Omit<React.ComponentProps<typeof Textarea>, "chrome">');
    expect(block).toContain('chrome="group"');
    expect(block).not.toContain("focus-visible:outline-none");
    const spread = block.indexOf("{...props}");
    const chrome = block.indexOf('chrome="group"');
    expect(spread).toBeGreaterThan(-1);
    expect(chrome).toBeGreaterThan(spread);
  });
});
