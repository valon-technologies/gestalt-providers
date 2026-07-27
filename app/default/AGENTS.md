# app/default — agent instructions

Portable contract for coding agents (Cursor, Claude Code, Codex, Gemini CLI, etc.).
Human porting notes: [`src/components/ui/PORTING.md`](src/components/ui/PORTING.md).

## Gestalt UI registry

**Source of truth:** upstream design-system registry (vendored under `src/components/ui/`).

When using a Registry component or pattern (RadioGroup choice cards, Field,
Button, Checkbox, Eyebrow, AgentConsole, Stepper, …):

1. **Name the recipe**, not only the primitive  
   Example: “Registry RadioGroup **ChoiceCardsGrid**”  
   (`apps/registry/ui/src/ui/radio-group.stories.tsx`), not just “RadioGroup”.

2. **Copy chrome verbatim**  
   Tile / control class recipes from the Registry story or guideline.  
   Prefer the shared export when one exists (e.g. `choiceCardClassName` from
   `@/lib/choice-card-chrome`). Do **not** invent a local `*_CHOICE_CARD_CLASS`
   or re-skin border / radius / padding / selected colors.

3. **Token-adapt only inside vendors**  
   Files under `src/components/ui/**` may bridge tokens via
   `shared/theme.css` / `globals.css`. Call sites must not re-map accent /
   border / selected fills.

4. **Call sites may change structure only**  
   Allowed: grid/flex, `max-w-*`, which children (eyebrow / title / description).  
   Forbidden: overriding choice-card or control chrome classes.

5. **Visual change needed?**  
   Change Registry (or the shared exported recipe) once — never one-off patches
   on a single page.

### Choice cards (RadioGroup)

```tsx
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { choiceCardClassName } from "@/lib/choice-card-chrome";
import { Label } from "@/components/ui/label";
import { Eyebrow } from "@/components/ui/eyebrow";

<RadioGroup className="grid grid-cols-1 gap-3 xl:grid-cols-4" …>
  <Label htmlFor={id} className={cn(choiceCardClassName, choiceCardHoverClassName, "h-full")}>
    <RadioGroupItem focusRing="none" value={…} id={id} className={choiceCardRadioClassName} />
    <div className={choiceCardContentClassName}>
      <Eyebrow>…</Eyebrow>
      <span data-choice-title className="text-sm font-medium text-foreground">…</span>
    </div>
  </Label>
</RadioGroup>
```

Keep choice-card chrome in sync with Registry `radio-group.stories.tsx` via
`@/lib/choice-card-chrome.ts`.

### Stepper (Build process nav)

```tsx
import {
  Stepper, StepperList, StepperItem, StepperSeparator,
  StepperTrigger, StepperIndicator, StepperTitle,
} from "@/components/ui/stepper";

<Stepper value={stepId} onValueChange={…} activationMode="jump">
  <StepperList aria-label="…">
    {steps.map((s) => (
      <StepperItem key={s.id} value={s.id}>
        <StepperSeparator />
        <StepperTrigger>
          <StepperIndicator />
          <StepperTitle>{s.title}</StepperTitle>
        </StepperTrigger>
      </StepperItem>
    ))}
  </StepperList>
</Stepper>
```

Do not override Stepper chrome classes at the call site.
