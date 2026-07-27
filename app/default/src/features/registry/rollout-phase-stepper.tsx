import { Fragment } from "react";
import {
  buildRolloutStepperModel,
  type RolloutStepperModel,
} from "@/features/registry/rollout-stepper";
import type { RegistryRollout } from "@/features/registry/types";
import { cn } from "@/lib/cn";

function StepNode({
  phase,
}: {
  phase: RolloutStepperModel["phases"][number];
}) {
  return (
    <span
      className={cn(
        "size-3 shrink-0 rounded-full border-2",
        phase.state === "current" && "border-primary bg-primary",
        phase.state === "completed" && "border-warning bg-warning",
        phase.state === "upcoming" && "border-warning bg-transparent",
        phase.state === "terminal" &&
          phase.tone === "success" &&
          "border-success bg-success",
        phase.state === "terminal" &&
          phase.tone === "error" &&
          "border-destructive bg-destructive",
        phase.state === "terminal" &&
          phase.tone === "muted" &&
          "border-muted-foreground/40 bg-transparent",
      )}
      data-testid={`rollout-phase-node-${phase.id}`}
    />
  );
}

function StepConnector({ active }: { active: boolean }) {
  return (
    <span
      className={cn(
        "mt-1.5 h-0.5 min-w-8 flex-1 self-start",
        active ? "bg-warning" : "bg-muted-foreground/25",
      )}
      aria-hidden="true"
    />
  );
}

export function RolloutPhaseStepper({
  rollout,
}: {
  rollout?: RegistryRollout;
}) {
  const model = buildRolloutStepperModel(rollout);

  return (
    <div
      className="flex w-full max-w-md items-start"
      data-testid="rollout-phase-stepper"
      aria-label="Rollout progress"
    >
      {model.phases.map((phase, index) => {
        const previousPhase = index > 0 ? model.phases[index - 1] : null;
        const connectorActive = previousPhase?.state === "completed";

        return (
          <Fragment key={phase.id}>
            {index > 0 ? <StepConnector active={connectorActive} /> : null}
            <div className="flex shrink-0 flex-col items-center gap-1.5">
              <StepNode phase={phase} />
              <span className="whitespace-nowrap text-xs text-muted-foreground">
                {phase.label}
              </span>
            </div>
          </Fragment>
        );
      })}
    </div>
  );
}
