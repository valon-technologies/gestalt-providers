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
        "h-0.5 min-w-8 flex-1",
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
  const [enrolling, restarting, terminal] = model.phases;

  return (
    <div
      className="flex items-center gap-2"
      data-testid="rollout-phase-stepper"
      aria-label="Rollout progress"
    >
      <div className="flex min-w-0 flex-1 flex-col items-center gap-1">
        <div className="flex w-full items-center gap-2">
          <StepNode phase={enrolling} />
          <StepConnector active={enrolling.state === "completed"} />
          <StepNode phase={restarting} />
          <StepConnector
            active={
              restarting.state === "completed" || terminal.state === "terminal"
            }
          />
          <StepNode phase={terminal} />
        </div>
        <div className="grid w-full grid-cols-3 gap-2 text-center text-xs text-muted-foreground">
          <span>{enrolling.label}</span>
          <span>{restarting.label}</span>
          <span>{terminal.label}</span>
        </div>
      </div>
    </div>
  );
}
