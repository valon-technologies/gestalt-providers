import type { LucideIcon } from "lucide-react";
import { Check, Circle, Dot, TriangleAlert } from "lucide-react";
import {
  TimelineSteps,
  TimelineStepsConnector,
  TimelineStepsHeader,
  TimelineStepsIcon,
  TimelineStepsItem,
  TimelineStepsTitle,
} from "@/components/ui/timeline-steps";
import { isActiveRegistryRollout } from "@/features/registry/format";
import {
  buildRolloutStepperModel,
  type RolloutStepperModel,
} from "@/features/registry/rollout-stepper";
import type { RegistryRollout } from "@/features/registry/types";
import { cn } from "@/lib/cn";

type PhaseItemStatus = "default" | "completed" | "current" | "upcoming" | "warning";

/** Registry CompactDots horizontal xs — dot rail, no inner glyphs, flex-1 steps. */
const MINI_TIMELINE_ROOT_CLASS = "w-full max-w-[17.5rem]";

function phaseItemStatus(
  phase: RolloutStepperModel["phases"][number],
): PhaseItemStatus {
  if (phase.state === "completed") return "completed";
  if (phase.state === "current") return "current";
  if (phase.state === "upcoming") return "upcoming";
  if (phase.state === "terminal" && phase.tone === "error") return "warning";
  if (phase.state === "terminal") return "completed";
  return "default";
}

function connectorLineState(
  phase: RolloutStepperModel["phases"][number],
): "completed" | "pending" {
  return phase.state === "completed" ? "completed" : "pending";
}

function phaseIconConfig(phase: RolloutStepperModel["phases"][number]): {
  state: "completed" | "active" | "pending" | "warning";
  icon: LucideIcon;
  pulse: boolean;
} {
  if (phase.state === "completed") {
    return { state: "completed", icon: Check, pulse: false };
  }
  if (phase.state === "current") {
    return { state: "active", icon: Circle, pulse: true };
  }
  if (phase.state === "terminal" && phase.tone === "error") {
    return { state: "warning", icon: TriangleAlert, pulse: false };
  }
  if (phase.state === "terminal") {
    return { state: "completed", icon: Check, pulse: false };
  }
  return { state: "pending", icon: Dot, pulse: false };
}

function RolloutPhaseIcon({
  phase,
  mini,
}: {
  phase: RolloutStepperModel["phases"][number];
  mini: boolean;
}) {
  if (mini) {
    return (
      <TimelineStepsIcon
        className={cn(phase.state === "current" && "motion-safe:animate-pulse")}
      />
    );
  }

  const { state, icon: Icon, pulse } = phaseIconConfig(phase);

  return (
    <TimelineStepsIcon
      state={state}
      glyph="none"
      className={cn(pulse && "motion-safe:animate-pulse")}
    >
      <Icon className="size-3.5" aria-hidden="true" />
    </TimelineStepsIcon>
  );
}

export function RolloutPhaseStepper({
  rollout,
  size = "default",
  className,
}: {
  rollout: RegistryRollout;
  size?: "default" | "mini";
  className?: string;
}) {
  const model = buildRolloutStepperModel(rollout);
  const isActiveRollout = isActiveRegistryRollout(rollout.state);
  const isMini = size === "mini";
  const timelineSize = isMini ? "xs" : "default";

  return (
    <TimelineSteps
      orientation="horizontal"
      size={timelineSize}
      glyph="none"
      className={cn(isMini ? MINI_TIMELINE_ROOT_CLASS : "w-full max-w-md", className)}
      data-testid="rollout-phase-stepper"
      data-rollout-active={isActiveRollout ? "true" : undefined}
      aria-label="Rollout progress"
    >
      {model.phases.map((phase, index) => {
        const isLast = index === model.phases.length - 1;

        return (
          <TimelineStepsItem
            key={phase.id}
            status={phaseItemStatus(phase)}
            index={index}
            data-testid={`rollout-phase-node-${phase.id}`}
          >
            <TimelineStepsHeader>
              <RolloutPhaseIcon phase={phase} mini={isMini} />
              <TimelineStepsTitle
                className={cn(
                  isMini && "font-normal text-muted-foreground",
                )}
              >
                {phase.label}
              </TimelineStepsTitle>
            </TimelineStepsHeader>
            {!isLast ? (
              <TimelineStepsConnector
                orientation="horizontal"
                lineState={connectorLineState(phase)}
              />
            ) : null}
          </TimelineStepsItem>
        );
      })}
    </TimelineSteps>
  );
}
