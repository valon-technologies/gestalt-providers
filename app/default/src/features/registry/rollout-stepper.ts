import type { RegistryRollout } from "@/features/registry/types";
import { isActiveRegistryRollout } from "@/features/registry/format";

export type RolloutStepperPhase = "enrolling" | "restarting" | "terminal";

export type RolloutStepperTerminalTone = "success" | "error" | "muted";

export type RolloutStepperModel = {
  phases: Array<{
    id: RolloutStepperPhase;
    label: string;
    state: "completed" | "current" | "upcoming" | "terminal";
    tone?: RolloutStepperTerminalTone;
  }>;
};

export function buildRolloutStepperModel(
  rollout?: RegistryRollout,
): RolloutStepperModel {
  const terminalLabel = rollout?.state === "failed" ? "Failed" : "Available";

  if (!rollout) {
    return {
      phases: [
        { id: "enrolling", label: "Enrolling", state: "upcoming" },
        { id: "restarting", label: "Restarting", state: "upcoming" },
        {
          id: "terminal",
          label: terminalLabel,
          state: "terminal",
          tone: "muted",
        },
      ],
    };
  }

  if (rollout.state === "enrolling") {
    return {
      phases: [
        { id: "enrolling", label: "Enrolling", state: "current" },
        { id: "restarting", label: "Restarting", state: "upcoming" },
        { id: "terminal", label: terminalLabel, state: "upcoming" },
      ],
    };
  }

  if (rollout.state === "restarting") {
    return {
      phases: [
        { id: "enrolling", label: "Enrolling", state: "completed" },
        { id: "restarting", label: "Restarting", state: "current" },
        { id: "terminal", label: terminalLabel, state: "upcoming" },
      ],
    };
  }

  if (rollout.state === "complete") {
    return {
      phases: [
        { id: "enrolling", label: "Enrolling", state: "completed" },
        { id: "restarting", label: "Restarting", state: "completed" },
        {
          id: "terminal",
          label: terminalLabel,
          state: "terminal",
          tone: "success",
        },
      ],
    };
  }

  if (rollout.state === "failed") {
    return {
      phases: [
        { id: "enrolling", label: "Enrolling", state: "completed" },
        { id: "restarting", label: "Restarting", state: "completed" },
        {
          id: "terminal",
          label: terminalLabel,
          state: "terminal",
          tone: "error",
        },
      ],
    };
  }

  return {
    phases: [
      { id: "enrolling", label: "Enrolling", state: "upcoming" },
      { id: "restarting", label: "Restarting", state: "upcoming" },
      {
        id: "terminal",
        label: terminalLabel,
        state: "terminal",
        tone: "muted",
      },
    ],
  };
}

export type SelectedVersionRowAffordance = "pulsing" | "success" | "error";

export function selectedVersionRowAffordance(
  rollout: RegistryRollout | undefined,
  rowVersion: string,
): SelectedVersionRowAffordance | null {
  if (!rollout || rollout.version !== rowVersion) return null;
  if (isActiveRegistryRollout(rollout.state)) return "pulsing";
  if (rollout.state === "complete") return "success";
  if (rollout.state === "failed") return "error";
  return null;
}

export function isRolloutDeployingAction(
  rollout: RegistryRollout | undefined,
  rowVersion: string,
): boolean {
  return (
    rollout?.version === rowVersion &&
    isActiveRegistryRollout(rollout.state)
  );
}
