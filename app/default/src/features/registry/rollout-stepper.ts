import {
  durationSecondsBetween,
  formatDurationSeconds,
  isActiveRegistryRollout,
} from "@/features/registry/format";
import type { RegistryRollout } from "@/features/registry/types";

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

const ROLLOUT_TERMINAL_SUCCESS_LABEL = "Current";
const ROLLOUT_TERMINAL_FAILURE_LABEL = "Deploy failed";
const ROLLOUT_PHASE_ENROLLING_LABEL = "Updating fleet";
const ROLLOUT_PHASE_RESTARTING_LABEL = "Reloading apps";

export function rolloutTerminalLabel(state?: string): string {
  return state === "failed" ? ROLLOUT_TERMINAL_FAILURE_LABEL : ROLLOUT_TERMINAL_SUCCESS_LABEL;
}

export function rolloutPhaseProgressLabel(state?: string): string | null {
  if (state === "enrolling") return ROLLOUT_PHASE_ENROLLING_LABEL;
  if (state === "restarting") return ROLLOUT_PHASE_RESTARTING_LABEL;
  return null;
}

export function fleetRolloutBadgeLabel(app: {
  rollout?: RegistryRollout;
  desiredVersion?: string;
}): string {
  const rolloutState = app.rollout?.state;
  if (rolloutState && isActiveRegistryRollout(rolloutState)) {
    return "Rolling out";
  }
  if (rolloutState === "failed") {
    return ROLLOUT_TERMINAL_FAILURE_LABEL;
  }
  if (app.desiredVersion) {
    return ROLLOUT_TERMINAL_SUCCESS_LABEL;
  }
  return "Not deployed";
}

export function rolloutMatchesRow(
  rollout: RegistryRollout | undefined,
  rowVersion: string,
): boolean {
  return rollout?.version === rowVersion;
}

export function shouldShowRowRolloutStepper(
  rollout: RegistryRollout | undefined,
  rowVersion: string,
): boolean {
  if (!rollout || rollout.version !== rowVersion) return false;
  return isActiveRegistryRollout(rollout.state) || rollout.state === "failed";
}

export function rolloutActiveDurationLabel(
  rollout: RegistryRollout,
  now?: number | Date,
): string | null {
  if (!isActiveRegistryRollout(rollout.state)) return null;
  const seconds =
    durationSecondsBetween(rollout.createdAt, now ?? Date.now()) ?? null;
  return seconds !== null ? `for ${formatDurationSeconds(seconds)}` : null;
}

export function rolloutProgressSubline(
  rollout: RegistryRollout,
  now?: number | Date,
): string | null {
  const phase = rolloutPhaseProgressLabel(rollout.state);
  const duration = rolloutActiveDurationLabel(rollout, now);
  if (!phase) return duration;
  if (!duration) return phase;
  return `${phase} · ${duration}`;
}

export function buildRolloutStepperModel(
  rollout?: RegistryRollout,
): RolloutStepperModel {
  const terminalLabel = rolloutTerminalLabel(rollout?.state);

  if (!rollout) {
    return {
      phases: [
        {
          id: "enrolling",
          label: ROLLOUT_PHASE_ENROLLING_LABEL,
          state: "upcoming",
        },
        {
          id: "restarting",
          label: ROLLOUT_PHASE_RESTARTING_LABEL,
          state: "upcoming",
        },
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
        {
          id: "enrolling",
          label: ROLLOUT_PHASE_ENROLLING_LABEL,
          state: "current",
        },
        {
          id: "restarting",
          label: ROLLOUT_PHASE_RESTARTING_LABEL,
          state: "upcoming",
        },
        { id: "terminal", label: terminalLabel, state: "upcoming" },
      ],
    };
  }

  if (rollout.state === "restarting") {
    return {
      phases: [
        {
          id: "enrolling",
          label: ROLLOUT_PHASE_ENROLLING_LABEL,
          state: "completed",
        },
        {
          id: "restarting",
          label: ROLLOUT_PHASE_RESTARTING_LABEL,
          state: "current",
        },
        { id: "terminal", label: terminalLabel, state: "upcoming" },
      ],
    };
  }

  if (rollout.state === "complete") {
    return {
      phases: [
        {
          id: "enrolling",
          label: ROLLOUT_PHASE_ENROLLING_LABEL,
          state: "completed",
        },
        {
          id: "restarting",
          label: ROLLOUT_PHASE_RESTARTING_LABEL,
          state: "completed",
        },
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
        {
          id: "enrolling",
          label: ROLLOUT_PHASE_ENROLLING_LABEL,
          state: "completed",
        },
        {
          id: "restarting",
          label: ROLLOUT_PHASE_RESTARTING_LABEL,
          state: "completed",
        },
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
      {
        id: "enrolling",
        label: ROLLOUT_PHASE_ENROLLING_LABEL,
        state: "upcoming",
      },
      {
        id: "restarting",
        label: ROLLOUT_PHASE_RESTARTING_LABEL,
        state: "upcoming",
      },
      {
        id: "terminal",
        label: terminalLabel,
        state: "terminal",
        tone: "muted",
      },
    ],
  };
}

export type SelectedVersionRowAffordance = "success" | "error";

export function isRolloutHighlightRow(
  rollout: RegistryRollout | undefined,
  rowVersion: string,
): boolean {
  if (!rollout || rollout.version !== rowVersion) return false;
  return isActiveRegistryRollout(rollout.state) || rollout.state === "failed";
}

export function selectedVersionRowAffordance(
  rollout: RegistryRollout | undefined,
  rowVersion: string,
): SelectedVersionRowAffordance | null {
  if (!rollout || rollout.version !== rowVersion) return null;
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
