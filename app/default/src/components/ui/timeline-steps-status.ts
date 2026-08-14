/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import type { StepRailSemanticState, StepRailVisualState } from "@/components/ui/step-rail";

/**
 * TimelineSteps item status — progress + semantic outcome states.
 * Failure outcomes use `error` (status intent). `danger` is a deprecated alias.
 */
export type TimelineItemStatus =
  | "default"
  | "completed"
  | "current"
  | "upcoming"
  | "success"
  | "warning"
  | "error"
  /** @deprecated Use `error` — kept so early TimelineSteps adopters keep painting. */
  | "danger"
  | null;

/** Map timeline item status → shared rail paint state. */
export function timelineItemStatusToRailState(
  status?: TimelineItemStatus,
): StepRailVisualState | StepRailSemanticState {
  switch (status) {
    case "completed":
      return "completed";
    case "current":
      return "active";
    case "success":
      return "success";
    case "warning":
      return "warning";
    case "error":
    case "danger":
      return "error";
    case "upcoming":
    default:
      return "pending";
  }
}

/** Connector fill: only completed progress and success outcomes advance the rail. */
export function timelineConnectorLineState(status?: TimelineItemStatus): "completed" | "pending" {
  return status === "completed" || status === "success" ? "completed" : "pending";
}
