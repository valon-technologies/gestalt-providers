import type {
  AgentInteraction,
  AgentMessage,
  AgentMessagePart,
  AgentTurn,
  AgentTurnDisplay,
  AgentTurnEvent,
} from "./api";

export type TranscriptKind =
  | "user"
  | "assistant"
  | "system"
  | "tool"
  | "interaction"
  | "reasoning"
  | "error"
  | "event";

export interface TranscriptItem {
  id: string;
  kind: TranscriptKind;
  title: string;
  text: string;
  streaming?: boolean;
  format?: string;
  language?: string;
  event?: AgentTurnEvent;
  toolKey?: string;
  optimistic?: boolean;
}

export interface TranscriptState {
  items: TranscriptItem[];
  rawPublicEvents: AgentTurnEvent[];
  lastSeqByTurnId: Record<string, number>;
}

const KNOWN_TURN_EVENT_TYPES = new Set([
  "agent.message.delta",
  "assistant.message",
  "assistant.delta",
  "assistant.completed",
  "turn.started",
  "turn.completed",
  "turn.failed",
  "turn.canceled",
  "tool.started",
  "tool.completed",
  "tool.failed",
  "interaction.requested",
  "interaction.resolved",
]);

export function createTranscriptState(): TranscriptState {
  return {
    items: [],
    rawPublicEvents: [],
    lastSeqByTurnId: {},
  };
}

export function cloneTranscriptState(state: TranscriptState): TranscriptState {
  return {
    items: [...state.items],
    rawPublicEvents: [...state.rawPublicEvents],
    lastSeqByTurnId: { ...state.lastSeqByTurnId },
  };
}

export function isKnownTurnEventType(type: string): boolean {
  return KNOWN_TURN_EVENT_TYPES.has(type);
}

export function isTurnLive(status?: string): boolean {
  return (
    status === "pending" ||
    status === "running" ||
    status === "waiting_for_input"
  );
}

export function isTurnTerminal(status?: string): boolean {
  return status === "succeeded" || status === "failed" || status === "canceled";
}

export function appendTurnMessages(
  state: TranscriptState,
  turn: AgentTurn,
): TranscriptState {
  const next = cloneTranscriptState(state);
  for (const [index, message] of (turn.messages ?? []).entries()) {
    const text = messageText(message);
    if (!text) continue;
    const kind = messageKind(message.role);
    const optimisticIdx = next.items.findIndex(
      (item) => item.optimistic && item.kind === kind && item.text === text,
    );
    if (optimisticIdx >= 0) {
      next.items.splice(optimisticIdx, 1);
    }
    next.items.push({
      id: `${turn.id}:message:${index}`,
      kind,
      title: messageTitle(message.role),
      text,
    });
  }
  return next;
}

export function appendOptimisticUserMessage(
  state: TranscriptState,
  text: string,
): TranscriptState {
  const trimmed = text.trim();
  if (!trimmed) return state;
  const next = cloneTranscriptState(state);
  next.items.push({
    id: `optimistic:${Date.now()}:${next.items.length}`,
    kind: "user",
    title: "You",
    text: trimmed,
    optimistic: true,
  });
  return next;
}

export function finishTurnSnapshot(
  state: TranscriptState,
  turn: AgentTurn,
): TranscriptState {
  const next = cloneTranscriptState(state);
  if (turn.status === "failed" && turn.statusMessage) {
    pushItem(next, turn.id, "error", "Turn failed", turn.statusMessage);
  } else if (turn.status === "canceled" && turn.statusMessage) {
    pushItem(next, turn.id, "system", "Turn canceled", turn.statusMessage);
  } else if (turn.status === "succeeded" && turn.outputText) {
    const hasAssistant = next.items.some(
      (item) => item.kind === "assistant" && item.text === turn.outputText,
    );
    if (!hasAssistant) {
      pushItem(next, turn.id, "assistant", "Assistant", turn.outputText);
    }
  }

  if (turn.structuredOutput && isTurnTerminal(turn.status)) {
    pushItem(
      next,
      turn.id,
      "system",
      "Structured output",
      prettyJSON(turn.structuredOutput),
    );
  }
  return next;
}

export function appendInteraction(
  state: TranscriptState,
  interaction: AgentInteraction,
): TranscriptState {
  const next = cloneTranscriptState(state);
  if (next.items.some((item) => item.id === `interaction:${interaction.id}`)) {
    return next;
  }
  const title = interaction.title || interaction.type || "Interaction";
  const prompt = interaction.prompt || title;
  next.items.push({
    id: `interaction:${interaction.id}`,
    kind: "interaction",
    title,
    text: prompt,
  });
  return next;
}

export function applyTurnEvent(
  state: TranscriptState,
  event: AgentTurnEvent,
): TranscriptState {
  const next = cloneTranscriptState(state);
  if (typeof event.seq === "number") {
    next.lastSeqByTurnId[event.turnId] = Math.max(
      next.lastSeqByTurnId[event.turnId] ?? 0,
      event.seq,
    );
  }

  if (event.visibility !== "private") {
    next.rawPublicEvents.push(event);
  }

  if (applyDisplayEvent(next, event)) {
    return next;
  }

  switch (event.type) {
    case "agent.message.delta":
    case "assistant.delta": {
      const text = stringAnyField(event.data, ["text", "delta", "content"]);
      if (text) pushAssistantDelta(next, event, text);
      return next;
    }
    case "assistant.message": {
      const text = stringAnyField(event.data, [
        "text",
        "message",
        "content",
      ]);
      if (text) completeAssistant(next, event, text);
      return next;
    }
    case "assistant.completed": {
      const text = stringAnyField(event.data, ["text", "content"]);
      if (text) completeAssistant(next, event, text);
      else finishAssistantStream(next);
      return next;
    }
    case "turn.started": {
      const status = stringAnyField(event.data, ["status", "state"]);
      pushItem(next, event.id, "system", "Turn started", status || "started");
      return next;
    }
    case "turn.completed":
      finishAssistantStream(next);
      pushItem(next, event.id, "system", "Turn completed", "completed");
      return next;
    case "turn.failed": {
      const error = stringAnyField(event.data, ["error", "message"]);
      pushItem(next, event.id, "error", "Turn failed", error || "failed");
      return next;
    }
    case "turn.canceled": {
      const reason = stringAnyField(event.data, ["reason", "message"]);
      pushItem(next, event.id, "system", "Turn canceled", reason || "canceled");
      return next;
    }
    case "tool.started":
    case "tool.completed":
    case "tool.failed":
      upsertToolItem(next, event, toolTitle(event), toolSummary(event));
      return next;
    case "interaction.requested": {
      const id =
        stringAnyField(event.data, ["interaction_id", "interactionId"]) ||
        "interaction";
      pushItem(next, event.id, "interaction", "Interaction requested", id);
      return next;
    }
    case "interaction.resolved": {
      const id =
        stringAnyField(event.data, ["interaction_id", "interactionId"]) ||
        "interaction";
      pushItem(next, event.id, "interaction", "Interaction resolved", id);
      return next;
    }
    default:
      if (event.visibility === "private") {
        return next;
      }
      pushItem(next, event.id, "event", event.type || "Event", publicEventText(event), event);
      return next;
  }
}

function applyDisplayEvent(state: TranscriptState, event: AgentTurnEvent) {
  const display = turnEventDisplay(event);
  if (!display) return false;

  switch (display.kind?.trim()) {
    case "text": {
      const text = displayText(display);
      if (!text) return false;
      if (display.phase === "delta") {
        pushAssistantDelta(state, event, text, display);
      } else if (display.phase === "completed") {
        completeAssistant(state, event, text, display);
      } else {
        pushItem(state, event.id, "assistant", "Assistant", text, event, display);
      }
      return true;
    }
    case "reasoning": {
      const text = displayText(display);
      if (!text) return false;
      pushItem(state, event.id, "reasoning", "Reasoning", text, event, display);
      return true;
    }
    case "tool":
      upsertToolItem(state, event, displayToolTitle(display), displayToolText(display));
      return true;
    case "interaction": {
      const title = display.label || "Interaction";
      const reference = display.ref || display.text || "interaction";
      pushItem(state, event.id, "interaction", title, reference, event);
      return true;
    }
    case "status": {
      const text = displayText(display);
      if (!text && !display.phase) return false;
      pushItem(
        state,
        event.id,
        "system",
        display.label || "Status",
        text || display.phase || "status",
        event,
      );
      return true;
    }
    case "error": {
      const text = displayText(display) || valueText(display.error);
      if (!text) return false;
      pushItem(state, event.id, "error", display.label || "Error", text, event);
      return true;
    }
    default:
      return false;
  }
}

function turnEventDisplay(event: AgentTurnEvent): AgentTurnDisplay | undefined {
  if (!event.display?.kind) return undefined;
  if (event.visibility === "private" && !isKnownTurnEventType(event.type)) {
    return undefined;
  }
  return event.display;
}

function pushItem(
  state: TranscriptState,
  id: string,
  kind: TranscriptKind,
  title: string,
  text: string,
  event?: AgentTurnEvent,
  display?: AgentTurnDisplay,
) {
  state.items.push({
    id: `${id}:${state.items.length}`,
    kind,
    title,
    text,
    event,
    format: display?.format,
    language: display?.language,
  });
}

function upsertToolItem(
  state: TranscriptState,
  event: AgentTurnEvent,
  title: string,
  text: string,
) {
  const key = toolEventKey(event);
  if (key) {
    const existing = state.items.find(
      (item) => item.kind === "tool" && item.toolKey === key,
    );
    if (existing) {
      existing.title = title || existing.title;
      existing.text = text || existing.text;
      existing.event = event;
      existing.format = event.display?.format || existing.format;
      existing.language = event.display?.language || existing.language;
      return;
    }
  }
  state.items.push({
    id: `${event.id}:${state.items.length}`,
    kind: "tool",
    title,
    text,
    event,
    toolKey: key,
    format: event.display?.format,
    language: event.display?.language,
  });
}

function toolEventKey(event: AgentTurnEvent): string | undefined {
  const ref = event.display?.ref?.trim();
  if (ref) return ref;
  const data = event.data;
  if (!data || typeof data !== "object") return undefined;
  for (const field of ["tool_call_id", "toolCallId", "call_id", "callId", "id"]) {
    const value = (data as Record<string, unknown>)[field];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return undefined;
}

function pushAssistantDelta(
  state: TranscriptState,
  event: AgentTurnEvent,
  text: string,
  display?: AgentTurnDisplay,
) {
  const last = state.items[state.items.length - 1];
  if (last?.kind === "assistant" && last.streaming) {
    last.text += text;
    last.format = display?.format || last.format;
    last.language = display?.language || last.language;
    return;
  }
  state.items.push({
    id: `${event.id}:${state.items.length}`,
    kind: "assistant",
    title: "Assistant",
    text,
    streaming: true,
    event,
    format: display?.format,
    language: display?.language,
  });
}

function completeAssistant(
  state: TranscriptState,
  event: AgentTurnEvent,
  text: string,
  display?: AgentTurnDisplay,
) {
  const last = state.items[state.items.length - 1];
  if (last?.kind === "assistant" && last.streaming) {
    last.text = text || last.text;
    last.streaming = false;
    last.format = display?.format || last.format;
    last.language = display?.language || last.language;
    return;
  }
  pushItem(state, event.id, "assistant", "Assistant", text, event, display);
}

function finishAssistantStream(state: TranscriptState) {
  const last = state.items[state.items.length - 1];
  if (last?.kind === "assistant") {
    last.streaming = false;
  }
}

function messageText(message: AgentMessage): string {
  if (message.text?.trim()) return message.text;
  return (message.parts ?? []).map(messagePartText).filter(Boolean).join("");
}

function messagePartText(part: AgentMessagePart): string {
  if (part.text?.trim()) return part.text;
  if (part.json) return compactJSON(part.json);
  if (part.toolCall) return `tool call ${compactJSON(part.toolCall)}`;
  if (part.toolResult) return `tool result ${compactJSON(part.toolResult)}`;
  if (part.imageRef) return `image ${compactJSON(part.imageRef)}`;
  return "";
}

function messageKind(role: string): TranscriptKind {
  switch (role) {
    case "user":
      return "user";
    case "assistant":
      return "assistant";
    case "tool":
      return "tool";
    default:
      return "system";
  }
}

function messageTitle(role: string): string {
  switch (role) {
    case "user":
      return "You";
    case "assistant":
      return "Assistant";
    case "tool":
      return "Tool";
    case "system":
      return "System";
    default:
      return role || "Message";
  }
}

function displayText(display: AgentTurnDisplay): string {
  return typeof display.text === "string" ? display.text : "";
}

function displayToolTitle(display: AgentTurnDisplay): string {
  return display.label || display.ref || "Tool";
}

function displayToolText(display: AgentTurnDisplay): string {
  const parts = [
    display.action,
    display.phase,
    display.text,
    valueText(display.error),
    valueText(display.output),
    valueText(display.input),
  ].filter(Boolean);
  return parts.join(" ") || "tool activity";
}

function toolTitle(event: AgentTurnEvent): string {
  return stringAnyField(event.data, [
    "tool_name",
    "toolName",
    "name",
    "operation",
    "tool_id",
    "toolId",
  ]) || "Tool";
}

function toolSummary(event: AgentTurnEvent): string {
  const phase = event.type.split(".")[1] || "activity";
  const status =
    stringAnyField(event.data, ["status", "state"]) ||
    numberAnyField(event.data, ["status", "statusCode"]);
  const error = stringAnyField(event.data, ["error", "message"]);
  const detail =
    error ||
    valueText(valueAnyField(event.data, ["output", "result", "body"])) ||
    valueText(valueAnyField(event.data, ["arguments", "input", "request"]));
  return [phase, status ? `(${status})` : "", detail].filter(Boolean).join(" ");
}

function publicEventText(event: AgentTurnEvent): string {
  return event.data ? prettyJSON(event.data) : event.type;
}

function stringAnyField(
  data: Record<string, unknown> | undefined,
  keys: string[],
): string {
  if (!data) return "";
  for (const key of keys) {
    const value = data[key];
    if (typeof value === "string" && value) return value;
  }
  return "";
}

function numberAnyField(
  data: Record<string, unknown> | undefined,
  keys: string[],
): string {
  if (!data) return "";
  for (const key of keys) {
    const value = data[key];
    if (typeof value === "number") return String(value);
  }
  return "";
}

function valueAnyField(
  data: Record<string, unknown> | undefined,
  keys: string[],
): unknown {
  if (!data) return undefined;
  for (const key of keys) {
    if (key in data) return data[key];
  }
  return undefined;
}

function valueText(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === undefined || value === null) return "";
  return compactJSON(value);
}

function compactJSON(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function prettyJSON(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}
