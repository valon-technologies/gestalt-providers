import type { ComponentType } from "react";
import {
  ChatGptIcon,
  ClaudeCodeIcon,
  ClaudeIcon,
  CodexIcon,
  CursorIcon,
  MoreHorizontalIcon,
} from "@/components/icons";
import type { AssistantHostIconKey } from "@/lib/assistantHosts";

/** Shared host marks for Setup cards and docs destination tabs. */
export const ASSISTANT_HOST_ICON: Record<
  AssistantHostIconKey,
  ComponentType<{ className?: string }>
> = {
  claude: ClaudeIcon,
  "claude-code": ClaudeCodeIcon,
  chatgpt: ChatGptIcon,
  cursor: CursorIcon,
  codex: CodexIcon,
  other: MoreHorizontalIcon,
};
