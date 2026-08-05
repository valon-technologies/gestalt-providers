import { GitBranch } from "lucide-react";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { readDevWorktreeName } from "@/lib/dev-worktree-name";

/** Local DEV strip naming the active git worktree (/prod-remote or /local-dev). */
export function DevWorktreeBanner() {
  const name = readDevWorktreeName();
  if (!name) {
    return null;
  }

  return (
    <Alert
      variant="warning"
      layout="banner"
      className="rounded-none border-b border-border"
      data-testid="dev-worktree-banner"
      aria-label={`Development worktree ${name}`}
    >
      <GitBranch aria-hidden />
      <AlertTitle>Worktree</AlertTitle>
      <AlertDescription>{name}</AlertDescription>
    </Alert>
  );
}
