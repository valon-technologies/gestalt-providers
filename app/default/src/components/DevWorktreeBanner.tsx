import { GitBranch } from "lucide-react";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import { readDevWorktreeName } from "@/lib/dev-worktree-name";

export { readDevWorktreeName } from "@/lib/dev-worktree-name";
export { DEV_WORKTREE_NAME_ENV } from "@/lib/dev-worktree-name";

/** Development-only strip naming the active git worktree (prod-remote). */
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
