import { GitBranch } from "lucide-react";
import {
  Banner,
  BannerDescription,
  BannerIcon,
  BannerTitle,
} from "@/components/ui/banner";
import { readDevWorktreeName } from "@/lib/dev-worktree-name";

/** Local DEV strip naming the active git worktree (/prod-remote or /local-dev).
 *  Registry Banner (shell chrome), not Alert. Title and Description stay
 *  siblings so the strip's gap is the space between "Worktree" and the name.
 *  Sticky placement stays on `__root.tsx`. */
export function DevWorktreeBanner() {
  const name = readDevWorktreeName();
  if (!name) {
    return null;
  }

  return (
    <Banner
      variant="warning"
      data-testid="dev-worktree-banner"
      aria-label={`Development worktree ${name}`}
    >
      <BannerIcon>
        <GitBranch />
      </BannerIcon>
      <BannerTitle>Worktree</BannerTitle>
      <BannerDescription>{name}</BannerDescription>
    </Banner>
  );
}
