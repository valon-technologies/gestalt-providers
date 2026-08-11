import { Link as RouterLink } from "@tanstack/react-router";
import {
  StepPager,
  StepPagerNext,
  StepPagerPrevious,
  StepPagerStartSpacer,
} from "@/components/ui/step-pager";
import { getDocsJourneyEdges, type DocsNavItem } from "./docs-data";

export function DocsJourneyFooter({ item }: { item: DocsNavItem }) {
  const { previous, next } = getDocsJourneyEdges(item);
  if (!previous && !next) return null;

  return (
    <StepPager
      aria-label="Docs journey navigation"
      className="mt-10"
      data-testid="docs-journey-footer"
    >
      {previous ? (
        <StepPagerPrevious
          asChild
          title={previous.label}
          data-testid="docs-journey-previous"
        >
          <RouterLink to={previous.href} />
        </StepPagerPrevious>
      ) : (
        <StepPagerStartSpacer />
      )}
      {next ? (
        <StepPagerNext
          asChild
          title={next.label}
          data-testid="docs-journey-next"
        >
          <RouterLink to={next.href} />
        </StepPagerNext>
      ) : null}
    </StepPager>
  );
}
