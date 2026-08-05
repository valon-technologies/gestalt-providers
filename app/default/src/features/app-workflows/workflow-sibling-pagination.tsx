import { Link } from "@tanstack/react-router";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";
import { siblingNavigation } from "@/features/app-workflows/workflow-sibling-navigation";

type SiblingLink =
  | {
      to: "/apps/$app/admin/workflows/runs/$runId";
      params: { app: string; runId: string };
    }
  | {
      to: "/apps/$app/admin/workflows/definitions/$definitionId";
      params: { app: string; definitionId: string };
    };

export function WorkflowSiblingPagination({
  itemLabel,
  ids,
  currentId,
  linkForId,
}: {
  itemLabel: string;
  ids: readonly string[];
  currentId: string;
  linkForId: (id: string) => SiblingLink;
}) {
  const { index, total, previousId, nextId } = siblingNavigation(
    ids,
    currentId,
  );
  const previousLink = previousId ? linkForId(previousId) : null;
  const nextLink = nextId ? linkForId(nextId) : null;

  return (
    <Pagination className="mx-0 mb-2 w-full justify-start">
      <PaginationContent>
        <PaginationItem>
          {previousLink ? (
            <PaginationPrevious
              asChild
              aria-label={`Go to previous ${itemLabel.toLowerCase()}`}
            >
              <Link {...previousLink}>
                <ChevronLeft />
                <span>Previous</span>
              </Link>
            </PaginationPrevious>
          ) : (
            <Button
              type="button"
              variant="ghost"
              size="default"
              className="gap-1 px-2.5"
              disabled
              aria-label={`Go to previous ${itemLabel.toLowerCase()}`}
            >
              <ChevronLeft />
              <span>Previous</span>
            </Button>
          )}
        </PaginationItem>
        {total > 0 && index >= 0 ? (
          <PaginationItem>
            <span className="text-muted-foreground px-2 text-sm whitespace-nowrap">
              {itemLabel} {index + 1} of {total}
            </span>
          </PaginationItem>
        ) : null}
        <PaginationItem>
          {nextLink ? (
            <PaginationNext
              asChild
              aria-label={`Go to next ${itemLabel.toLowerCase()}`}
            >
              <Link {...nextLink}>
                <span>Next</span>
                <ChevronRight />
              </Link>
            </PaginationNext>
          ) : (
            <Button
              type="button"
              variant="ghost"
              size="default"
              className="gap-1 px-2.5"
              disabled
              aria-label={`Go to next ${itemLabel.toLowerCase()}`}
            >
              <span>Next</span>
              <ChevronRight />
            </Button>
          )}
        </PaginationItem>
      </PaginationContent>
    </Pagination>
  );
}
