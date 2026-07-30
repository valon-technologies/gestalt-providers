import { useParams } from "@tanstack/react-router";
import { AppAdminHistoryTable } from "@/features/registry/app-admin-history-table";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { useAppAdminRegistryHistoryQuery } from "@/lib/queries";

export default function AppAdminHistoryPage() {
  const { app } = useParams({ from: "/apps/$app/admin/history" });
  const historyQuery = useAppAdminRegistryHistoryQuery(app, true);
  const historyRevisions =
    historyQuery.data?.pages.flatMap((page) => page.revisions) ?? [];
  const historyError = historyQuery.error
    ? historyQuery.error instanceof Error
      ? historyQuery.error.message
      : "Failed to load revision history"
    : null;

  return (
    <section aria-label="Revision history">
      <PageHeader>
        <PageHeaderContent>
          <PageHeaderTitle>Revision history</PageHeaderTitle>
          <PageHeaderDescription>
            Accepted fleet version changes in reverse chronological order.
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <div className="mt-6">
        <AppAdminHistoryTable
          revisions={historyRevisions}
          loading={historyQuery.isPending}
          loadingMore={historyQuery.isFetchingNextPage}
          error={historyError}
          hasMore={historyQuery.hasNextPage}
          onLoadMore={() => void historyQuery.fetchNextPage()}
        />
      </div>
    </section>
  );
}
