import { useParams } from "@tanstack/react-router";
import { AppAdminHistoryTable } from "@/features/registry/app-admin-history-table";
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
      <h1 className="text-2xl font-heading text-foreground">Revision history</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        Fleet version changes in reverse chronological order.
      </p>

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
