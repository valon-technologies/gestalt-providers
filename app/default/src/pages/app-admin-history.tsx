import { useParams } from "@tanstack/react-router";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderDescription,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { AppAdminHistoryTable } from "@/features/registry/app-admin-history-table";
import { useAppAdminRegistryHistoryQuery } from "@/lib/queries";

export default function AppAdminHistoryPage() {
  const { app: appName } = useParams({ from: "/apps/$app/admin/history" });
  const historyQuery = useAppAdminRegistryHistoryQuery(appName, true);
  const historyRevisions =
    historyQuery.data?.pages.flatMap((page) => page.revisions) ?? [];
  const historyError = historyQuery.error
    ? historyQuery.error instanceof Error
      ? historyQuery.error.message
      : "Failed to load revision history"
    : null;

  return (
    <section className="space-y-4 rounded-2xl border border-border bg-card p-6 text-card-foreground">
      <SectionHeader>
        <SectionHeaderContent>
          <SectionHeaderTitle>Revision history</SectionHeaderTitle>
          <SectionHeaderDescription>
            Accepted fleet version changes in reverse chronological order.
          </SectionHeaderDescription>
        </SectionHeaderContent>
      </SectionHeader>

      <AppAdminHistoryTable
        revisions={historyRevisions}
        loading={historyQuery.isPending}
        loadingMore={historyQuery.isFetchingNextPage}
        error={historyError}
        hasMore={historyQuery.hasNextPage}
        onLoadMore={() => void historyQuery.fetchNextPage()}
      />
    </section>
  );
}
