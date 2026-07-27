import { useAuthSessionQuery } from "@/lib/queries";

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const sessionQuery = useAuthSessionQuery();

  if (sessionQuery.isPending && !sessionQuery.data) return null;

  if (sessionQuery.isError && !sessionQuery.data) {
    return (
      <main className="mx-auto max-w-lg px-4 py-16 text-center">
        <h1 className="text-lg font-heading text-foreground">
          Could not verify your session
        </h1>
        <p className="mt-2 text-sm text-muted-foreground">
          The session check failed. Try again or sign in from the login page.
        </p>
        <button
          type="button"
          onClick={() => void sessionQuery.refetch()}
          disabled={sessionQuery.isFetching}
          className="mt-6 inline-flex items-center justify-center rounded-md border border-border px-4 py-2 text-sm font-medium text-foreground transition-colors duration-150 hover:border-input hover:bg-accent hover:text-accent-foreground disabled:cursor-not-allowed disabled:opacity-60"
        >
          {sessionQuery.isFetching ? "Retrying…" : "Try again"}
        </button>
      </main>
    );
  }

  return <>{children}</>;
}
