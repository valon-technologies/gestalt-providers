import { useAuthSessionQuery } from "@/lib/queries";

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const { isPending, isError } = useAuthSessionQuery();

  if (isPending || isError) return null;

  return <>{children}</>;
}
