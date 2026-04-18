import { Suspense } from "react";
import ManagedIdentitiesPageClient from "@/components/ManagedIdentitiesPageClient";

export default function ManagedIdentitiesPage() {
  return (
    <Suspense fallback={null}>
      <ManagedIdentitiesPageClient />
    </Suspense>
  );
}
