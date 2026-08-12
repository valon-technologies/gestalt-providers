import { useEffect } from "react";

import { usePlatformBrand } from "@/hooks/use-platform-brand";

export function useDocumentTitle(title: string) {
  const brand = usePlatformBrand();

  useEffect(() => {
    const previous = document.title;
    document.title = title ? `${title} · ${brand.name}` : brand.name;
    return () => {
      document.title = previous;
    };
  }, [title, brand.name]);
}
