"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import Nav from "@/components/Nav";
import { docsNavItems, getActiveDocsNavItem } from "./docs-data";

export default function DocsShell({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const activeItem = getActiveDocsNavItem(pathname);

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="mx-auto max-w-[1400px] px-6 py-16">
        <div className="grid gap-10 xl:grid-cols-[220px_minmax(0,1fr)_240px]">
          <aside className="hidden xl:block">
            <div className="sticky top-24">
              <nav className="space-y-0.5">
                {docsNavItems.map((item) => {
                  const isActive = item.id === activeItem.id;
                  return (
                    <Link
                      key={item.id}
                      href={item.href}
                      className={`block rounded-md px-3 py-2 text-sm transition-colors duration-150 ${
                        isActive
                          ? "bg-alpha-5 font-medium text-primary"
                          : "text-muted hover:text-primary"
                      }`}
                    >
                      {item.label}
                    </Link>
                  );
                })}
              </nav>
            </div>
          </aside>

          <article className="min-w-0">{children}</article>

          <aside className="hidden xl:block">
            <div className="sticky top-24 space-y-6">
              {activeItem.subsections.length > 0 && (
                <div>
                  <p className="text-xs font-medium uppercase tracking-[0.16em] text-faint">
                    On This Page
                  </p>
                  <nav className="mt-3 space-y-0.5">
                    {activeItem.subsections.map((subsection) => (
                      <a
                        key={subsection.id}
                        href={`#${subsection.id}`}
                        className="block border-l-2 border-transparent py-1.5 pl-3 text-sm text-muted transition-colors duration-150 hover:border-base-300 hover:text-primary dark:hover:border-base-600"
                      >
                        {subsection.label}
                      </a>
                    ))}
                  </nav>
                </div>
              )}
            </div>
          </aside>
        </div>
      </main>
    </div>
  );
}
