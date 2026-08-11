import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";
import { AppProviders } from "@/providers/app-providers";
import { router } from "./router";
import "./globals.css";
// Reading contract: load after globals so Tailwind establishes `@layer`
// (theme/base/components/utilities) first. Keep this a real module import —
// a CSS `@import` inside globals can vanish from the Vite graph when the
// file is briefly missing (stash/rebase) and leave docs with zero block
// margins while the app otherwise looks fine.
import "./styles/typeset-reading.css";

const rootElement = document.getElementById("root");
if (!rootElement) {
  throw new Error("Root element #root not found");
}

createRoot(rootElement).render(
  <StrictMode>
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>
  </StrictMode>,
);
