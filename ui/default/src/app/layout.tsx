import type { Metadata } from "next";
import localFont from "next/font/local";
import "./globals.css";
// Theme slot: resolves to the empty stub unless GESTALT_THEME_FILE points it
// at a live tenant theme during development. Imported after globals.css so
// theme declarations win equal-specificity ties, like the serve-time
// /theme.css link below.
import "@theme.css";

// Bundled faces are OFL (licenses in public/fonts/OFL-*.txt) — the only kind
// that may live in this public repo; commercially licensed fonts arrive via
// the tenant theme's @font-face (see THEMING.md). Same faces as gestalt/docs:
// Newsreader pinned at optical size 72 — the serif only renders display text,
// and the 72 cut carries the sharp serifs and contrast the role calls for.
const newsreader = localFont({
  src: [
    { path: "../../public/fonts/newsreader-opsz72-latin-400-normal.woff2", weight: "400", style: "normal" },
    { path: "../../public/fonts/newsreader-opsz72-latin-400-italic.woff2", weight: "400", style: "italic" },
  ],
  // The *-default variables are the next/font side of the font seam: they are
  // set via a hashed class on <body> (specificity 0,1,0), so the consumed
  // --font-* tokens are re-declared from them at zero specificity in
  // globals.css, where a tenant `body { --font-* }` override can win.
  variable: "--font-display-default",
});

const instrumentSans = localFont({
  src: [
    { path: "../../public/fonts/instrument-sans-latin-wght-normal.woff2", weight: "400 700", style: "normal" },
    { path: "../../public/fonts/instrument-sans-latin-wght-italic.woff2", weight: "400 700", style: "italic" },
  ],
  variable: "--font-body-default",
});

const geistMono = localFont({
  src: [
    { path: "../../public/fonts/GeistMono_Regular.woff2", weight: "400", style: "normal" },
  ],
  variable: "--font-mono-default",
});

export const metadata: Metadata = {
  title: "Gestalt",
  description: "Integration management for Gestalt",
};

const themeScript = `
  (function() {
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const getTheme = function() {
      const stored = localStorage.getItem('theme');
      return stored === 'light' || stored === 'dark' || stored === 'system' ? stored : 'system';
    };
    const applyTheme = function(theme) {
      const useDark = theme === 'dark' || (theme === 'system' && media.matches);
      document.documentElement.classList.toggle('dark', useDark);
    };

    applyTheme(getTheme());
    media.addEventListener('change', function() {
      if (getTheme() === 'system') {
        applyTheme('system');
      }
    });
  })();
`;

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeScript }} />
        {/* Serve-time tenant theme: gestaltd serves the deployment-configured
            stylesheet here (empty 200 when unconfigured). Plain link, no
            React `precedence`, so it stays after the bundled CSS in document
            order. See THEMING.md. */}
        {/* eslint-disable-next-line @next/next/no-css-tags */}
        <link rel="stylesheet" href="/theme.css" />
      </head>
      <body className={`${newsreader.variable} ${instrumentSans.variable} ${geistMono.variable} font-sans antialiased`}>
        {children}
      </body>
    </html>
  );
}
