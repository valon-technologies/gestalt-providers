import type { Metadata } from "next";
import { appTitle } from "../src/app-definition";

export const metadata: Metadata = {
  title: appTitle,
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
