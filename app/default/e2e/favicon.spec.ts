import { test, expect } from "@playwright/test";

const faviconRoutes = [
  { path: "/favicon.svg", contentType: "image/svg+xml" },
  { path: "/favicon-32x32.png", contentType: "image/png" },
  { path: "/favicon-48x48.png", contentType: "image/png" },
  { path: "/apple-touch-icon.png", contentType: "image/png" },
] as const;

test.describe("Favicon assets", () => {
  for (const route of faviconRoutes) {
    test(`serves ${route.path}`, async ({ request }) => {
      const response = await request.get(route.path);
      expect(response.ok()).toBeTruthy();
      expect(response.headers()["content-type"]).toContain(route.contentType);
      expect((await response.body()).byteLength).toBeGreaterThan(0);
    });
  }

  test("index.html references favicon links", async ({ request }) => {
    const response = await request.get("/");
    expect(response.ok()).toBeTruthy();
    const html = await response.text();
    expect(html).toMatch(/href="\.?\/?favicon\.svg"/);
    expect(html).toMatch(/href="\.?\/?apple-touch-icon\.png"/);
  });
});
