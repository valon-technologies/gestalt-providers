import { describe, expect, test } from "vitest";

import { formatAppSourceLabel, resolveAppSourceHref } from "./appSource";

describe("resolveAppSourceHref", () => {
  test("accepts http(s) tree URLs", () => {
    expect(
      resolveAppSourceHref(
        "https://github.com/example-org/example-app/tree/main/apps/roadmap",
      ),
    ).toBe("https://github.com/example-org/example-app/tree/main/apps/roadmap");
    expect(resolveAppSourceHref("http://git.example.com/apps/roadmap")).toBe(
      "http://git.example.com/apps/roadmap",
    );
  });

  test("trims whitespace", () => {
    expect(
      resolveAppSourceHref("  https://github.com/example/app/tree/main/src  "),
    ).toBe("https://github.com/example/app/tree/main/src");
  });

  test("rejects missing, relative, and non-http URLs", () => {
    expect(resolveAppSourceHref(undefined)).toBeNull();
    expect(resolveAppSourceHref("")).toBeNull();
    expect(resolveAppSourceHref("   ")).toBeNull();
    expect(resolveAppSourceHref("/apps/roadmap")).toBeNull();
    expect(resolveAppSourceHref("javascript:alert(1)")).toBeNull();
    expect(
      resolveAppSourceHref(
        "git+https://github.com/example-org/example-app.git@main#apps/roadmap",
      ),
    ).toBeNull();
  });
});

describe("formatAppSourceLabel", () => {
  test("drops GitHub tree and blob refs", () => {
    expect(
      formatAppSourceLabel(
        "https://github.com/example-org/example-app/tree/main/apps/roadmap",
      ),
    ).toBe("example-org/example-app/apps/roadmap");
    expect(
      formatAppSourceLabel(
        "https://www.github.com/example-org/example-app/blob/abc123/apps/roadmap/README.md",
      ),
    ).toBe("example-org/example-app/apps/roadmap/README.md");
    expect(
      formatAppSourceLabel(
        "https://github.com/example-org/example-app/tree/feat%2Fui/apps/roadmap",
      ),
    ).toBe("example-org/example-app/apps/roadmap");
  });

  test("keeps host plus path for other URLs", () => {
    expect(
      formatAppSourceLabel("https://git.example.com/group/app/-/tree/main/src"),
    ).toBe("git.example.com/group/app/-/tree/main/src");
  });
});
