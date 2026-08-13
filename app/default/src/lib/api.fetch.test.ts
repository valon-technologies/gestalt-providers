import { afterEach, describe, expect, it, vi } from "vitest";

import {
  APITimeoutError,
  APPS_CATALOG_TIMEOUT_MS,
  fetchAPI,
  getIntegrations,
} from "@/lib/api";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function hangingFetch(): typeof fetch {
  return vi.fn((_url: string | URL | Request, init?: RequestInit) => {
    return new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener(
        "abort",
        () => {
          reject(new DOMException("The operation was aborted.", "AbortError"));
        },
        { once: true },
      );
    });
  }) as unknown as typeof fetch;
}

describe("fetchAPI timeout", () => {
  it("throws APITimeoutError when the request exceeds timeoutMs", async () => {
    vi.stubGlobal("fetch", hangingFetch());

    await expect(fetchAPI("/api/v1/apps", { timeoutMs: 20 })).rejects.toBeInstanceOf(
      APITimeoutError,
    );
  });

  it("does not treat a caller abort as a timeout", async () => {
    vi.stubGlobal("fetch", hangingFetch());
    const controller = new AbortController();
    const pending = fetchAPI("/api/v1/apps", {
      signal: controller.signal,
      timeoutMs: 5_000,
    });
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });

  it("still maps HTTP 503 to APIError", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        return new Response(JSON.stringify({ error: "Service Unavailable" }), {
          status: 503,
          headers: { "Content-Type": "application/json" },
        });
      }),
    );

    await expect(fetchAPI("/api/v1/apps")).rejects.toMatchObject({
      name: "APIError",
      status: 503,
      message: "Service Unavailable",
    });
  });
});

describe("getIntegrations", () => {
  it("aborts a hung catalog list instead of waiting forever", async () => {
    vi.stubGlobal("fetch", hangingFetch());
    const realTimeout = AbortSignal.timeout.bind(AbortSignal);
    vi.spyOn(AbortSignal, "timeout").mockImplementation((ms: number) => {
      expect(ms).toBe(APPS_CATALOG_TIMEOUT_MS);
      return realTimeout(20);
    });

    await expect(getIntegrations()).rejects.toBeInstanceOf(APITimeoutError);
  });
});
