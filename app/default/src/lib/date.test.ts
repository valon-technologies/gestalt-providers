import { afterEach, describe, expect, it, vi } from "vitest";
import { formatEventWhen } from "./date";

function localStamp(year: number, monthIndex: number, day: number, hour: number, minute: number) {
  return new Date(year, monthIndex, day, hour, minute, 0);
}

function clock(date: Date): string {
  return date.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

describe("formatEventWhen", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses Today / Yesterday plus a clock time", () => {
    vi.useFakeTimers();
    vi.setSystemTime(localStamp(2026, 7, 19, 17, 8));

    const today = localStamp(2026, 7, 19, 17, 5);
    const yesterday = localStamp(2026, 7, 18, 17, 44);

    expect(formatEventWhen(today.toISOString())).toBe(`Today at ${clock(today)}`);
    expect(formatEventWhen(yesterday.toISOString())).toBe(
      `Yesterday at ${clock(yesterday)}`,
    );
  });

  it("keeps month, day, and time in the same year, and adds the year when it differs", () => {
    vi.useFakeTimers();
    vi.setSystemTime(localStamp(2026, 7, 19, 17, 8));

    const sameYear = localStamp(2026, 0, 15, 10, 0);
    const priorYear = localStamp(2025, 0, 22, 11, 0);

    expect(formatEventWhen(sameYear.toISOString())).toBe(
      `${sameYear.toLocaleDateString(undefined, { month: "short", day: "numeric" })} at ${clock(sameYear)}`,
    );
    expect(formatEventWhen(priorYear.toISOString())).toBe(
      `${priorYear.toLocaleDateString(undefined, {
        month: "short",
        day: "numeric",
        year: "numeric",
      })} at ${clock(priorYear)}`,
    );
  });

  it("omits the clock for all-day calendar values", () => {
    vi.useFakeTimers();
    vi.setSystemTime(localStamp(2026, 7, 19, 17, 8));

    expect(formatEventWhen("2026-01-15")).toBe(
      localStamp(2026, 0, 15, 0, 0).toLocaleDateString(undefined, {
        month: "short",
        day: "numeric",
      }),
    );
  });
});
