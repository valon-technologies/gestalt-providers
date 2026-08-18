// @vitest-environment happy-dom
import { afterEach, describe, expect, test, vi } from "vitest";
import { watchOAuthPopup } from "./oauthPopup";

describe("watchOAuthPopup", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("refreshes after the sign-in window was open and then closed", () => {
    vi.useFakeTimers();
    const popup = { closed: false, close() {}, location: { assign() {} } };
    const onReturned = vi.fn();
    watchOAuthPopup(popup, onReturned);

    vi.advanceTimersByTime(400);
    expect(onReturned).not.toHaveBeenCalled();

    popup.closed = true;
    vi.advanceTimersByTime(400);
    expect(onReturned).toHaveBeenCalledOnce();
  });

  test("does not treat an immediately severed handle as a finished sign-in", () => {
    vi.useFakeTimers();
    const popup = { closed: true, close() {}, location: { assign() {} } };
    const onReturned = vi.fn();
    watchOAuthPopup(popup, onReturned);

    vi.advanceTimersByTime(2000);
    expect(onReturned).not.toHaveBeenCalled();
  });

  test("refreshes when the opener is focused after the window closed", () => {
    const popup = { closed: true, close() {}, location: { assign() {} } };
    const onReturned = vi.fn();
    watchOAuthPopup(popup, onReturned);

    window.dispatchEvent(new Event("focus"));
    expect(onReturned).toHaveBeenCalledOnce();
  });
});
