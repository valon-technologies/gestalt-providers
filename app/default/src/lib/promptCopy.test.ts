import { describe, expect, it } from "vitest";
import {
  createPromptCopyController,
  PROMPT_COPY_RESET_DELAY_MS,
  type PromptCopyScheduler,
  type PromptCopyState,
} from "./promptCopy";

function deferred() {
  let resolve!: () => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function fakeScheduler() {
  let nextHandle = 1;
  const tasks = new Map<number, { callback: () => void; delay: number }>();
  const scheduler: PromptCopyScheduler = {
    setTimeout(callback, delay) {
      const handle = nextHandle++;
      tasks.set(handle, { callback, delay });
      return handle;
    },
    clearTimeout(handle) {
      tasks.delete(handle);
    },
  };
  return {
    scheduler,
    tasks,
    run(handle: number) {
      const task = tasks.get(handle);
      if (!task) return;
      tasks.delete(handle);
      task.callback();
    },
  };
}

describe("createPromptCopyController", () => {
  it("exposes pending and copied states, then resets on schedule", async () => {
    const request = deferred();
    const timer = fakeScheduler();
    const states: PromptCopyState[] = [];
    const controller = createPromptCopyController({
      writeText: () => request.promise,
      onStateChange: (state) => states.push(state),
      scheduler: timer.scheduler,
    });

    const copy = controller.copy("@Example prompt");
    expect(states).toEqual(["copying"]);
    request.resolve();
    await copy;

    expect(states).toEqual(["copying", "copied"]);
    const [[handle, task]] = [...timer.tasks];
    expect(task.delay).toBe(PROMPT_COPY_RESET_DELAY_MS.copied);
    timer.run(handle);
    expect(states).toEqual(["copying", "copied", "idle"]);
  });

  it("reports failure and uses the longer retry reset", async () => {
    const timer = fakeScheduler();
    const states: PromptCopyState[] = [];
    const controller = createPromptCopyController({
      writeText: () => Promise.reject(new Error("unavailable")),
      onStateChange: (state) => states.push(state),
      scheduler: timer.scheduler,
    });

    await controller.copy("@Example prompt");

    expect(states).toEqual(["copying", "error"]);
    const [task] = timer.tasks.values();
    expect(task.delay).toBe(PROMPT_COPY_RESET_DELAY_MS.error);
  });

  it("cancels a prior reset when the user retries", async () => {
    const timer = fakeScheduler();
    const states: PromptCopyState[] = [];
    const controller = createPromptCopyController({
      writeText: () => Promise.resolve(),
      onStateChange: (state) => states.push(state),
      scheduler: timer.scheduler,
    });

    await controller.copy("first");
    const [firstReset] = timer.tasks.keys();
    await controller.copy("second");

    expect(timer.tasks.has(firstReset)).toBe(false);
    expect(states).toEqual(["copying", "copied", "copying", "copied"]);
  });

  it("ignores stale completion and all updates after disposal", async () => {
    const first = deferred();
    const second = deferred();
    const states: PromptCopyState[] = [];
    const writes = [first, second];
    const controller = createPromptCopyController({
      writeText: () => writes.shift()!.promise,
      onStateChange: (state) => states.push(state),
      scheduler: fakeScheduler().scheduler,
    });

    const firstCopy = controller.copy("first");
    const secondCopy = controller.copy("second");
    first.resolve();
    await firstCopy;
    expect(states).toEqual(["copying", "copying"]);

    controller.dispose();
    second.resolve();
    await secondCopy;
    await controller.copy("after disposal");
    expect(states).toEqual(["copying", "copying"]);
  });
});
