export class CursorExecutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CursorExecutionError";
  }
}

export class CursorExecutionCanceled extends Error {
  constructor(message = "Cursor Agent SDK turn was canceled") {
    super(message);
    this.name = "CursorExecutionCanceled";
  }
}
