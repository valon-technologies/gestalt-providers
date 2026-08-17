import { useId, useState, type FormEvent } from "react";
import { CircleAlert } from "lucide-react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Field,
  FieldDescription,
  FieldError,
  FieldLabel,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { APIError } from "@/lib/api";
import { userFacingError } from "@/lib/user-facing-error";
import { AccessGroupBadge } from "./access-group-badge";
import { parseGroupSelector, type AppAccessEntry } from "./admin-access";
import {
  ADD_GROUP_INVALID,
  DIALOG_CANCEL,
  LOCKED_FROM_CONFIG,
  REMOVE_ACCESS_CONFIRM_ACTION,
  REMOVE_ACCESS_CONFIRM_DESCRIPTION,
  REMOVE_ACCESS_CONFIRM_TITLE,
  REMOVE_ACCESS_LABEL,
} from "./admin-access-copy";

export function accessActionErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof APIError) return userFacingError(error, fallback);
  return fallback;
}

export function AccessEntryRow({
  entry,
  busy,
  onRemove,
  testId = "admin-access-entry",
  confirmDescription = REMOVE_ACCESS_CONFIRM_DESCRIPTION,
}: {
  entry: AppAccessEntry;
  busy: boolean;
  onRemove: () => void;
  testId?: string;
  confirmDescription?: (label: string) => string;
}) {
  const locked = !entry.mutable;
  const lockedHintId = useId();
  const [confirmOpen, setConfirmOpen] = useState(false);
  return (
    <li className="flex flex-col gap-2 px-4 py-3" data-testid={testId}>
      <div className="flex min-w-0 items-baseline justify-between gap-4">
        <div className="inline-flex min-w-0 max-w-full">
          {entry.kind === "group" ? (
            <AccessGroupBadge label={entry.label} className="max-w-full" />
          ) : (
            <p className="truncate text-sm font-medium text-foreground">
              {entry.label}
            </p>
          )}
        </div>
        <Button
          type="button"
          variant="outline"
          size="xs"
          className="shrink-0"
          disabled={locked || busy}
          aria-label={`${REMOVE_ACCESS_LABEL} for ${entry.label}`}
          aria-describedby={locked ? lockedHintId : undefined}
          onClick={() => setConfirmOpen(true)}
        >
          {REMOVE_ACCESS_LABEL}
        </Button>
      </div>
      {locked ? (
        <p
          id={lockedHintId}
          className="flex max-w-64 items-start justify-end gap-1 self-end text-right text-xs text-muted-foreground"
        >
          <CircleAlert
            className="mt-px size-3.5 shrink-0"
            aria-hidden="true"
          />
          <span>{LOCKED_FROM_CONFIG}</span>
        </p>
      ) : null}
      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{REMOVE_ACCESS_CONFIRM_TITLE}</AlertDialogTitle>
            <AlertDialogDescription>
              {confirmDescription(entry.label)}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={busy}>{DIALOG_CANCEL}</AlertDialogCancel>
            <AlertDialogAction
              disabled={busy}
              onClick={() => {
                setConfirmOpen(false);
                onRemove();
              }}
            >
              {REMOVE_ACCESS_CONFIRM_ACTION}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </li>
  );
}

export function AddAccessDialog({
  open,
  title,
  description,
  fieldLabel,
  fieldHint,
  inputType,
  placeholder,
  confirmLabel,
  busy,
  onOpenChange,
  onSubmit,
}: {
  open: boolean;
  title: string;
  description: string;
  fieldLabel: string;
  fieldHint: string;
  inputType: "email" | "text";
  placeholder: string;
  confirmLabel: string;
  busy: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (value: string) => Promise<void>;
}) {
  const [value, setValue] = useState("");
  const [formError, setFormError] = useState<string | null>(null);
  const inputId = useId();
  const errorId = `${inputId}-error`;

  async function handleSubmit(event: FormEvent) {
    event.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      setFormError(`Enter ${fieldLabel.toLowerCase()}.`);
      return;
    }
    if (inputType === "email" && !trimmed.includes("@")) {
      setFormError("Enter a work email.");
      return;
    }
    if (inputType === "text" && !parseGroupSelector(trimmed).id) {
      setFormError(ADD_GROUP_INVALID);
      return;
    }
    setFormError(null);
    try {
      await onSubmit(trimmed);
      setValue("");
      onOpenChange(false);
    } catch (error) {
      setFormError(accessActionErrorMessage(error, "Couldn't save that change."));
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) {
          setValue("");
          setFormError(null);
        }
        onOpenChange(next);
      }}
    >
      <DialogContent>
        <form onSubmit={handleSubmit} className="grid gap-4">
          <DialogHeader>
            <DialogTitle className="text-xl">{title}</DialogTitle>
            <DialogDescription className="text-base">{description}</DialogDescription>
          </DialogHeader>
          <Field data-invalid={formError ? true : undefined}>
            <FieldLabel htmlFor={inputId}>{fieldLabel}</FieldLabel>
            <Input
              id={inputId}
              type={inputType}
              autoComplete="off"
              value={value}
              onChange={(event) => setValue(event.target.value)}
              placeholder={placeholder}
              aria-invalid={formError ? true : undefined}
              aria-describedby={formError ? errorId : undefined}
            />
            <FieldDescription>{fieldHint}</FieldDescription>
            {formError ? <FieldError id={errorId}>{formError}</FieldError> : null}
          </Field>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
            >
              {DIALOG_CANCEL}
            </Button>
            <Button type="submit" loading={busy}>
              {confirmLabel}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
