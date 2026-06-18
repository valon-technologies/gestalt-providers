const AUTH_SESSION_KEY = "gestalt.auth.session";
const USER_EMAIL_KEY = "user_email";

export type CachedAuthSession = {
  subjectId: string;
  email?: string;
  displayName?: string;
};

function trimOptional(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

export function getCachedSession(): CachedAuthSession | null {
  if (typeof window === "undefined") return null;
  const raw = localStorage.getItem(AUTH_SESSION_KEY);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as CachedAuthSession;
    if (!parsed?.subjectId?.trim()) return null;
    return {
      subjectId: parsed.subjectId.trim(),
      email: trimOptional(parsed.email),
      displayName: trimOptional(parsed.displayName),
    };
  } catch {
    return null;
  }
}

export function setCachedSession(session: CachedAuthSession): void {
  if (typeof window === "undefined") return;
  const subjectId = session.subjectId?.trim();
  if (!subjectId) return;
  const stored: CachedAuthSession = { subjectId };
  const email = trimOptional(session.email);
  const displayName = trimOptional(session.displayName);
  if (email) stored.email = email;
  if (displayName) stored.displayName = displayName;
  localStorage.setItem(AUTH_SESSION_KEY, JSON.stringify(stored));
}

export function clearSession(): void {
  if (typeof window === "undefined") return;
  localStorage.removeItem(AUTH_SESSION_KEY);
  localStorage.removeItem(USER_EMAIL_KEY);
}

export function getUserEmail(): string | null {
  return getCachedSession()?.email ?? null;
}

export function sessionDisplayLabel(
  session: CachedAuthSession | null,
): string | null {
  if (!session) return null;
  const displayName = session.displayName?.trim();
  if (displayName) return displayName;
  const email = session.email?.trim();
  if (email) return email;
  const subjectId = session.subjectId?.trim();
  if (subjectId) return subjectId;
  return null;
}
