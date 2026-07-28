/** Wire format for sidebar open/collapsed state — owned by the Sidebar primitive, read by apps. */

export const SIDEBAR_COOKIE_NAME = "sidebar_state"
export const SIDEBAR_COOKIE_MAX_AGE = 60 * 60 * 24 * 7
/** Pre–registry-sidebar localStorage key; migrated once into the cookie contract. */
export const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "gestalt:sidebar-collapsed"

/** `open` — the Sidebar primitive's controlled prop (`true` = expanded). */
export function writeSidebarOpenCookie(open: boolean): void {
  if (typeof document === "undefined") return
  document.cookie = `${SIDEBAR_COOKIE_NAME}=${String(open)}; path=/; max-age=${SIDEBAR_COOKIE_MAX_AGE}`
}

function readSidebarOpenCookie(): boolean | null {
  if (typeof document === "undefined") return null
  const prefix = `${SIDEBAR_COOKIE_NAME}=`
  const entry = document.cookie.split("; ").find((row) => row.startsWith(prefix))
  if (!entry) return null
  const value = entry.slice(prefix.length)
  if (value === "true") return true
  if (value === "false") return false
  return null
}

/** Whether the sidebar should start collapsed (inverse of cookie `open`). */
export function readSidebarCollapsedFromDocument(): boolean {
  const open = readSidebarOpenCookie()
  if (open !== null) return !open

  try {
    if (localStorage.getItem(LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY) === "1") {
      writeSidebarOpenCookie(false)
      localStorage.removeItem(LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY)
      return true
    }
  } catch {
    // private browsing / blocked storage
  }

  return false
}
