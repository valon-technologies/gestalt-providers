export const APP_ADMIN_NAV_ITEMS = [
  {
    id: "snapshots",
    label: "Published snapshots",
    to: "/apps/$app/admin/snapshots" as const,
  },
  {
    id: "history",
    label: "Revision history",
    to: "/apps/$app/admin/history" as const,
  },
  {
    id: "workflows",
    label: "Workflows",
    to: "/apps/$app/admin/workflows" as const,
  },
] as const;
