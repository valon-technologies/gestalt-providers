
import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"
import { PanelLeftIcon } from "lucide-react"
import { Slot } from "@radix-ui/react-slot"

import { useIsMobile } from "@/hooks/use-mobile"
import { cn } from "@/lib/utils"
import {
  writeSidebarOpenCookie,
} from "@/lib/sidebar-state"
import { Button } from "@/components/ui/button"
import { eyebrowVariants } from "@/components/ui/eyebrow"
import { Input } from "@/components/ui/input"
import { Separator } from "@/components/ui/separator"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  Group as PanelGroup,
  Panel,
  Separator as ResizeSeparator,
  useDefaultLayout,
  useGroupRef,
  type PanelImperativeHandle,
} from "react-resizable-panels"


const SIDEBAR_WIDTH = "16rem"
const SIDEBAR_WIDTH_MOBILE = "18rem"
const SIDEBAR_WIDTH_ICON = "3rem"
const SIDEBAR_KEYBOARD_SHORTCUT = "b"
// Drag-to-resize bounds (px). The expanded sidebar resizes fluidly down to MIN; dragging narrower
// than SNAP collapses to the icon rail.
const SIDEBAR_WIDTH_MIN = 180
const SIDEBAR_WIDTH_MAX = 360
const SIDEBAR_WIDTH_SNAP = 150
// react-resizable-panels shell (collapsible="fill"). The collapsed panel = the floating icon-rail
// width: 3rem icon + 1rem (the inner m-2) = 4rem = 64px. Must be a plain px string — RRP does not
// parse calc(). Anything at/below the threshold counts as collapsed so the shadcn icon-rail data
// attr flips in lockstep with the panel width.
const SIDEBAR_PANEL_COLLAPSED_SIZE = "64px"
const SIDEBAR_PANEL_COLLAPSED_THRESHOLD_PX = 96
// First-paint width (% of the shell) before any stored layout — RRP clamps it to min/maxSize.
const SIDEBAR_PANEL_DEFAULT_SIZE = "20%"
const SIDEBAR_PANEL_ID = "sidebar-nav"
const SIDEBAR_CONTENT_PANEL_ID = "sidebar-content"
// Grace window after pointer-up before re-enabling the flex-grow transition. RRP commits its
// release-snap (to min / collapsed) a frame or two after the drag ends; clearing data-resizing
// any sooner lets that snap animate — residual easing at the end of a drag. Generous vs the snap's
// 1-2 frame commit, far shorter than any deliberate re-toggle.
const SIDEBAR_RESIZE_SETTLE_MS = 120
const SIDEBAR_RESIZE_TARGET_MINIMUM_SIZE = { coarse: 24, fine: 16 }
// Cmd/Ctrl-B is also rich-text bold; don't hijack it while the user types in an editable element.
const SIDEBAR_EDITABLE_TARGET_SELECTOR = 'input, textarea, select, [contenteditable="true"]'

type SidebarContextProps = {
  state: "expanded" | "collapsed"
  open: boolean
  setOpen: (open: boolean) => void
  openMobile: boolean
  setOpenMobile: (open: boolean) => void
  isMobile: boolean
  toggleSidebar: () => void
  width: string
  setWidth: (width: string) => void
}

const SidebarContext = React.createContext<SidebarContextProps | null>(null)

function useSidebar() {
  const context = React.useContext(SidebarContext)
  if (!context) {
    throw new Error("useSidebar must be used within a SidebarProvider.")
  }

  return context
}

function useSidebarNavDismiss() {
  const { isMobile, setOpenMobile } = useSidebar()
  return React.useCallback(() => {
    if (isMobile) setOpenMobile(false)
  }, [isMobile, setOpenMobile])
}

function SidebarProvider({
  defaultOpen = true,
  open: openProp,
  onOpenChange: setOpenProp,
  defaultWidth = SIDEBAR_WIDTH,
  width: widthProp,
  onWidthChange,
  className,
  style,
  children,
  ...props
}: React.ComponentProps<"div"> & {
  defaultOpen?: boolean
  open?: boolean
  onOpenChange?: (open: boolean) => void
  defaultWidth?: string
  width?: string
  onWidthChange?: (width: string) => void
}) {
  const isMobile = useIsMobile()
  const [openMobile, setOpenMobile] = React.useState(false)

  // Resizable width of the expanded sidebar. Controlled via widthProp/onWidthChange, else internal.
  // The icon rail keeps its own fixed width, so this only affects the expanded state.
  const [_width, _setWidth] = React.useState(defaultWidth)
  const width = widthProp ?? _width
  const setWidth = React.useCallback(
    (value: string) => {
      // Uncontrolled (no width prop): own the state so the drag re-renders. onWidthChange is a
      // persistence notification either way — callers pass it without controlling `width`.
      if (widthProp === undefined) _setWidth(value)
      onWidthChange?.(value)
    },
    [widthProp, onWidthChange]
  )

  // This is the internal state of the sidebar.
  // We use openProp and setOpenProp for control from outside the component.
  const [_open, _setOpen] = React.useState(defaultOpen)
  const open = openProp ?? _open
  const setOpen = React.useCallback(
    (value: boolean | ((value: boolean) => boolean)) => {
      const openState = typeof value === "function" ? value(open) : value
      if (setOpenProp) {
        setOpenProp(openState)
      } else {
        _setOpen(openState)
      }

      // This sets the cookie to keep the sidebar state.
      writeSidebarOpenCookie(openState)
    },
    [setOpenProp, open]
  )

  // Helper to toggle the sidebar.
  const toggleSidebar = React.useCallback(() => {
    return isMobile ? setOpenMobile((open) => !open) : setOpen((open) => !open)
  }, [isMobile, setOpen, setOpenMobile])

  // Adds a keyboard shortcut to toggle the sidebar.
  React.useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (
        event.key === SIDEBAR_KEYBOARD_SHORTCUT &&
        (event.metaKey || event.ctrlKey)
      ) {
        const target = event.target as HTMLElement | null
        if (target?.closest(SIDEBAR_EDITABLE_TARGET_SELECTOR)) return
        event.preventDefault()
        toggleSidebar()
      }
    }

    window.addEventListener("keydown", handleKeyDown)
    return () => window.removeEventListener("keydown", handleKeyDown)
  }, [toggleSidebar])

  // We add a state so that we can do data-state="expanded" or "collapsed".
  // This makes it easier to style the sidebar with Tailwind classes.
  const state = open ? "expanded" : "collapsed"

  const contextValue = React.useMemo<SidebarContextProps>(
    () => ({
      state,
      open,
      setOpen,
      isMobile,
      openMobile,
      setOpenMobile,
      toggleSidebar,
      width,
      setWidth,
    }),
    [state, open, setOpen, isMobile, openMobile, setOpenMobile, toggleSidebar, width, setWidth]
  )

  return (
    <SidebarContext.Provider value={contextValue}>
      <TooltipProvider delayDuration={0}>
        <div
          data-slot="sidebar-wrapper"
          style={
            {
              "--sidebar-width": width,
              "--sidebar-width-icon": SIDEBAR_WIDTH_ICON,
              ...style,
            } as React.CSSProperties
          }
          className={cn(
            "group/sidebar-wrapper flex min-h-svh w-full has-data-[variant=inset]:bg-sidebar",
            className
          )}
          {...props}
        >
          {children}
        </div>
      </TooltipProvider>
    </SidebarContext.Provider>
  )
}

function Sidebar({
  side = "left",
  variant = "sidebar",
  collapsible = "offcanvas",
  className,
  children,
  ...props
}: React.ComponentProps<"div"> & {
  side?: "left" | "right"
  variant?: "sidebar" | "floating" | "inset"
  collapsible?: "offcanvas" | "icon" | "none" | "fill"
}) {
  const { isMobile, state, openMobile, setOpenMobile } = useSidebar()

  if (collapsible === "none") {
    return (
      <div
        data-slot="sidebar"
        className={cn(
          "flex h-full w-(--sidebar-width) flex-col bg-sidebar text-sidebar-foreground",
          className
        )}
        {...props}
      >
        {children}
      </div>
    )
  }

  if (isMobile) {
    return (
      <Sheet open={openMobile} onOpenChange={setOpenMobile} {...props}>
        <SheetContent
          data-sidebar="sidebar"
          data-slot="sidebar"
          data-mobile="true"
          className="w-(--sidebar-width) bg-sidebar p-0 text-sidebar-foreground [&>button]:hidden"
          style={
            {
              "--sidebar-width": SIDEBAR_WIDTH_MOBILE,
            } as React.CSSProperties
          }
          side={side}
        >
          <SheetHeader className="sr-only">
            <SheetTitle>Sidebar</SheetTitle>
            <SheetDescription>Displays the mobile sidebar.</SheetDescription>
          </SheetHeader>
          <div className="flex h-full w-full flex-col">{children}</div>
        </SheetContent>
      </Sheet>
    )
  }

  // "fill" mode: the sidebar fills its parent (a react-resizable-panels Panel that owns the width),
  // so it drops the fixed container + gap-spacer geometry. It keeps the SAME `group [data-slot=sidebar]`
  // wrapper + data attrs the icon-rail CSS keys off — data-collapsible flips to "icon" when collapsed
  // — so every group-data-[collapsible=icon] rule (icon-only buttons, hidden labels, tooltips) still fires.
  if (collapsible === "fill") {
    return (
      <div
        className={cn(
          // Floating gap is PADDING on the panel-filling wrapper (not margin on the inner) so the inner
          // stays within the panel width — margin + w-full overflowed and the panel's overflow clipped it.
          "group flex h-full w-full min-h-0 flex-col text-sidebar-foreground",
          variant === "floating" && "p-2"
        )}
        data-state={state}
        data-collapsible={state === "collapsed" ? "icon" : ""}
        data-variant={variant}
        data-side={side}
        data-slot="sidebar"
        {...props}
      >
        <div
          data-sidebar="sidebar"
          data-slot="sidebar-inner"
          className={cn(
            "flex h-full w-full min-h-0 flex-col bg-sidebar",
            variant === "floating" && "rounded-lg",
            className
          )}
        >
          {children}
        </div>
      </div>
    )
  }

  return (
    <div
      className="group peer hidden text-sidebar-foreground md:block"
      data-state={state}
      data-collapsible={state === "collapsed" ? collapsible : ""}
      data-variant={variant}
      data-side={side}
      data-slot="sidebar"
    >
      {/* This is what handles the sidebar gap on desktop */}
      <div
        data-slot="sidebar-gap"
        className={cn(
          "relative w-(--sidebar-width) bg-transparent transition-[width] duration-move group-data-[state=expanded]:ease-out-quart group-data-[state=collapsed]:ease-out-expo group-data-[resizing=true]:transition-none",
          "group-data-[collapsible=offcanvas]:w-0",
          "group-data-[side=right]:rotate-180",
          variant === "floating" || variant === "inset"
            ? "group-data-[collapsible=icon]:w-[calc(var(--sidebar-width-icon)+(--spacing(4)))]"
            : "group-data-[collapsible=icon]:w-(--sidebar-width-icon)"
        )}
      />
      <div
        data-slot="sidebar-container"
        className={cn(
          "fixed inset-y-0 z-10 hidden h-svh w-(--sidebar-width) transition-[left,right,width] duration-move group-data-[state=expanded]:ease-out-quart group-data-[state=collapsed]:ease-out-expo group-data-[resizing=true]:transition-none md:flex",
          side === "left"
            ? "left-0 group-data-[collapsible=offcanvas]:left-[calc(var(--sidebar-width)*-1)]"
            : "right-0 group-data-[collapsible=offcanvas]:right-[calc(var(--sidebar-width)*-1)]",
          // Adjust the padding for floating and inset variants.
          variant === "floating" || variant === "inset"
            ? "p-2 group-data-[collapsible=icon]:w-[calc(var(--sidebar-width-icon)+(--spacing(4))+2px)]"
            : "group-data-[collapsible=icon]:w-(--sidebar-width-icon) group-data-[side=left]:border-r group-data-[side=right]:border-l",
          className
        )}
        {...props}
      >
        <div
          data-sidebar="sidebar"
          data-slot="sidebar-inner"
          className="flex h-full w-full flex-col bg-sidebar group-data-[variant=floating]:rounded-lg"
        >
          {children}
        </div>
      </div>
    </div>
  )
}

function SidebarTrigger({
  className,
  onClick,
  ...props
}: React.ComponentProps<typeof Button>) {
  const { toggleSidebar } = useSidebar()

  return (
    <Button
      data-sidebar="trigger"
      data-slot="sidebar-trigger"
      variant="ghost"
      size="icon"
      className={cn("size-7", className)}
      onClick={(event) => {
        onClick?.(event)
        toggleSidebar()
      }}
      {...props}
    >
      <PanelLeftIcon />
      <span className="sr-only">Toggle sidebar</span>
    </Button>
  )
}

// The rail is both the click-to-toggle affordance and the drag-to-resize handle. A drag updates the
// expanded width live and snaps to the icon rail below SIDEBAR_WIDTH_SNAP; a press without movement
// toggles. Keyboard users toggle via SidebarTrigger / Cmd-B (the rail is intentionally tabIndex=-1).
function SidebarRail({ className, ...props }: React.ComponentProps<"button">) {
  const { toggleSidebar, setOpen, setWidth } = useSidebar()
  const movedRef = React.useRef(false)

  // Track the drag on window rather than via setPointerCapture: capturing keeps :hover stuck on the
  // rail after release (the browser doesn't re-evaluate hover until the next move), so the divider
  // stayed in its dragging state. Window listeners let :hover reflect the real cursor, and let the
  // drag continue when the cursor outruns the (moving) rail.
  const onPointerDown = (event: React.PointerEvent<HTMLButtonElement>) => {
    if (event.button !== 0) return
    // Prevent the default focus + text-selection so a finished drag leaves nothing selected/focused.
    event.preventDefault()
    const root = event.currentTarget.closest('[data-slot="sidebar"]') as HTMLElement | null
    if (!root) return
    const container = root.querySelector('[data-slot="sidebar-container"]') as HTMLElement | null
    const side = root.getAttribute("data-side")
    movedRef.current = false
    root.setAttribute("data-resizing", "true")

    const onMove = (moveEvent: PointerEvent) => {
      if (moveEvent.movementX !== 0 || moveEvent.movementY !== 0) movedRef.current = true
      const rect = container?.getBoundingClientRect()
      if (!rect) return
      const next = side === "right" ? rect.right - moveEvent.clientX : moveEvent.clientX - rect.left
      if (next < SIDEBAR_WIDTH_SNAP) {
        setOpen(false)
        return
      }
      setOpen(true)
      setWidth(`${Math.round(Math.min(SIDEBAR_WIDTH_MAX, Math.max(SIDEBAR_WIDTH_MIN, next)))}px`)
    }
    const onUp = () => {
      root.removeAttribute("data-resizing")
      window.removeEventListener("pointermove", onMove)
      window.removeEventListener("pointerup", onUp)
      window.removeEventListener("pointercancel", onUp)
      if (!movedRef.current) toggleSidebar()
    }
    window.addEventListener("pointermove", onMove)
    window.addEventListener("pointerup", onUp)
    window.addEventListener("pointercancel", onUp)
  }

  return (
    <button
      data-sidebar="rail"
      data-slot="sidebar-rail"
      aria-label="Toggle sidebar"
      tabIndex={-1}
      onPointerDown={onPointerDown}
      title="Toggle sidebar"
      className={cn(
        "absolute inset-y-0 z-20 hidden w-4 -translate-x-1/2 touch-none group-data-[side=left]:-right-4 group-data-[side=right]:left-0 sm:flex",
        // Full-height divider: an always-visible 1px line, centered in the grab area, that thickens to
        // a 3px brand-gold line on hover and one ramp step darker while dragging (instant, no transition).
        "after:absolute after:inset-y-0 after:left-1/2 after:w-px after:-translate-x-1/2 after:bg-sidebar-border after:transition-none hover:after:w-[3px] hover:after:bg-separator-hover group-data-[resizing=true]:after:w-[3px] group-data-[resizing=true]:after:bg-separator-active",
        "in-data-[side=left]:cursor-w-resize in-data-[side=right]:cursor-e-resize",
        "[[data-side=left][data-state=collapsed]_&]:cursor-e-resize [[data-side=right][data-state=collapsed]_&]:cursor-w-resize",
        "group-data-[collapsible=offcanvas]:translate-x-0 group-data-[collapsible=offcanvas]:after:left-full hover:group-data-[collapsible=offcanvas]:bg-sidebar",
        "[[data-side=left][data-collapsible=offcanvas]_&]:-right-2",
        "[[data-side=right][data-collapsible=offcanvas]_&]:-left-2",
        className
      )}
      {...props}
    />
  )
}

function SidebarInset({ className, ...props }: React.ComponentProps<"main">) {
  return (
    <main
      data-slot="sidebar-inset"
      className={cn(
        "relative flex w-full flex-1 flex-col bg-background",
        "md:peer-data-[variant=inset]:m-2 md:peer-data-[variant=inset]:ml-0 md:peer-data-[variant=inset]:rounded-xl md:peer-data-[variant=inset]:border md:peer-data-[variant=inset]:border-border md:peer-data-[variant=inset]:peer-data-[state=collapsed]:ml-2",
        className
      )}
      {...props}
    />
  )
}

// Desktop resizable shell: a react-resizable-panels group with the (collapsible="fill") sidebar in
// one Panel and the page content in the other, split by a draggable full-height divider. Below md it
// drops the panel group and renders the sidebar (its mobile Sheet) above the content. The panel is
// driven both ways: dragging resizes/snaps (onResize -> setOpen), while the header toggle / Cmd-B
// flip `open` and the reconcile effect expands/collapses the panel to match.
function SidebarShell({
  sidebar,
  storageId,
  className,
  children,
}: {
  sidebar: React.ReactNode
  /** Stable id the resized width persists under (localStorage), per react-resizable-panels. */
  storageId: string
  className?: string
  children: React.ReactNode
}) {
  const { isMobile, open, setOpen } = useSidebar()
  const groupRef = useGroupRef()
  const shellElementRef = React.useRef<HTMLDivElement | null>(null)
  const panelRef = React.useRef<PanelImperativeHandle | null>(null)
  const collapsedRef = React.useRef(!open)
  // Arm the collapse transition only AFTER the initial RRP layout settles (two frames). Otherwise the
  // mount animates flex-grow up from 0, dipping under the collapse threshold and snapping collapsed on load.
  const [motionArmed, setMotionArmed] = React.useState(false)
  React.useEffect(() => {
    let inner = 0
    const outer = requestAnimationFrame(() => {
      inner = requestAnimationFrame(() => setMotionArmed(true))
    })
    return () => {
      cancelAnimationFrame(outer)
      cancelAnimationFrame(inner)
    }
  }, [])
  // RRP needs an initial layout or it collapses the collapsible panel on first measure; this also
  // persists the resized width (device-local). Collapse stays driven by `open` (cookie/server pref).
  const { defaultLayout, onLayoutChanged } = useDefaultLayout({
    id: storageId,
    panelIds: [SIDEBAR_PANEL_ID, SIDEBAR_CONTENT_PANEL_ID],
    storage: typeof window !== "undefined" ? localStorage : undefined,
  })

  React.useEffect(() => {
    collapsedRef.current = !open
  }, [open])

  React.useEffect(() => {
    const panel = panelRef.current
    if (!panel || isMobile) return
    // Guarded so this never ping-pongs with onResize: only act when the panel disagrees with `open`.
    if (open && panel.isCollapsed()) panel.expand()
    else if (!open && !panel.isCollapsed()) panel.collapse()
  }, [open, isMobile])

  // A toggle/Cmd-B collapse animates RRP's flex-grow with an ease-out-back; a live drag must NOT
  // (the transition would lag the cursor). We mark the group data-resizing SYNCHRONOUSLY on pointer-down
  // (not via React state, which lags one render and lets the first pointer-move animate) so the very
  // first drag frame is instant; CSS drops the flex-grow transition while the attribute is set.
  const beginResize = React.useCallback((event: React.PointerEvent) => {
    // Mark the group data-resizing SYNCHRONOUSLY on pointer-down (no React state, which lags a render).
    // The transition is gated on :not([data-resizing]), so setting the attribute kills the easing the
    // same frame — no flex-grow transition fires while the cursor drives the width.
    const group = event.currentTarget.closest("[data-sidebar-shell]")
    group?.setAttribute("data-resizing", "true")
    const end = () => {
      window.removeEventListener("pointerup", end)
      // Hold the attribute past pointer-up so RRP's release-snap commits with transitions still off
      // (see SIDEBAR_RESIZE_SETTLE_MS) — otherwise that snap animates as residual end-of-drag easing.
      window.setTimeout(() => group?.removeAttribute("data-resizing"), SIDEBAR_RESIZE_SETTLE_MS)
    }
    window.addEventListener("pointerup", end)
  }, [])
  // RRP sizes panels by flex-grow on the group's direct children (NOT the className target). Animate
  // that with ease-out-back on toggle/Cmd-B, but ONLY while the group is not being dragged: the
  // :not([data-resizing]) guard makes the synchronous attribute disable the transition outright.
  const groupMotion = motionArmed
    ? "[&:not([data-resizing])>*]:transition-[flex-grow] [&:not([data-resizing])>*]:duration-move [&:not([data-resizing])>*]:ease-out-back-soft"
    : undefined

  if (isMobile) {
    return (
      <div className={cn("flex h-full min-h-0 w-full flex-col", className)}>
        {sidebar}
        <div className="flex min-h-0 flex-1 flex-col">
          <div className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
            <SidebarTrigger />
          </div>
          <SidebarInset className="min-h-0 flex-1 overflow-y-auto">{children}</SidebarInset>
        </div>
      </div>
    )
  }

  return (
    <PanelGroup
      orientation="horizontal"
      groupRef={groupRef}
      elementRef={shellElementRef}
      defaultLayout={defaultLayout}
      onLayoutChanged={onLayoutChanged}
      resizeTargetMinimumSize={SIDEBAR_RESIZE_TARGET_MINIMUM_SIZE}
      // Stable hook so the divider can find THIS group via closest() to toggle data-resizing — the
      // separator's parentElement is not reliably the group across RRP re-renders (collapse/expand).
      data-sidebar-shell=""
      className={cn("flex h-full min-h-0 w-full", groupMotion, className)}
    >
      <Panel
        id={SIDEBAR_PANEL_ID}
        panelRef={panelRef}
        collapsible
        collapsedSize={SIDEBAR_PANEL_COLLAPSED_SIZE}
        minSize={`${SIDEBAR_WIDTH_MIN}px`}
        maxSize={`${SIDEBAR_WIDTH_MAX}px`}
        defaultSize={SIDEBAR_PANEL_DEFAULT_SIZE}
        style={{ overflow: "hidden" }}
        onResize={(size) => {
          // Only user drags may drive `open`. Toggle/Cmd-B collapse animates through sizes
          // still above the threshold; treating those as expand would snap the rail back open.
          if (!shellElementRef.current?.hasAttribute("data-resizing")) return
          const collapsed = size.inPixels <= SIDEBAR_PANEL_COLLAPSED_THRESHOLD_PX
          if (collapsed !== collapsedRef.current) {
            collapsedRef.current = collapsed
            setOpen(!collapsed)
          }
        }}
      >
        {sidebar}
      </Panel>
      <SidebarDivider onPointerDown={beginResize} />
      <Panel id={SIDEBAR_CONTENT_PANEL_ID} minSize="40%" style={{ overflow: "hidden" }}>
        <SidebarInset className="h-full overflow-y-auto">{children}</SidebarInset>
      </Panel>
    </PanelGroup>
  )
}

// Full-height resize divider: a 1px line that thickens to a brand-gold line on hover/drag.
// react-resizable-panels sets data-separator=hover|active on the handle (same recipe as InspectorRail).
function SidebarDivider({ className, onPointerDown }: { className?: string; onPointerDown?: React.PointerEventHandler }) {
  return (
    <ResizeSeparator
      onPointerDown={onPointerDown}
      onPointerUp={(event) => event.currentTarget.blur()}
      className={cn(
        "group relative z-20 w-px shrink-0 cursor-col-resize",
        "before:pointer-events-none before:absolute before:inset-y-0 before:left-1/2 before:w-px before:-translate-x-1/2 before:bg-sidebar-border before:content-['']",
        "data-[separator=hover]:before:w-[3px] data-[separator=hover]:before:bg-separator-hover",
        "focus-visible:before:w-[3px] focus-visible:before:bg-separator-hover",
        "data-[separator=active]:before:w-[3px] data-[separator=active]:before:bg-separator-active",
        className
      )}
    />
  )
}

function SidebarInput({
  className,
  ...props
}: React.ComponentProps<typeof Input>) {
  return (
    <Input
      data-slot="sidebar-input"
      data-sidebar="input"
      className={cn("h-8 w-full bg-background shadow-none", className)}
      {...props}
    />
  )
}

function SidebarHeader({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="sidebar-header"
      data-sidebar="header"
      className={cn("flex flex-col gap-2 p-2", className)}
      {...props}
    />
  )
}

function SidebarFooter({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="sidebar-footer"
      data-sidebar="footer"
      className={cn("flex flex-col gap-2 p-2", className)}
      {...props}
    />
  )
}

function SidebarSeparator({
  className,
  ...props
}: React.ComponentProps<typeof Separator>) {
  return (
    <Separator
      data-slot="sidebar-separator"
      data-sidebar="separator"
      className={cn(
        "mx-2 bg-sidebar-border data-[orientation=horizontal]:w-auto",
        className
      )}
      {...props}
    />
  )
}

function SidebarContent({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="sidebar-content"
      data-sidebar="content"
      className={cn(
        "flex min-h-0 flex-1 flex-col gap-2 overflow-auto group-data-[collapsible=icon]:overflow-hidden",
        className
      )}
      {...props}
    />
  )
}

function SidebarGroup({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="sidebar-group"
      data-sidebar="group"
      className={cn("relative flex w-full min-w-0 flex-col p-2", className)}
      {...props}
    />
  )
}

// Section labels are Eyebrow microtype at sm on sidebar ink (guidelines/eyebrow.md).
function SidebarGroupLabel({
  className,
  asChild = false,
  ...props
}: React.ComponentProps<"div"> & { asChild?: boolean }) {
  const Comp = asChild ? Slot : "div"

  return (
    <Comp
      data-slot="sidebar-group-label"
      data-sidebar="group-label"
      className={cn(
        eyebrowVariants({ size: "sm" }),
        "text-sidebar-foreground/60",
        "flex h-8 shrink-0 items-center rounded-md px-2 transition-[margin,opacity] duration-move ease-out-quart focus-ring [&>svg]:size-4 [&>svg]:shrink-0",
        "group-data-[collapsible=icon]:-mt-8 group-data-[collapsible=icon]:opacity-0",
        className
      )}
      {...props}
    />
  )
}

function SidebarGroupAction({
  className,
  asChild = false,
  ...props
}: React.ComponentProps<"button"> & { asChild?: boolean }) {
  const Comp = asChild ? Slot : "button"

  return (
    <Comp
      data-slot="sidebar-group-action"
      data-sidebar="group-action"
      className={cn(
        "absolute top-3.5 right-3 flex aspect-square w-5 items-center justify-center rounded-md p-0 text-sidebar-foreground transition-transform hover:bg-neutral-dark-hover hover:text-sidebar-foreground focus-ring [&>svg]:size-4 [&>svg]:shrink-0",
        // Increases the hit area of the button on mobile.
        "after:absolute after:-inset-2 md:after:hidden",
        "group-data-[collapsible=icon]:hidden",
        className
      )}
      {...props}
    />
  )
}

function SidebarGroupContent({
  className,
  ...props
}: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="sidebar-group-content"
      data-sidebar="group-content"
      className={cn("w-full text-sm", className)}
      {...props}
    />
  )
}

function SidebarMenu({ className, ...props }: React.ComponentProps<"ul">) {
  return (
    <ul
      data-slot="sidebar-menu"
      data-sidebar="menu"
      className={cn("flex w-full min-w-0 flex-col gap-1", className)}
      {...props}
    />
  )
}

function SidebarMenuItem({ className, ...props }: React.ComponentProps<"li">) {
  return (
    <li
      data-slot="sidebar-menu-item"
      data-sidebar="menu-item"
      className={cn("group/menu-item relative", className)}
      {...props}
    />
  )
}

const sidebarMenuButtonVariants = cva(
  "peer/menu-button flex w-full items-center gap-2 overflow-hidden rounded-md p-2 text-left text-sm text-sidebar-foreground/65 transition-[width,height,padding] group-has-data-[sidebar=menu-action]/menu-item:pr-8 group-data-[collapsible=icon]:size-8! group-data-[collapsible=icon]:p-2! focus-ring-inset disabled:pointer-events-none disabled:opacity-50 aria-disabled:pointer-events-none aria-disabled:opacity-50 [&:not([data-active=true])]:hover:bg-neutral-dark-hover [&:not([data-active=true])]:hover:text-sidebar-foreground [&:not([data-active=true])]:active:bg-neutral-dark-pressed [&:not([data-active=true])]:active:text-sidebar-foreground data-[active=true]:bg-accent-vivid data-[active=true]:font-medium data-[active=true]:text-accent-vivid-foreground data-[active=true]:hover:bg-accent-vivid-hover data-[active=true]:hover:text-accent-vivid-foreground data-[active=true]:active:bg-accent-vivid-pressed data-[active=true]:active:text-accent-vivid-foreground data-[state=open]:[&:not([data-active=true])]:hover:bg-neutral-dark-hover data-[state=open]:[&:not([data-active=true])]:hover:text-sidebar-foreground [&>span:last-child]:truncate [&>svg]:size-4 [&>svg]:shrink-0",
  {
    variants: {
      variant: {
        default: "",
        outline:
          "bg-background text-sidebar-foreground shadow-[0_0_0_1px_var(--sidebar-border)] [&:not([data-active=true])]:hover:shadow-[0_0_0_1px_var(--sidebar-border)]",
      },
      size: {
        default: "h-8 text-sm",
        sm: "h-7 text-xs",
        lg: "h-12 text-sm group-data-[collapsible=icon]:p-0!",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
)

function SidebarMenuButton({
  asChild = false,
  isActive = false,
  variant = "default",
  size = "default",
  tooltip,
  className,
  ...props
}: React.ComponentProps<"button"> & {
  asChild?: boolean
  isActive?: boolean
  tooltip?: string | React.ComponentProps<typeof TooltipContent>
} & VariantProps<typeof sidebarMenuButtonVariants>) {
  const Comp = asChild ? Slot : "button"
  const { isMobile, state } = useSidebar()

  const button = (
    <Comp
      data-slot="sidebar-menu-button"
      data-sidebar="menu-button"
      data-size={size}
      data-active={isActive}
      className={cn(sidebarMenuButtonVariants({ variant, size }), className)}
      {...props}
    />
  )

  if (!tooltip) {
    return button
  }

  if (typeof tooltip === "string") {
    tooltip = {
      children: tooltip,
    }
  }

  return (
    <Tooltip>
      <TooltipTrigger asChild>{button}</TooltipTrigger>
      <TooltipContent
        side="right"
        align="center"
        hidden={state !== "collapsed" || isMobile}
        {...tooltip}
      />
    </Tooltip>
  )
}

function SidebarMenuAction({
  className,
  asChild = false,
  showOnHover = false,
  ...props
}: React.ComponentProps<"button"> & {
  asChild?: boolean
  showOnHover?: boolean
}) {
  const Comp = asChild ? Slot : "button"

  return (
    <Comp
      data-slot="sidebar-menu-action"
      data-sidebar="menu-action"
      className={cn(
        "absolute top-1.5 right-1 flex aspect-square w-5 items-center justify-center rounded-md p-0 text-sidebar-foreground transition-transform peer-hover/menu-button:text-accent-vivid-foreground hover:bg-neutral-dark-hover hover:text-sidebar-foreground focus-ring [&>svg]:size-4 [&>svg]:shrink-0",
        // Increases the hit area of the button on mobile.
        "after:absolute after:-inset-2 md:after:hidden",
        "peer-data-[size=sm]/menu-button:top-1",
        "peer-data-[size=default]/menu-button:top-1.5",
        "peer-data-[size=lg]/menu-button:top-2.5",
        "group-data-[collapsible=icon]:hidden",
        showOnHover &&
          "group-focus-within/menu-item:opacity-100 group-hover/menu-item:opacity-100 peer-data-[active=true]/menu-button:text-accent-vivid-foreground data-[state=open]:opacity-100 md:opacity-0",
        className
      )}
      {...props}
    />
  )
}

function SidebarMenuBadge({
  className,
  ...props
}: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="sidebar-menu-badge"
      data-sidebar="menu-badge"
      className={cn(
        "pointer-events-none absolute right-1 flex h-5 min-w-5 items-center justify-center rounded-md px-1 text-xs font-medium text-sidebar-foreground tabular-nums select-none",
        "peer-hover/menu-button:text-accent-vivid-foreground peer-data-[active=true]/menu-button:text-accent-vivid-foreground",
        "peer-data-[size=sm]/menu-button:top-1",
        "peer-data-[size=default]/menu-button:top-1.5",
        "peer-data-[size=lg]/menu-button:top-2.5",
        "group-data-[collapsible=icon]:hidden",
        className
      )}
      {...props}
    />
  )
}

function SidebarMenuSkeleton({
  className,
  showIcon = false,
  ...props
}: React.ComponentProps<"div"> & {
  showIcon?: boolean
}) {
  // Random width between 50 to 90%.
  const width = React.useMemo(() => {
    return `${Math.floor(Math.random() * 40) + 50}%`
  }, [])

  return (
    <div
      data-slot="sidebar-menu-skeleton"
      data-sidebar="menu-skeleton"
      className={cn("flex h-8 items-center gap-2 rounded-md px-2", className)}
      {...props}
    >
      {showIcon && (
        <Skeleton
          className="size-4 rounded-md"
          data-sidebar="menu-skeleton-icon"
        />
      )}
      <Skeleton
        className="h-4 max-w-(--skeleton-width) flex-1"
        data-sidebar="menu-skeleton-text"
        style={
          {
            "--skeleton-width": width,
          } as React.CSSProperties
        }
      />
    </div>
  )
}

function SidebarMenuSub({ className, ...props }: React.ComponentProps<"ul">) {
  return (
    <ul
      data-slot="sidebar-menu-sub"
      data-sidebar="menu-sub"
      className={cn(
        "mx-3.5 flex min-w-0 translate-x-px flex-col gap-1 border-l border-sidebar-border px-2.5 py-0.5",
        "group-data-[collapsible=icon]:hidden",
        className
      )}
      {...props}
    />
  )
}

function SidebarMenuSubItem({
  className,
  ...props
}: React.ComponentProps<"li">) {
  return (
    <li
      data-slot="sidebar-menu-sub-item"
      data-sidebar="menu-sub-item"
      className={cn("group/menu-sub-item relative", className)}
      {...props}
    />
  )
}

function SidebarMenuSubButton({
  asChild = false,
  size = "md",
  isActive = false,
  className,
  ...props
}: React.ComponentProps<"a"> & {
  asChild?: boolean
  size?: "sm" | "md"
  isActive?: boolean
}) {
  const Comp = asChild ? Slot : "a"

  return (
    <Comp
      data-slot="sidebar-menu-sub-button"
      data-sidebar="menu-sub-button"
      data-size={size}
      data-active={isActive}
      className={cn(
        "flex h-7 min-w-0 -translate-x-px items-center gap-2 overflow-hidden rounded-md px-2 text-sidebar-foreground/65 focus-ring-inset disabled:pointer-events-none disabled:opacity-50 aria-disabled:pointer-events-none aria-disabled:opacity-50 [&:not([data-active=true])]:hover:bg-neutral-dark-hover [&:not([data-active=true])]:hover:text-sidebar-foreground [&:not([data-active=true])]:active:bg-neutral-dark-pressed [&:not([data-active=true])]:active:text-sidebar-foreground [&>span:last-child]:truncate [&>svg]:size-4 [&>svg]:shrink-0",
        "data-[active=true]:bg-accent-vivid data-[active=true]:text-accent-vivid-foreground data-[active=true]:hover:bg-accent-vivid-hover data-[active=true]:hover:text-accent-vivid-foreground data-[active=true]:active:bg-accent-vivid-pressed data-[active=true]:active:text-accent-vivid-foreground",
        size === "sm" && "text-xs",
        size === "md" && "text-sm",
        "group-data-[collapsible=icon]:hidden",
        className
      )}
      {...props}
    />
  )
}

export {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupAction,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInput,
  SidebarInset,
  SidebarMenu,
  SidebarMenuAction,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSkeleton,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  SidebarProvider,
  SidebarRail,
  SidebarSeparator,
  SidebarShell,
  SidebarTrigger,
  useSidebar,
  useSidebarNavDismiss,
}
export {
  SIDEBAR_COOKIE_MAX_AGE,
  SIDEBAR_COOKIE_NAME,
  readSidebarCollapsedFromDocument,
  writeSidebarOpenCookie,
} from "@/lib/sidebar-state"
