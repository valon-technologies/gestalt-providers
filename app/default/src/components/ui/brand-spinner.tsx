/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";

import { cn } from "@/lib/cn";

const BRAND_SPINNER_SIZE_DEFAULT = 24;
const BRAND_SPINNER_STROKE_WIDTH_DEFAULT = 20;

const BRAND_SPINNER_CENTER = 118;
const BRAND_SPINNER_PIECE_COUNT = 8;
const BRAND_SPINNER_STEP_DEG = 360 / BRAND_SPINNER_PIECE_COUNT;

/*
 * Brand / identity loading experience (RES-20260804-002). Implementation today
 * is the Gestalt mark: an open V centerline (left tip → bottom apex → right tip)
 * rotated about the center in 45° steps (pieceIndex = clockwise position). Each
 * V renders twice: a static muted "track" so the whole mark stays legible, and
 * an accent "trail" path that fades the whole V on then off. Staggered by
 * --piece-index, each V snaps on as a unit and hands off around the ring.
 *
 * The API is BrandSpinner (role: brand moment), not the glyph — swap the
 * artwork later without renaming the component. For routine busy (buttons,
 * rows, tables) use Spinner.
 */
const BRAND_SPINNER_BASE_V: ReadonlyArray<readonly [number, number]> = [
  [91.261, 0], // left tip (outer edge of the ring)
  [118, 56.804], // apex (points in toward the center)
  [144.739, 0], // right tip (outer edge of the ring)
];

function rotateAboutCenter([x, y]: readonly [number, number], deg: number): [number, number] {
  const rad = (deg * Math.PI) / 180;
  const dx = x - BRAND_SPINNER_CENTER;
  const dy = y - BRAND_SPINNER_CENTER;
  const round = (n: number) => Math.round(n * 1000) / 1000;
  return [
    round(BRAND_SPINNER_CENTER + dx * Math.cos(rad) - dy * Math.sin(rad)),
    round(BRAND_SPINNER_CENTER + dx * Math.sin(rad) + dy * Math.cos(rad)),
  ];
}

const BRAND_SPINNER_PIECES = Array.from({ length: BRAND_SPINNER_PIECE_COUNT }, (_, pieceIndex) => {
  const d = BRAND_SPINNER_BASE_V.map((point, i) => {
    const [x, y] = rotateAboutCenter(point, pieceIndex * BRAND_SPINNER_STEP_DEG);
    return `${i === 0 ? "M" : "L"}${x} ${y}`;
  }).join(" ");
  return { pieceIndex, d };
});

export interface BrandSpinnerProps extends React.SVGProps<SVGSVGElement> {
  size?: number;
}

const BrandSpinner = React.forwardRef<SVGSVGElement, BrandSpinnerProps>(
  (
    {
      size = BRAND_SPINNER_SIZE_DEFAULT,
      strokeWidth = BRAND_SPINNER_STROKE_WIDTH_DEFAULT,
      className,
      ...props
    },
    ref,
  ) => (
    <svg
      ref={ref}
      data-slot="brand-spinner"
      role="status"
      aria-label="Loading"
      viewBox="-50.853 -50.853 337.707 337.707"
      fill="none"
      stroke="currentColor"
      strokeWidth={strokeWidth}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={cn("brand-spinner text-accent-strong", className)}
      {...props}
      width={size}
      height={size}
    >
      {BRAND_SPINNER_PIECES.map((piece) => (
        <path key={`track-${piece.pieceIndex}`} d={piece.d} className="brand-spinner__track" />
      ))}
      {BRAND_SPINNER_PIECES.map((piece) => (
        <path
          key={`trail-${piece.pieceIndex}`}
          d={piece.d}
          className="brand-spinner__trail"
          style={{ "--piece-index": piece.pieceIndex } as React.CSSProperties}
        />
      ))}
    </svg>
  ),
);
BrandSpinner.displayName = "BrandSpinner";

export { BrandSpinner };
