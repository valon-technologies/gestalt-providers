import type { Integration } from "@/lib/api";
import { cn } from "@/lib/cn";
import { ghostQuietChromePaintClassName } from "@/lib/press-feedback";
import { setupSeeMorePreview } from "@/lib/setupSeeMore";
import IntegrationIcon from "@/components/IntegrationIcon";

/**
 * ChatGPT plugins-store overflow row: stacked app marks + “See A, B, and more”.
 * Click appends the next page of apps in place.
 */
export default function SeeMoreAppsTrigger({
  remaining,
  onSeeMore,
}: {
  remaining: Integration[];
  onSeeMore: () => void;
}) {
  if (remaining.length === 0) return null;

  const { icons, label } = setupSeeMorePreview(remaining);

  return (
    <button
      type="button"
      data-testid="build-see-more-apps"
      onClick={onSeeMore}
      className={cn(
        "flex w-fit max-w-full items-center gap-3 rounded-md py-1 text-left text-sm",
        ghostQuietChromePaintClassName,
        "focus-ring",
      )}
    >
      <span className="flex shrink-0 items-center" aria-hidden>
        {icons.map((integration, index) => (
          <span
            key={integration.name}
            className={cn(
              "relative inline-flex rounded-lg ring-2 ring-background",
              index > 0 && "-ml-2",
            )}
            style={{ zIndex: index + 1 }}
          >
            <IntegrationIcon
              iconSvg={integration.iconSvg}
              name={integration.name}
              displayName={integration.displayName}
              size="sm"
            />
          </span>
        ))}
      </span>
      <span className="min-w-0 text-pretty">{label}</span>
    </button>
  );
}
