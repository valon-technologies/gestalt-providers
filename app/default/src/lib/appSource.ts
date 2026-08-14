/**
 * Canonical app source is a browseable http(s) tree URL from the apps list
 * `sourceTreeUrl` field. Distinct from per-version `sourceUrl`, which points
 * at a published commit.
 */

const GITHUB_TREE_PATH =
  /^\/([^/]+)\/([^/]+)\/(?:tree|blob)\/[^/]+\/(.+)$/;

export function resolveAppSourceHref(sourceTreeUrl?: string): string | null {
  const trimmed = sourceTreeUrl?.trim();
  if (!trimmed) return null;
  try {
    const url = new URL(trimmed);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    if (!url.hostname) return null;
    return url.toString();
  } catch {
    return null;
  }
}

/** Compact label for Details: GitHub tree URLs drop `/tree/<ref>/`. */
export function formatAppSourceLabel(href: string): string {
  let url: URL;
  try {
    url = new URL(href);
  } catch {
    return href;
  }
  const pathname = url.pathname.replace(/\/+$/, "");
  const githubTree = pathname.match(GITHUB_TREE_PATH);
  if (isGitHubHost(url.hostname) && githubTree) {
    return `${decodePathLabel(githubTree[1])}/${decodePathLabel(githubTree[2])}/${decodePathLabel(githubTree[3])}`;
  }
  const path = pathname.replace(/^\//, "");
  return path ? `${url.hostname}/${decodePathLabel(path)}` : url.hostname;
}

function decodePathLabel(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function isGitHubHost(hostname: string): boolean {
  return hostname === "github.com" || hostname === "www.github.com";
}
