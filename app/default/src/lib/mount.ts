function moduleEntryURL(): URL | null {
  if (typeof window === "undefined" || typeof document === "undefined") {
    return null;
  }

  const scripts = Array.from(
    document.querySelectorAll<HTMLScriptElement>('script[type="module"][src]'),
  );
  const entry = scripts.find((script) => {
    try {
      return new URL(script.src, window.location.href).pathname.includes("/assets/");
    } catch {
      return false;
    }
  }) ?? scripts.at(-1);

  return entry ? new URL(entry.src, window.location.href) : null;
}

function viteDevBasepath(): string | null {
  const base = import.meta.env.BASE_URL;
  if (!base.startsWith("/")) {
    return null;
  }
  return base === "/" ? "/" : base.replace(/\/+$/, "");
}

function detectAppBasepath(): string {
  const devBasepath = viteDevBasepath();
  if (devBasepath) {
    return devBasepath;
  }

  const entryURL = moduleEntryURL();
  if (!entryURL) {
    return "/";
  }

  const assetsIndex = entryURL.pathname.lastIndexOf("/assets/");
  if (assetsIndex <= 0) {
    return "/";
  }

  return entryURL.pathname.slice(0, assetsIndex);
}

// Vite emits the application entry below <mount>/assets/ in production. The
// mount is therefore discoverable without a tenant-specific build variable.
// Native Gestaltd development supplies the Vite base under its mounted path.
export const appBasepath = detectAppBasepath();

export function appPath(path: string): string {
  if (!path.startsWith("/")) {
    throw new Error(`appPath requires an absolute app path, received ${path}`);
  }
  return appBasepath === "/" ? path : `${appBasepath}${path}`;
}
