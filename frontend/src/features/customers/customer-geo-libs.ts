const loadedScripts = new Map<string, Promise<void>>();

const LEAFLET_SCRIPT_URL = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
const TOPOJSON_SCRIPT_URL = "https://unpkg.com/topojson-client@3";
const GLOBE_SCRIPT_URL = "https://cdn.jsdelivr.net/npm/globe.gl@2.41.6/dist/globe.gl.min.js";

type CustomerGeoRuntimeWindow = Window & {
  L?: unknown;
  Globe?: unknown;
  topojson?: {
    feature?: unknown;
  };
};

function loadExternalScript(src: string) {
  const existingPromise = loadedScripts.get(src);
  if (existingPromise) {
    return existingPromise;
  }

  const nextPromise = new Promise<void>((resolve, reject) => {
    const existingScript = document.querySelector(`script[src="${src}"]`);
    if (existingScript instanceof HTMLScriptElement) {
      if (existingScript.dataset.loaded === "true") {
        resolve();
        return;
      }
      existingScript.addEventListener("load", () => resolve(), { once: true });
      existingScript.addEventListener("error", () => reject(new Error(`Script failed: ${src}`)), { once: true });
      return;
    }

    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.addEventListener("load", () => {
      script.dataset.loaded = "true";
      resolve();
    }, { once: true });
    script.addEventListener("error", () => reject(new Error(`Script failed: ${src}`)), { once: true });
    document.body.appendChild(script);
  });

  const trackedPromise = nextPromise.catch((error) => {
    loadedScripts.delete(src);
    throw error;
  });

  loadedScripts.set(src, trackedPromise);
  return trackedPromise;
}

export async function loadCustomerMapLibrary() {
  const runtimeWindow = window as CustomerGeoRuntimeWindow;
  if (runtimeWindow.L) {
    return;
  }

  await loadExternalScript(LEAFLET_SCRIPT_URL);
  if (!runtimeWindow.L) {
    throw new Error("Leaflet konnte nicht geladen werden.");
  }
}

export async function loadCustomerGlobeLibraries() {
  const runtimeWindow = window as CustomerGeoRuntimeWindow;
  const tasks: Promise<void>[] = [];

  if (!runtimeWindow.topojson?.feature) {
    tasks.push(loadExternalScript(TOPOJSON_SCRIPT_URL));
  }
  if (typeof runtimeWindow.Globe !== "function") {
    tasks.push(loadExternalScript(GLOBE_SCRIPT_URL));
  }

  if (tasks.length) {
    await Promise.all(tasks);
  }

  if (typeof runtimeWindow.Globe !== "function" || !runtimeWindow.topojson?.feature) {
    throw new Error("Hex-Globus konnte nicht geladen werden.");
  }
}
