import { createContext, useCallback, useContext, useEffect, useState, type PropsWithChildren } from "react";

import {
  CUSTOM_THEME_STORAGE_KEY,
  injectCustomThemeStyle,
  persistCustomTheme,
  readStoredCustomTheme,
  type CustomThemeValues,
} from "@/shared/theme/custom-theme";

const THEME_STORAGE_KEY = "dash-combined.theme";

type ThemeUpdateOptions = {
  persist?: boolean;
};

type ThemeContextValue = {
  theme: string;
  customThemeValues: CustomThemeValues | null;
  themeVersion: number;
  setTheme: (nextTheme: string, options?: ThemeUpdateOptions) => void;
  setCustomThemeValues: (nextValues: CustomThemeValues | null, options?: ThemeUpdateOptions) => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

function cloneCustomThemeValues(values: CustomThemeValues | null) {
  if (!values || typeof values !== "object") {
    return null;
  }
  return { ...values };
}

function readCurrentTheme() {
  const attributeTheme = document.documentElement.getAttribute("data-theme");
  if (attributeTheme) {
    return attributeTheme;
  }

  try {
    return localStorage.getItem(THEME_STORAGE_KEY) || "";
  } catch (_error) {
    return "";
  }
}

function persistTheme(theme: string) {
  try {
    if (theme) {
      localStorage.setItem(THEME_STORAGE_KEY, theme);
    } else {
      localStorage.removeItem(THEME_STORAGE_KEY);
    }
  } catch (_error) {
    // Ignore localStorage write failures.
  }
}

function applyTheme(theme: string) {
  if (theme) {
    document.documentElement.setAttribute("data-theme", theme);
  } else {
    document.documentElement.removeAttribute("data-theme");
  }
}

export function ThemeProvider({ children }: PropsWithChildren) {
  const [theme, setThemeState] = useState(() => readCurrentTheme());
  const [customThemeValues, setCustomThemeValuesState] = useState<CustomThemeValues | null>(() => {
    return cloneCustomThemeValues(readStoredCustomTheme());
  });
  const [themeVersion, setThemeVersion] = useState(0);

  const setTheme = useCallback((nextTheme: string, options?: ThemeUpdateOptions) => {
    const normalizedTheme = String(nextTheme || "").trim();
    setThemeState(normalizedTheme);
    if (options?.persist !== false) {
      persistTheme(normalizedTheme);
    }
  }, []);

  const setCustomThemeValues = useCallback((nextValues: CustomThemeValues | null, options?: ThemeUpdateOptions) => {
    const normalizedValues = cloneCustomThemeValues(nextValues);
    setCustomThemeValuesState(normalizedValues);
    if (options?.persist !== false) {
      persistCustomTheme(normalizedValues);
    }
  }, []);

  useEffect(() => {
    injectCustomThemeStyle(customThemeValues);
    applyTheme(theme);
    setThemeVersion((current) => current + 1);
  }, [customThemeValues, theme]);

  useEffect(() => {
    const documentElement = document.documentElement;
    const observer = new MutationObserver(() => {
      const nextTheme = documentElement.getAttribute("data-theme") || "";
      setThemeState((currentTheme) => (currentTheme === nextTheme ? currentTheme : nextTheme));
    });

    const handleStorage = (event: StorageEvent) => {
      if (event.key === THEME_STORAGE_KEY) {
        const nextTheme = event.newValue || "";
        setThemeState((currentTheme) => (currentTheme === nextTheme ? currentTheme : nextTheme));
        return;
      }

      if (event.key !== CUSTOM_THEME_STORAGE_KEY) {
        return;
      }

      const nextValues = cloneCustomThemeValues(readStoredCustomTheme());
      setCustomThemeValuesState(nextValues);
    };

    observer.observe(documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });
    window.addEventListener("storage", handleStorage);

    return () => {
      observer.disconnect();
      window.removeEventListener("storage", handleStorage);
    };
  }, []);

  return (
    <ThemeContext.Provider
      value={{
        theme,
        customThemeValues,
        themeVersion,
        setTheme,
        setCustomThemeValues,
      }}
    >
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error("useTheme must be used inside ThemeProvider.");
  }
  return context;
}
