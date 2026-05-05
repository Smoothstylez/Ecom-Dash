import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  BUILT_IN_THEME_OPTIONS,
  CUSTOM_THEME_GROUPS,
  isHexColor,
  normalizeHexColor,
  readBaseThemeValues,
  type CustomThemeValues,
} from "@/shared/theme/custom-theme";
import { useDashboardShellState } from "@/app/dashboard-shell-state";
import { useTheme } from "@/shared/theme/theme-provider";

function copyThemeValues(values: CustomThemeValues | null) {
  if (!values || typeof values !== "object") {
    return null;
  }
  return { ...values } as CustomThemeValues;
}

function cloneThemeValues(values: CustomThemeValues | null) {
  return copyThemeValues(values) || {} as CustomThemeValues;
}

function buildCustomPreviewStyles(values: CustomThemeValues | null) {
  if (!values) {
    return {
      preview: undefined,
      bar: undefined,
      card: undefined,
    };
  }

  return {
    preview: {
      background: values["--th-page-bg"] || undefined,
    },
    bar: {
      background: `linear-gradient(90deg, ${values["--th-btn-primary-from"] || "#888"}, ${values["--th-btn-primary-to"] || "#666"})`,
    },
    card: {
      background: values["--th-card-from"] || undefined,
      border: `1px solid ${values["--th-card-border"] || "transparent"}`,
    },
  };
}

export function DashboardThemeModal() {
  const { requestCloseSettingsPanel, themeModalRequestToken } = useDashboardShellState();
  const { theme, customThemeValues, setTheme, setCustomThemeValues } = useTheme();
  const [isOpen, setIsOpen] = useState(false);
  const [isEditorOpen, setIsEditorOpen] = useState(false);
  const [baseTheme, setBaseTheme] = useState("");
  const [draftValues, setDraftValues] = useState<CustomThemeValues>(() => cloneThemeValues(customThemeValues));
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>(() => {
    return Object.fromEntries(CUSTOM_THEME_GROUPS.map(([groupLabel], index) => [groupLabel, index === 0]));
  });

  const isOpenRef = useRef(isOpen);
  const isEditorOpenRef = useRef(isEditorOpen);
  const previousThemeRef = useRef(theme);
  const previousCustomThemeValuesRef = useRef<CustomThemeValues | null>(copyThemeValues(customThemeValues));

  isOpenRef.current = isOpen;
  isEditorOpenRef.current = isEditorOpen;

  useEffect(() => {
    if (isOpen || isEditorOpen) {
      return;
    }
    setDraftValues(cloneThemeValues(customThemeValues));
  }, [customThemeValues, isEditorOpen, isOpen]);

  const customPreview = useMemo(() => buildCustomPreviewStyles(customThemeValues), [customThemeValues]);

  const cancelEditor = useCallback(() => {
    setCustomThemeValues(previousCustomThemeValuesRef.current, { persist: false });
    setTheme(previousThemeRef.current, { persist: false });
    setDraftValues(cloneThemeValues(previousCustomThemeValuesRef.current));
    setIsEditorOpen(false);
  }, [setCustomThemeValues, setTheme]);

  const closeModal = useCallback(() => {
    if (isEditorOpenRef.current) {
      cancelEditor();
      return;
    }
    setIsOpen(false);
  }, [cancelEditor]);

  const openModal = useCallback(() => {
    requestCloseSettingsPanel();
    setIsEditorOpen(false);
    setIsOpen(true);
  }, [requestCloseSettingsPanel]);

  const openEditor = useCallback(() => {
    const savedValues = cloneThemeValues(customThemeValues);
    const previousTheme = theme;
    const nextBaseTheme = previousTheme === "custom" ? "" : previousTheme;
    const nextDraftValues = Object.keys(savedValues).length ? savedValues : readBaseThemeValues(nextBaseTheme);

    previousThemeRef.current = previousTheme;
    previousCustomThemeValuesRef.current = copyThemeValues(customThemeValues);

    setBaseTheme(nextBaseTheme);
    setDraftValues(nextDraftValues);
    setCustomThemeValues(nextDraftValues, { persist: false });
    setTheme("custom", { persist: false });
    setIsOpen(true);
    setIsEditorOpen(true);
  }, [customThemeValues, setCustomThemeValues, setTheme, theme]);

  const saveEditor = useCallback(() => {
    const nextValues = cloneThemeValues(draftValues);
    previousThemeRef.current = "custom";
    previousCustomThemeValuesRef.current = copyThemeValues(nextValues);
    setCustomThemeValues(nextValues);
    setTheme("custom");
    setIsEditorOpen(false);
  }, [draftValues, setCustomThemeValues, setTheme]);

  const resetEditor = useCallback(() => {
    const nextDraftValues = readBaseThemeValues(baseTheme);
    setDraftValues(nextDraftValues);
    setCustomThemeValues(nextDraftValues, { persist: false });
    setTheme("custom", { persist: false });
  }, [baseTheme, setCustomThemeValues, setTheme]);

  const switchBaseTheme = useCallback((nextThemeId: string) => {
    const normalizedThemeId = String(nextThemeId || "").trim();
    const nextDraftValues = readBaseThemeValues(normalizedThemeId);
    setBaseTheme(normalizedThemeId);
    setDraftValues(nextDraftValues);
    setCustomThemeValues(nextDraftValues, { persist: false });
    setTheme("custom", { persist: false });
  }, [setCustomThemeValues, setTheme]);

  const updateDraftValue = useCallback((prop: string, value: string) => {
    setDraftValues((current) => {
      const nextValues = {
        ...current,
        [prop]: value,
      };
      setCustomThemeValues(nextValues, { persist: false });
      return nextValues;
    });
    setTheme("custom", { persist: false });
  }, [setCustomThemeValues, setTheme]);

  useEffect(() => {
    if (themeModalRequestToken === 0) {
      return;
    }
    openModal();
  }, [openModal, themeModalRequestToken]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || !isOpenRef.current) {
        return;
      }
      event.preventDefault();
      closeModal();
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [closeModal]);

  return (
    <div
      id="themeModal"
      className={isOpen ? "modal active" : "modal"}
      role="dialog"
      aria-modal="true"
      aria-hidden={isOpen ? "false" : "true"}
      data-react-owned="true"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          closeModal();
        }
      }}
    >
      <div className={isEditorOpen ? "modal-card theme-modal-card cte-active" : "modal-card theme-modal-card"}>
        <div className="modal-head">
          <div className="modal-title">Design waehlen</div>
          <button
            id="themeModalCloseBtn"
            className="modal-close"
            type="button"
            aria-label="Schliessen"
            onClick={() => {
              closeModal();
            }}
          >
            &times;
          </button>
        </div>
        <div className="modal-body theme-modal-body">
          <div id="themeGridView" style={{ display: isEditorOpen ? "none" : undefined }}>
            <div className="theme-grid">
              {BUILT_IN_THEME_OPTIONS.map((option) => (
                <button
                  key={option.id || "default"}
                  className={theme === option.id ? "theme-card active" : "theme-card"}
                  data-theme-id={option.id}
                  type="button"
                  onClick={() => {
                    setTheme(option.id);
                  }}
                >
                  <div className={`theme-preview ${option.previewClass}`}>
                    <div className="tp-bar" />
                    <div className="tp-content">
                      <div className="tp-card-mock" />
                      <div className="tp-card-mock" />
                    </div>
                  </div>
                  <div className="theme-name">{option.name}</div>
                  <div className="theme-desc">{option.description}</div>
                </button>
              ))}

              <button
                className={theme === "custom" ? "theme-card active" : "theme-card"}
                id="customThemeCard"
                type="button"
                onClick={() => {
                  openEditor();
                }}
              >
                <div className="theme-preview tp-custom" style={customPreview.preview}>
                  <div className="tp-bar" style={customPreview.bar} />
                  <div className="tp-content">
                    <div className="tp-card-mock" style={customPreview.card} />
                    <div className="tp-card-mock" style={customPreview.card} />
                  </div>
                </div>
                <div className="theme-name">Eigenes Design</div>
                <div className="theme-desc">Erstelle dein eigenes Theme</div>
              </button>
            </div>
          </div>

          <div id="customThemeEditor" className="cte" style={{ display: isEditorOpen ? undefined : "none" }}>
            <div className="cte-header">
              <button
                id="cteBackBtn"
                className="btn btn-soft"
                type="button"
                onClick={() => {
                  cancelEditor();
                }}
              >
                &larr; Zurueck
              </button>
              <div className="cte-title">Eigenes Design</div>
              <div className="cte-base-wrap">
                <label className="cte-base-label" htmlFor="cteBaseSelect">Basis</label>
                <select
                  id="cteBaseSelect"
                  className="cte-select"
                  value={baseTheme}
                  onChange={(event) => {
                    switchBaseTheme(event.target.value);
                  }}
                >
                  {BUILT_IN_THEME_OPTIONS.map((option) => (
                    <option key={option.id || "default"} value={option.id}>{option.name}</option>
                  ))}
                </select>
              </div>
            </div>
            <div id="cteBody" className="cte-body">
              {CUSTOM_THEME_GROUPS.map(([groupLabel, entries], groupIndex) => (
                <details
                  key={groupLabel}
                  className="cte-group"
                  open={Boolean(openGroups[groupLabel])}
                  onToggle={(event) => {
                    const nextOpen = event.currentTarget.open;
                    setOpenGroups((current) => {
                      if (current[groupLabel] === nextOpen) {
                        return current;
                      }
                      return {
                        ...current,
                        [groupLabel]: nextOpen,
                      };
                    });
                  }}
                >
                  <summary className="cte-group-title">
                    {groupLabel} <span className="cte-group-count">{entries.length}</span>
                  </summary>
                  <div className="cte-group-grid">
                    {entries.map(([prop, label]) => {
                      const value = draftValues[prop] || "";
                      const showColorInput = isHexColor(value);
                      const colorValue = showColorInput ? normalizeHexColor(value) : "#000000";
                      return (
                        <div key={prop} className="cte-field">
                          <label className="cte-label" htmlFor={`cte-field-${prop}`}>{label}</label>
                          {showColorInput ? (
                            <div className="cte-color-wrap">
                              <input
                                id={`cte-color-${prop}`}
                                className="cte-color"
                                data-prop={prop}
                                type="color"
                                value={colorValue}
                                onChange={(event) => {
                                  updateDraftValue(prop, event.target.value);
                                }}
                              />
                              <input
                                id={`cte-field-${prop}`}
                                className="cte-text cte-text-hex"
                                data-prop={prop}
                                spellCheck={false}
                                type="text"
                                value={value}
                                onChange={(event) => {
                                  updateDraftValue(prop, event.target.value);
                                }}
                              />
                            </div>
                          ) : (
                            <input
                              id={`cte-field-${prop}`}
                              className="cte-text"
                              data-prop={prop}
                              spellCheck={false}
                              type="text"
                              value={value}
                              onChange={(event) => {
                                updateDraftValue(prop, event.target.value);
                              }}
                            />
                          )}
                        </div>
                      );
                    })}
                  </div>
                </details>
              ))}
            </div>
            <div className="cte-footer">
              <button
                id="cteResetBtn"
                className="btn btn-soft"
                type="button"
                onClick={() => {
                  resetEditor();
                }}
              >
                Zuruecksetzen
              </button>
              <button
                id="cteSaveBtn"
                className="btn btn-primary"
                type="button"
                onClick={() => {
                  saveEditor();
                }}
              >
                Speichern
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
