// ─────────────────────────────────────────────────────────────────────────────
// Best Grid Ever — Theme System
// Each theme ships with a `dark` variant. The grid resolves the right set of
// tokens automatically when `darkMode` is true.
// ─────────────────────────────────────────────────────────────────────────────

export interface GridThemeTokens {
  // ── Typography ──────────────────────────────────────────────────────────────
  fontFamily: string;
  fontSizeHeader: string;
  fontSizeCell: string;
  fontSizeMeta: string;
  fontSizeAction: string;
  fontWeightHeader: string;
  fontWeightCell: string;
  fontWeightMeta: string;
  fontWeightCellPrimary: string;

  // ── Surfaces ─────────────────────────────────────────────────────────────────
  colorBg: string;
  colorSurface: string;
  colorRowBg: string;
  colorRowHover: string;
  colorRowStripe: string;
  colorRowSelected: string;

  // ── Text ─────────────────────────────────────────────────────────────────────
  colorText: string;
  colorTextMuted: string;
  colorTextHeader: string;
  colorTextAction: string;
  colorTextActionHover: string;

  // ── Borders ───────────────────────────────────────────────────────────────────
  colorBorder: string;
  colorDivider: string;
  colorHeaderBorder: string;

  // ── Badges ────────────────────────────────────────────────────────────────────
  colorBadgeActiveBg: string;
  colorBadgeActiveText: string;
  colorBadgeInactiveBg: string;
  colorBadgeInactiveText: string;
  colorBadgePendingBg: string;
  colorBadgePendingText: string;

  // ── Sort / Filter ─────────────────────────────────────────────────────────────
  colorSortActive: string;
  colorSortInactive: string;
  colorFilterFocusBorder: string;
  colorFilterBg: string;
  colorFilterText: string;
  colorFilterPlaceholder: string;

  // ── Shape & Spacing ───────────────────────────────────────────────────────────
  radiusCard: string;
  radiusBadge: string;
  radiusButton: string;
  radiusInput: string;
  cellPaddingV: string;
  cellPaddingH: string;
  headerPaddingV: string;
  headerPaddingH: string;
  rowMinHeight: string;

  // ── Title bar ─────────────────────────────────────────────────────────────────
  colorTitleText: string;
  colorTitleCount: string;
  fontSizeTitle: string;
  fontWeightTitle: string;
  titlePaddingV: string;
  titlePaddingH: string;

  // ── Shadows ───────────────────────────────────────────────────────────────────
  shadowCard: string;
  shadowRow: string;

  // ── Pagination ────────────────────────────────────────────────────────────────
  colorPagBg: string;
  colorPagBgHover: string;
  colorPagBgActive: string;
  colorPagText: string;
  colorPagTextActive: string;
  colorPagBorder: string;

  // ── Danger ────────────────────────────────────────────────────────────────────
  colorDanger?: string;
  colorDangerHoverBg?: string;

  // ── Transitions ───────────────────────────────────────────────────────────────
  transitionSpeed: string;
  transitionEasing: string;
}

export interface GridTheme {
  /** Display name shown in theme picker */
  name: string;
  /** Light mode tokens */
  light: GridThemeTokens;
  /** Dark mode tokens */
  dark: GridThemeTokens;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────
const BASE_TYPE = {
  fontFamily: "'Inter', 'Geist', system-ui, sans-serif",
  fontSizeHeader: "0.6875rem",
  fontSizeCell: "0.875rem",
  fontSizeMeta: "0.75rem",
  fontSizeAction: "0.875rem",
  fontWeightHeader: "600",
  fontWeightCell: "400",
  fontWeightMeta: "400",
  fontWeightCellPrimary: "700",
  fontSizeTitle: "0.9375rem",
  fontWeightTitle: "600",
  titlePaddingV: "1rem",
  titlePaddingH: "1.25rem",
  cellPaddingV: "1.125rem",
  cellPaddingH: "1rem",
  headerPaddingV: "0.625rem",
  headerPaddingH: "1rem",
  rowMinHeight: "3rem",
  radiusCard: "0.75rem",
  radiusBadge: "9999px",
  radiusButton: "0.375rem",
  radiusInput: "0.5rem",
  shadowRow: "none",
  transitionSpeed: "150ms",
  transitionEasing: "cubic-bezier(0.16, 1, 0.3, 1)",
};

// ─────────────────────────────────────────────────────────────────────────────
// PERFECTION — crisp white light / deep charcoal dark
// ─────────────────────────────────────────────────────────────────────────────
export const PerfectionTheme: GridTheme = {
  name: "Perfection",
  light: {
    ...BASE_TYPE,
    colorBg: "#f0f0f0",
    colorSurface: "#ffffff",
    colorRowBg: "#ffffff",
    colorRowHover: "#f8f8f8",
    colorRowStripe: "#fafafa",
    colorRowSelected: "#edf7f7",
    colorText: "#1a1a1a",
    colorTextMuted: "#6b7280",
    colorTextHeader: "#6b7280",
    colorTextAction: "#374151",
    colorTextActionHover: "#111827",
    colorBorder: "#e5e7eb",
    colorDivider: "#f3f4f6",
    colorHeaderBorder: "#e5e7eb",
    colorBadgeActiveBg: "#d1f0ed",
    colorBadgeActiveText: "#0d7a6e",
    colorBadgeInactiveBg: "#f3f4f6",
    colorBadgeInactiveText: "#6b7280",
    colorBadgePendingBg: "#fef3c7",
    colorBadgePendingText: "#92400e",
    colorSortActive: "#111827",
    colorSortInactive: "#d1d5db",
    colorFilterFocusBorder: "#6ee7df",
    colorFilterBg: "#f9fafb",
    colorFilterText: "#111827",
    colorFilterPlaceholder: "#9ca3af",
    colorTitleText: "#111827",
    colorTitleCount: "#9ca3af",
    shadowCard: "0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)",
    colorPagBg: "#ffffff",
    colorPagBgHover: "#f3f4f6",
    colorPagBgActive: "#111827",
    colorPagText: "#374151",
    colorPagTextActive: "#ffffff",
    colorPagBorder: "#e5e7eb",
  },
  dark: {
    ...BASE_TYPE,
    colorBg: "#0f1117",
    colorSurface: "#1a1d27",
    colorRowBg: "#1a1d27",
    colorRowHover: "#1f2335",
    colorRowStripe: "#1c1f2e",
    colorRowSelected: "#1e2d3d",
    colorText: "#e2e8f0",
    colorTextMuted: "#718096",
    colorTextHeader: "#718096",
    colorTextAction: "#a0aec0",
    colorTextActionHover: "#e2e8f0",
    colorBorder: "#2d3748",
    colorDivider: "#232636",
    colorHeaderBorder: "#2d3748",
    colorBadgeActiveBg: "#1a3340",
    colorBadgeActiveText: "#4fd1c7",
    colorBadgeInactiveBg: "#2d3748",
    colorBadgeInactiveText: "#718096",
    colorBadgePendingBg: "#3d3117",
    colorBadgePendingText: "#f6ad55",
    colorSortActive: "#e2e8f0",
    colorSortInactive: "#4a5568",
    colorFilterFocusBorder: "#4fd1c7",
    colorFilterBg: "#141720",
    colorFilterText: "#e2e8f0",
    colorFilterPlaceholder: "#4a5568",
    colorTitleText: "#e2e8f0",
    colorTitleCount: "#4a5568",
    shadowCard: "0 4px 24px rgba(0,0,0,0.4)",
    colorPagBg: "#1a1d27",
    colorPagBgHover: "#232636",
    colorPagBgActive: "#4fd1c7",
    colorPagText: "#a0aec0",
    colorPagTextActive: "#0f1117",
    colorPagBorder: "#2d3748",
    colorDanger: "#f87171",
    colorDangerHoverBg: "#2d1a1a",
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// MIDNIGHT — already a dark theme; light counterpart is cool blue-white
// ─────────────────────────────────────────────────────────────────────────────
export const MidnightTheme: GridTheme = {
  name: "Midnight",
  // Midnight's "light" mode is a cool, slightly blue-tinted white
  light: {
    ...BASE_TYPE,
    colorBg: "#eef2f7",
    colorSurface: "#ffffff",
    colorRowBg: "#ffffff",
    colorRowHover: "#f0f4fa",
    colorRowStripe: "#f5f8fc",
    colorRowSelected: "#dbeafe",
    colorText: "#1e293b",
    colorTextMuted: "#64748b",
    colorTextHeader: "#475569",
    colorTextAction: "#334155",
    colorTextActionHover: "#0f172a",
    colorBorder: "#dde3ed",
    colorDivider: "#eef2f7",
    colorHeaderBorder: "#dde3ed",
    colorBadgeActiveBg: "#cffafe",
    colorBadgeActiveText: "#0e7490",
    colorBadgeInactiveBg: "#e2e8f0",
    colorBadgeInactiveText: "#64748b",
    colorBadgePendingBg: "#fef9c3",
    colorBadgePendingText: "#854d0e",
    colorSortActive: "#0f172a",
    colorSortInactive: "#cbd5e1",
    colorFilterFocusBorder: "#38bdf8",
    colorFilterBg: "#f5f8fc",
    colorFilterText: "#0f172a",
    colorFilterPlaceholder: "#94a3b8",
    colorTitleText: "#0f172a",
    colorTitleCount: "#94a3b8",
    shadowCard: "0 1px 3px rgba(15,23,42,0.08)",
    colorPagBg: "#ffffff",
    colorPagBgHover: "#eef2f7",
    colorPagBgActive: "#0e7490",
    colorPagText: "#334155",
    colorPagTextActive: "#ffffff",
    colorPagBorder: "#dde3ed",
  },
  dark: {
    ...BASE_TYPE,
    colorBg: "#0f1117",
    colorSurface: "#1a1d27",
    colorRowBg: "#1a1d27",
    colorRowHover: "#1f2335",
    colorRowStripe: "#1c1f2e",
    colorRowSelected: "#1e2d3d",
    colorText: "#e2e8f0",
    colorTextMuted: "#718096",
    colorTextHeader: "#718096",
    colorTextAction: "#a0aec0",
    colorTextActionHover: "#e2e8f0",
    colorBorder: "#2d3748",
    colorDivider: "#232636",
    colorHeaderBorder: "#2d3748",
    colorBadgeActiveBg: "#1a3340",
    colorBadgeActiveText: "#4fd1c7",
    colorBadgeInactiveBg: "#2d3748",
    colorBadgeInactiveText: "#718096",
    colorBadgePendingBg: "#3d3117",
    colorBadgePendingText: "#f6ad55",
    colorSortActive: "#e2e8f0",
    colorSortInactive: "#4a5568",
    colorFilterFocusBorder: "#4fd1c7",
    colorFilterBg: "#141720",
    colorFilterText: "#e2e8f0",
    colorFilterPlaceholder: "#4a5568",
    colorTitleText: "#e2e8f0",
    colorTitleCount: "#4a5568",
    shadowCard: "0 4px 24px rgba(0,0,0,0.4)",
    colorPagBg: "#1a1d27",
    colorPagBgHover: "#232636",
    colorPagBgActive: "#4fd1c7",
    colorPagText: "#a0aec0",
    colorPagTextActive: "#0f1117",
    colorPagBorder: "#2d3748",
    colorDanger: "#f87171",
    colorDangerHoverBg: "#2d1a1a",
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// SLATE — cool professional; dark is deep navy
// ─────────────────────────────────────────────────────────────────────────────
export const SlateTheme: GridTheme = {
  name: "Slate",
  light: {
    ...BASE_TYPE,
    fontFamily: "'Geist', 'Inter', system-ui, sans-serif",
    fontWeightHeader: "500",
    fontWeightCellPrimary: "600",
    radiusCard: "0.5rem",
    radiusBadge: "0.375rem",
    radiusButton: "0.375rem",
    radiusInput: "0.375rem",
    cellPaddingV: "1rem",
    cellPaddingH: "1rem",
    transitionSpeed: "120ms",
    transitionEasing: "ease-out",
    colorBg: "#f1f5f9",
    colorSurface: "#ffffff",
    colorRowBg: "#ffffff",
    colorRowHover: "#f8fafc",
    colorRowStripe: "#f8fafc",
    colorRowSelected: "#eff6ff",
    colorText: "#0f172a",
    colorTextMuted: "#64748b",
    colorTextHeader: "#475569",
    colorTextAction: "#334155",
    colorTextActionHover: "#0f172a",
    colorBorder: "#e2e8f0",
    colorDivider: "#f1f5f9",
    colorHeaderBorder: "#e2e8f0",
    colorBadgeActiveBg: "#dbeafe",
    colorBadgeActiveText: "#1d4ed8",
    colorBadgeInactiveBg: "#f1f5f9",
    colorBadgeInactiveText: "#64748b",
    colorBadgePendingBg: "#fef9c3",
    colorBadgePendingText: "#854d0e",
    colorSortActive: "#0f172a",
    colorSortInactive: "#cbd5e1",
    colorFilterFocusBorder: "#3b82f6",
    colorFilterBg: "#f8fafc",
    colorFilterText: "#0f172a",
    colorFilterPlaceholder: "#94a3b8",
    colorTitleText: "#0f172a",
    colorTitleCount: "#94a3b8",
    shadowCard: "0 0 0 1px #e2e8f0",
    colorPagBg: "#ffffff",
    colorPagBgHover: "#f1f5f9",
    colorPagBgActive: "#3b82f6",
    colorPagText: "#334155",
    colorPagTextActive: "#ffffff",
    colorPagBorder: "#e2e8f0",
  },
  dark: {
    ...BASE_TYPE,
    fontFamily: "'Geist', 'Inter', system-ui, sans-serif",
    fontWeightHeader: "500",
    fontWeightCellPrimary: "600",
    radiusCard: "0.5rem",
    radiusBadge: "0.375rem",
    radiusButton: "0.375rem",
    radiusInput: "0.375rem",
    cellPaddingV: "1rem",
    cellPaddingH: "1rem",
    transitionSpeed: "120ms",
    transitionEasing: "ease-out",
    colorBg: "#0d1117",
    colorSurface: "#161b22",
    colorRowBg: "#161b22",
    colorRowHover: "#1c2330",
    colorRowStripe: "#191e28",
    colorRowSelected: "#1a2744",
    colorText: "#e6edf3",
    colorTextMuted: "#7d8590",
    colorTextHeader: "#7d8590",
    colorTextAction: "#8b949e",
    colorTextActionHover: "#e6edf3",
    colorBorder: "#30363d",
    colorDivider: "#21262d",
    colorHeaderBorder: "#30363d",
    colorBadgeActiveBg: "#1f3058",
    colorBadgeActiveText: "#79c0ff",
    colorBadgeInactiveBg: "#21262d",
    colorBadgeInactiveText: "#7d8590",
    colorBadgePendingBg: "#3d2b00",
    colorBadgePendingText: "#e3b341",
    colorSortActive: "#e6edf3",
    colorSortInactive: "#30363d",
    colorFilterFocusBorder: "#388bfd",
    colorFilterBg: "#0d1117",
    colorFilterText: "#e6edf3",
    colorFilterPlaceholder: "#484f58",
    colorTitleText: "#e6edf3",
    colorTitleCount: "#484f58",
    shadowCard: "0 0 0 1px #30363d",
    colorPagBg: "#161b22",
    colorPagBgHover: "#1c2330",
    colorPagBgActive: "#388bfd",
    colorPagText: "#8b949e",
    colorPagTextActive: "#ffffff",
    colorPagBorder: "#30363d",
    colorDanger: "#f85149",
    colorDangerHoverBg: "#2d1b1b",
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// WARMTH — earthy light / deep espresso dark
// ─────────────────────────────────────────────────────────────────────────────
export const WarmthTheme: GridTheme = {
  name: "Warmth",
  light: {
    ...BASE_TYPE,
    fontFamily: "'Satoshi', 'Inter', system-ui, sans-serif",
    fontWeightCellPrimary: "600",
    fontWeightTitle: "700",
    radiusCard: "1rem",
    radiusButton: "0.5rem",
    radiusInput: "0.5rem",
    cellPaddingH: "1.125rem",
    headerPaddingV: "0.75rem",
    headerPaddingH: "1.125rem",
    rowMinHeight: "3.25rem",
    titlePaddingV: "1.125rem",
    colorBg: "#f5f0eb",
    colorSurface: "#fffdfb",
    colorRowBg: "#fffdfb",
    colorRowHover: "#fdf8f4",
    colorRowStripe: "#fdf7f2",
    colorRowSelected: "#fef3e7",
    colorText: "#2c1810",
    colorTextMuted: "#8a6c5c",
    colorTextHeader: "#8a6c5c",
    colorTextAction: "#5c3d2e",
    colorTextActionHover: "#2c1810",
    colorBorder: "#e8ddd4",
    colorDivider: "#f0e8e0",
    colorHeaderBorder: "#e8ddd4",
    colorBadgeActiveBg: "#dcf5e7",
    colorBadgeActiveText: "#166534",
    colorBadgeInactiveBg: "#f0e8e0",
    colorBadgeInactiveText: "#8a6c5c",
    colorBadgePendingBg: "#fef9c3",
    colorBadgePendingText: "#854d0e",
    colorSortActive: "#2c1810",
    colorSortInactive: "#d4c5bb",
    colorFilterFocusBorder: "#d97706",
    colorFilterBg: "#fdf8f4",
    colorFilterText: "#2c1810",
    colorFilterPlaceholder: "#b89b8a",
    colorTitleText: "#2c1810",
    colorTitleCount: "#b89b8a",
    shadowCard: "0 2px 8px rgba(100,50,20,0.08)",
    colorPagBg: "#fffdfb",
    colorPagBgHover: "#fdf0e8",
    colorPagBgActive: "#d97706",
    colorPagText: "#5c3d2e",
    colorPagTextActive: "#ffffff",
    colorPagBorder: "#e8ddd4",
  },
  dark: {
    ...BASE_TYPE,
    fontFamily: "'Satoshi', 'Inter', system-ui, sans-serif",
    fontWeightCellPrimary: "600",
    fontWeightTitle: "700",
    radiusCard: "1rem",
    radiusButton: "0.5rem",
    radiusInput: "0.5rem",
    cellPaddingH: "1.125rem",
    headerPaddingV: "0.75rem",
    headerPaddingH: "1.125rem",
    rowMinHeight: "3.25rem",
    titlePaddingV: "1.125rem",
    colorBg: "#1a0f08",
    colorSurface: "#241510",
    colorRowBg: "#241510",
    colorRowHover: "#2e1c14",
    colorRowStripe: "#291812",
    colorRowSelected: "#3d2510",
    colorText: "#f5e6d8",
    colorTextMuted: "#a08070",
    colorTextHeader: "#a08070",
    colorTextAction: "#c4a090",
    colorTextActionHover: "#f5e6d8",
    colorBorder: "#3d2820",
    colorDivider: "#2e1c14",
    colorHeaderBorder: "#3d2820",
    colorBadgeActiveBg: "#1a3020",
    colorBadgeActiveText: "#6ee7a0",
    colorBadgeInactiveBg: "#2e1c14",
    colorBadgeInactiveText: "#a08070",
    colorBadgePendingBg: "#3d2d00",
    colorBadgePendingText: "#fbbf24",
    colorSortActive: "#f5e6d8",
    colorSortInactive: "#4a3028",
    colorFilterFocusBorder: "#f59e0b",
    colorFilterBg: "#1a0f08",
    colorFilterText: "#f5e6d8",
    colorFilterPlaceholder: "#6b4a3a",
    colorTitleText: "#f5e6d8",
    colorTitleCount: "#6b4a3a",
    shadowCard: "0 4px 24px rgba(0,0,0,0.5)",
    colorPagBg: "#241510",
    colorPagBgHover: "#2e1c14",
    colorPagBgActive: "#f59e0b",
    colorPagText: "#c4a090",
    colorPagTextActive: "#1a0f08",
    colorPagBorder: "#3d2820",
    colorDanger: "#fca5a5",
    colorDangerHoverBg: "#3d1a1a",
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// 50 SHADES — nothing but grey, light and dark
// ─────────────────────────────────────────────────────────────────────────────
export const FiftyShadesTheme: GridTheme = {
  name: "50 Shades",
  light: {
    ...BASE_TYPE,
    radiusCard: "0.5rem",
    radiusBadge: "0.25rem",
    radiusButton: "0.25rem",
    radiusInput: "0.375rem",
    colorBg: "#c8c8c8",
    colorSurface: "#f5f5f5",
    colorRowBg: "#f5f5f5",
    colorRowHover: "#ebebeb",
    colorRowStripe: "#f0f0f0",
    colorRowSelected: "#dcdcdc",
    colorText: "#1a1a1a",
    colorTextMuted: "#707070",
    colorTextHeader: "#888888",
    colorTextAction: "#505050",
    colorTextActionHover: "#1a1a1a",
    colorBorder: "#d4d4d4",
    colorDivider: "#e8e8e8",
    colorHeaderBorder: "#d8d8d8",
    colorBadgeActiveBg: "#e0e0e0",
    colorBadgeActiveText: "#2a2a2a",
    colorBadgeInactiveBg: "#ececec",
    colorBadgeInactiveText: "#909090",
    colorBadgePendingBg: "#d6d6d6",
    colorBadgePendingText: "#3a3a3a",
    colorSortActive: "#1a1a1a",
    colorSortInactive: "#cccccc",
    colorFilterFocusBorder: "#808080",
    colorFilterBg: "#f0f0f0",
    colorFilterText: "#1a1a1a",
    colorFilterPlaceholder: "#aaaaaa",
    colorTitleText: "#1a1a1a",
    colorTitleCount: "#b0b0b0",
    shadowCard: "0 1px 4px rgba(0,0,0,0.10)",
    colorPagBg: "#f5f5f5",
    colorPagBgHover: "#e8e8e8",
    colorPagBgActive: "#3a3a3a",
    colorPagText: "#505050",
    colorPagTextActive: "#f5f5f5",
    colorPagBorder: "#d4d4d4",
    colorDanger: "#505050",
    colorDangerHoverBg: "#e0e0e0",
  },
  dark: {
    ...BASE_TYPE,
    radiusCard: "0.5rem",
    radiusBadge: "0.25rem",
    radiusButton: "0.25rem",
    radiusInput: "0.375rem",
    colorBg: "#141414",
    colorSurface: "#1e1e1e",
    colorRowBg: "#1e1e1e",
    colorRowHover: "#282828",
    colorRowStripe: "#222222",
    colorRowSelected: "#333333",
    colorText: "#e8e8e8",
    colorTextMuted: "#888888",
    colorTextHeader: "#666666",
    colorTextAction: "#aaaaaa",
    colorTextActionHover: "#e8e8e8",
    colorBorder: "#333333",
    colorDivider: "#2a2a2a",
    colorHeaderBorder: "#333333",
    colorBadgeActiveBg: "#2e2e2e",
    colorBadgeActiveText: "#cccccc",
    colorBadgeInactiveBg: "#252525",
    colorBadgeInactiveText: "#666666",
    colorBadgePendingBg: "#2a2a2a",
    colorBadgePendingText: "#a0a0a0",
    colorSortActive: "#e8e8e8",
    colorSortInactive: "#404040",
    colorFilterFocusBorder: "#707070",
    colorFilterBg: "#141414",
    colorFilterText: "#e8e8e8",
    colorFilterPlaceholder: "#555555",
    colorTitleText: "#e8e8e8",
    colorTitleCount: "#555555",
    shadowCard: "0 4px 20px rgba(0,0,0,0.5)",
    colorPagBg: "#1e1e1e",
    colorPagBgHover: "#282828",
    colorPagBgActive: "#c0c0c0",
    colorPagText: "#888888",
    colorPagTextActive: "#141414",
    colorPagBorder: "#333333",
    colorDanger: "#aaaaaa",
    colorDangerHoverBg: "#2e2e2e",
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// Theme registry
// ─────────────────────────────────────────────────────────────────────────────
export const THEMES: Record<string, GridTheme> = {
  perfection:  PerfectionTheme,
  midnight:    MidnightTheme,
  slate:       SlateTheme,
  warmth:      WarmthTheme,
  fiftyshades: FiftyShadesTheme,
};

/** Resolve tokens for the current mode */
export function resolveTokens(theme: GridTheme, dark: boolean): GridThemeTokens {
  return dark ? theme.dark : theme.light;
}

/** Convert resolved tokens into CSS custom properties for the grid root */
export function themeToCSS(tokens: GridThemeTokens): React.CSSProperties {
  return {
    "--bg-grid-font-family":         tokens.fontFamily,
    "--bg-font-size-header":         tokens.fontSizeHeader,
    "--bg-font-size-cell":           tokens.fontSizeCell,
    "--bg-font-size-meta":           tokens.fontSizeMeta,
    "--bg-font-size-action":         tokens.fontSizeAction,
    "--bg-font-weight-header":       tokens.fontWeightHeader,
    "--bg-font-weight-cell":         tokens.fontWeightCell,
    "--bg-font-weight-meta":         tokens.fontWeightMeta,
    "--bg-font-weight-cell-primary": tokens.fontWeightCellPrimary,
    "--bg-color-bg":                 tokens.colorBg,
    "--bg-color-surface":            tokens.colorSurface,
    "--bg-color-row-bg":             tokens.colorRowBg,
    "--bg-color-row-hover":          tokens.colorRowHover,
    "--bg-color-row-stripe":         tokens.colorRowStripe,
    "--bg-color-row-selected":       tokens.colorRowSelected,
    "--bg-color-text":               tokens.colorText,
    "--bg-color-text-muted":         tokens.colorTextMuted,
    "--bg-color-text-header":        tokens.colorTextHeader,
    "--bg-color-text-action":        tokens.colorTextAction,
    "--bg-color-text-action-hover":  tokens.colorTextActionHover,
    "--bg-color-border":             tokens.colorBorder,
    "--bg-color-divider":            tokens.colorDivider,
    "--bg-color-header-border":      tokens.colorHeaderBorder,
    "--bg-color-badge-active-bg":    tokens.colorBadgeActiveBg,
    "--bg-color-badge-active-text":  tokens.colorBadgeActiveText,
    "--bg-color-badge-inactive-bg":  tokens.colorBadgeInactiveBg,
    "--bg-color-badge-inactive-text":tokens.colorBadgeInactiveText,
    "--bg-color-badge-pending-bg":   tokens.colorBadgePendingBg,
    "--bg-color-badge-pending-text": tokens.colorBadgePendingText,
    "--bg-color-sort-active":        tokens.colorSortActive,
    "--bg-color-sort-inactive":      tokens.colorSortInactive,
    "--bg-color-filter-focus-border":tokens.colorFilterFocusBorder,
    "--bg-color-filter-bg":          tokens.colorFilterBg,
    "--bg-color-filter-text":        tokens.colorFilterText,
    "--bg-color-filter-placeholder": tokens.colorFilterPlaceholder,
    "--bg-radius-card":              tokens.radiusCard,
    "--bg-radius-badge":             tokens.radiusBadge,
    "--bg-radius-button":            tokens.radiusButton,
    "--bg-radius-input":             tokens.radiusInput,
    "--bg-cell-padding-v":           tokens.cellPaddingV,
    "--bg-cell-padding-h":           tokens.cellPaddingH,
    "--bg-header-padding-v":         tokens.headerPaddingV,
    "--bg-header-padding-h":         tokens.headerPaddingH,
    "--bg-row-min-height":           tokens.rowMinHeight,
    "--bg-color-title-text":         tokens.colorTitleText,
    "--bg-color-title-count":        tokens.colorTitleCount,
    "--bg-font-size-title":          tokens.fontSizeTitle,
    "--bg-font-weight-title":        tokens.fontWeightTitle,
    "--bg-title-padding-v":          tokens.titlePaddingV,
    "--bg-title-padding-h":          tokens.titlePaddingH,
    "--bg-shadow-card":              tokens.shadowCard,
    "--bg-shadow-row":               tokens.shadowRow,
    "--bg-color-pag-bg":             tokens.colorPagBg,
    "--bg-color-pag-bg-hover":       tokens.colorPagBgHover,
    "--bg-color-pag-bg-active":      tokens.colorPagBgActive,
    "--bg-color-pag-text":           tokens.colorPagText,
    "--bg-color-pag-text-active":    tokens.colorPagTextActive,
    "--bg-color-pag-border":         tokens.colorPagBorder,
    "--bg-color-danger":             tokens.colorDanger ?? "#dc2626",
    "--bg-color-danger-hover-bg":    tokens.colorDangerHoverBg ?? "#fef2f2",
    "--bg-transition-speed":         tokens.transitionSpeed,
    "--bg-transition-easing":        tokens.transitionEasing,
  } as React.CSSProperties;
}
