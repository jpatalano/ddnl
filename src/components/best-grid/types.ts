// ─────────────────────────────────────────────────────────────────────────────
// Best Grid Ever — Core Types
// ─────────────────────────────────────────────────────────────────────────────

import { GridTheme } from "./themes";

export type SortDirection = "asc" | "desc" | null;

export type StatusVariant = "active" | "inactive" | "pending" | string;

// ── Column filter types ──────────────────────────────────────────────────────

/** How a column's values should be filtered */
export type ColumnFilterType =
  | "text"        // free-text substring match
  | "number"      // numeric range (min/max)
  | "select"      // multi-select from enumerated values
  | "date"        // smart date parsing (before/after/between/natural lang)
  | "boolean";    // true/false toggle

/** The active filter value for a single column */
export type ColumnFilterValue =
  | { type: "text";    value: string }
  | { type: "number";  min?: number; max?: number }
  | { type: "select";  values: string[] }   // OR logic across selections
  | { type: "date";    from?: string; to?: string; raw?: string }  // ISO dates or natural text
  | { type: "boolean"; value: boolean | null };  // null = no filter

/** A saved named view = columns config + active column filters */
export interface SavedView {
  id: string;
  name: string;
  columnFilters: Record<string, ColumnFilterValue>;
  globalFilter: string;
  sortKey: string | null;
  sortDir: SortDirection;
}

// ── Column Definition ────────────────────────────────────────────────────────

export interface ColumnDef<TRow = Record<string, unknown>> {
  /** Unique column identifier */
  key: string;

  /** Display header label (can be a React node) */
  header: React.ReactNode;

  /**
   * How to pull the value from a row for sorting / filtering.
   * Defaults to `row[key]` if not provided.
   */
  accessor?: (row: TRow) => unknown;

  /**
   * Custom cell renderer. Receives the row and the resolved value.
   * If omitted, renders the value as a string.
   */
  cell?: (row: TRow, value: unknown) => React.ReactNode;

  /** Enable column sorting. Default: true */
  sortable?: boolean;

  /** Whether this column participates in global text filtering. Default: true */
  filterable?: boolean;

  /** 
   * The filter type for this column's per-column filter UI.
   * Auto-detected from column data if not provided.
   */
  filterType?: ColumnFilterType;

  /** 
   * Explicit set of options for "select" filter type.
   * If not provided and filterType=select, options are auto-derived from data.
   */
  filterOptions?: string[];

  /** Tailwind / CSS class applied to both th and td */
  className?: string;

  /** Min-width applied to the column via inline style */
  minWidth?: string;

  /** Alignment — default "left" */
  align?: "left" | "center" | "right";

  /** If true, column is hidden by default (but can be toggled on) */
  hidden?: boolean;

  /** Pin column to the left (future: right) */
  pin?: "left" | "right";
}

// ── Action Definition ────────────────────────────────────────────────────────

export interface ActionDef<TRow = Record<string, unknown>> {
  /** Unique key */
  key: string;
  /** Label text */
  label: string;
  /** Optional icon (Lucide or any React node) */
  icon?: React.ReactNode;
  /** Called when the action is triggered */
  onClick: (row: TRow) => void;
  /** Conditionally show/hide action per row */
  visible?: (row: TRow) => boolean;
  /** Conditionally disable action per row */
  disabled?: (row: TRow) => boolean;
  /** Visual variant — default "ghost" */
  variant?: "ghost" | "danger" | "primary";
}

// ── BestGrid Props ───────────────────────────────────────────────────────────

export interface BestGridProps<TRow = Record<string, unknown>> {
  /** The dataset to display */
  data: TRow[];

  /** Column definitions (ordered) */
  columns: ColumnDef<TRow>[];

  /** Row actions rendered in the ACTIONS column. If omitted, no actions column shown. */
  actions?: ActionDef<TRow>[];

  /** A unique key per row (used for selection). Defaults to row index. */
  rowKey?: keyof TRow | ((row: TRow) => string | number);

  // ── Title bar ──────────────────────────────────────────────────────────────
  /** Title shown above the table (e.g. "All Investigators") */
  title?: string;

  /** Whether to show the row count badge next to the title */
  showCount?: boolean;

  // ── Features ──────────────────────────────────────────────────────────────
  /** Enable global search / filter bar */
  filterable?: boolean;

  /** Placeholder for the filter input */
  filterPlaceholder?: string;

  /** Enable per-column filter dropdowns (funnel icon on each header) */
  columnFilters?: boolean;

  /** Enable column sorting */
  sortable?: boolean;

  /** Enable row selection checkboxes */
  selectable?: boolean;

  /** Enable alternating row stripe */
  striped?: boolean;

  /** Enable row hover highlight */
  hoverable?: boolean;

  /** Enable column visibility toggling */
  columnToggle?: boolean;

  // ── Pagination ─────────────────────────────────────────────────────────────
  /** Paginate the results. Set to false to disable. Default: true */
  paginate?: boolean;

  /** Page sizes available in the selector */
  pageSizes?: number[];

  /** Default page size */
  defaultPageSize?: number;

  // ── Virtual Scroll ─────────────────────────────────────────────────────────
  /** Use virtual scrolling instead of pagination. Set maxHeight for the scroll container. */
  virtualScroll?: boolean;

  /** Height of the virtual scroll container (default: "600px") */
  virtualScrollHeight?: string;

  // ── Selection callbacks ───────────────────────────────────────────────────
  onSelectionChange?: (selectedRows: TRow[]) => void;

  // ── Empty state ───────────────────────────────────────────────────────────
  emptyMessage?: string;

  // ── Theming ───────────────────────────────────────────────────────────────
  /** Pass a GridTheme object to style the grid */
  theme?: GridTheme;

  /** Force dark mode (overrides OS preference) */
  darkMode?: boolean;

  /** Extra className on the outermost wrapper */
  className?: string;

  /** Extra inline style on the outermost wrapper */
  style?: React.CSSProperties;
}
