// ─────────────────────────────────────────────────────────────────────────────
// Best Grid Ever — Main Component
// Features: global filter, per-column smart filters, dark mode, virtual scroll
// ─────────────────────────────────────────────────────────────────────────────

import {
  useState, useMemo, useCallback, useRef, useEffect, CSSProperties,
} from "react";
import {
  ChevronUp, ChevronDown, ChevronsUpDown,
  Search, X, ChevronLeft, ChevronRight,
  Columns3, Check, Filter, FilterX, Sun, Moon,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  BestGridProps, ColumnDef, SortDirection,
  ColumnFilterValue, ColumnFilterType,
} from "./types";
import { PerfectionTheme, resolveTokens, themeToCSS } from "./themes";

// ─────────────────────────────────────────────────────────────────────────────
// Date parsing utilities
// ─────────────────────────────────────────────────────────────────────────────
function parseNaturalDate(input: string): Date | null {
  const s = input.trim().toLowerCase();
  if (!s) return null;

  const now = new Date();

  // natural keywords
  if (s === "today") return new Date(now.getFullYear(), now.getMonth(), now.getDate());
  if (s === "yesterday") {
    const d = new Date(now);
    d.setDate(d.getDate() - 1);
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }
  if (s === "this week") {
    const d = new Date(now);
    d.setDate(d.getDate() - d.getDay());
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }
  if (s === "last week") {
    const d = new Date(now);
    d.setDate(d.getDate() - d.getDay() - 7);
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }
  if (s === "this month") return new Date(now.getFullYear(), now.getMonth(), 1);
  if (s === "last month") return new Date(now.getFullYear(), now.getMonth() - 1, 1);
  if (s === "this year")  return new Date(now.getFullYear(), 0, 1);
  if (s === "last year")  return new Date(now.getFullYear() - 1, 0, 1);

  // "N days ago"
  const daysAgo = s.match(/^(\d+)\s+days?\s+ago$/);
  if (daysAgo) {
    const d = new Date(now);
    d.setDate(d.getDate() - parseInt(daysAgo[1]));
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }

  // "N months ago"
  const monthsAgo = s.match(/^(\d+)\s+months?\s+ago$/);
  if (monthsAgo) {
    const d = new Date(now);
    d.setMonth(d.getMonth() - parseInt(monthsAgo[1]));
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }

  // specific month name + year: "jan 2024"
  const monthYear = s.match(/^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+(\d{4})$/);
  if (monthYear) {
    const months: Record<string, number> = { jan:0, feb:1, mar:2, apr:3, may:4, jun:5, jul:6, aug:7, sep:8, oct:9, nov:10, dec:11 };
    return new Date(parseInt(monthYear[2]), months[monthYear[1]], 1);
  }

  // try native date parse as fallback
  const d = new Date(input);
  return isNaN(d.getTime()) ? null : d;
}

/** Normalize a cell value to a Date for comparison */
function cellToDate(val: unknown): Date | null {
  if (val instanceof Date) return val;
  if (typeof val === "number") return new Date(val);
  if (typeof val === "string") {
    const d = new Date(val);
    return isNaN(d.getTime()) ? null : d;
  }
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter matching
// ─────────────────────────────────────────────────────────────────────────────
function matchesColumnFilter(val: unknown, filter: ColumnFilterValue): boolean {
  if (!filter) return true;

  switch (filter.type) {
    case "text": {
      if (!filter.value) return true;
      return String(val ?? "").toLowerCase().includes(filter.value.toLowerCase());
    }
    case "number": {
      const n = typeof val === "number" ? val : parseFloat(String(val ?? ""));
      if (isNaN(n)) return true;
      if (filter.min !== undefined && n < filter.min) return false;
      if (filter.max !== undefined && n > filter.max) return false;
      return true;
    }
    case "select": {
      if (!filter.values || filter.values.length === 0) return true;
      const s = String(val ?? "").toUpperCase();
      return filter.values.some(v => s === v.toUpperCase() || String(val ?? "").toLowerCase().includes(v.toLowerCase()));
    }
    case "date": {
      const cellDate = cellToDate(val);
      if (!cellDate) return true;

      // If there's raw text, parse it as a "from" date
      if (filter.raw && !filter.from && !filter.to) {
        const parsed = parseNaturalDate(filter.raw);
        if (!parsed) return true;
        // match same day/month/year depending on specificity
        const raw = filter.raw.trim().toLowerCase();
        if (raw.match(/^\d{4}$/) || raw.includes("year")) {
          return cellDate.getFullYear() === parsed.getFullYear();
        }
        if (raw.match(/^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/)) {
          return cellDate.getFullYear() === parsed.getFullYear() &&
                 cellDate.getMonth() === parsed.getMonth();
        }
        // default: on or after
        return cellDate >= parsed;
      }
      if (filter.from) {
        const from = parseNaturalDate(filter.from);
        if (from && cellDate < from) return false;
      }
      if (filter.to) {
        const to = parseNaturalDate(filter.to);
        if (to) {
          // inclusive: end of that day
          const endOfTo = new Date(to);
          endOfTo.setDate(endOfTo.getDate() + 1);
          if (cellDate >= endOfTo) return false;
        }
      }
      return true;
    }
    case "boolean": {
      if (filter.value === null || filter.value === undefined) return true;
      return Boolean(val) === filter.value;
    }
    default:
      return true;
  }
}

/** Auto-detect filter type from column data */
function detectFilterType(col: ColumnDef<any>, data: any[]): ColumnFilterType {
  if (col.filterType) return col.filterType;
  // Sniff first non-null value
  for (const row of data) {
    const val = col.accessor ? col.accessor(row) : (row as any)[col.key];
    if (val === null || val === undefined) continue;
    if (val instanceof Date) return "date";
    if (typeof val === "boolean") return "boolean";
    if (typeof val === "number") return "number";
    if (typeof val === "string") {
      // date-like string?
      if (!isNaN(Date.parse(val)) && /\d{4}/.test(val)) return "date";
      // small enum? Check distinct values
    }
    break;
  }
  // Check if it's a small enum (status, badge)
  const distinctVals = new Set(
    data.map(r => {
      const v = col.accessor ? col.accessor(r) : (r as any)[col.key];
      return typeof v === "string" ? v : null;
    }).filter(Boolean)
  );
  if (distinctVals.size > 0 && distinctVals.size <= 8) return "select";
  return "text";
}

/** Derive select options from data */
function deriveSelectOptions(col: ColumnDef<any>, data: any[]): string[] {
  if (col.filterOptions) return col.filterOptions;
  const vals = new Set<string>();
  for (const row of data) {
    const v = col.accessor ? col.accessor(row) : (row as any)[col.key];
    if (v !== null && v !== undefined && typeof v === "string") vals.add(v);
  }
  return Array.from(vals).sort();
}

// ─────────────────────────────────────────────────────────────────────────────
// Sort icon
// ─────────────────────────────────────────────────────────────────────────────
function SortIcon({ dir }: { dir: SortDirection }) {
  if (dir === "asc")
    return <ChevronUp size={13} strokeWidth={2.5} />;
  if (dir === "desc")
    return <ChevronDown size={13} strokeWidth={2.5} />;
  return (
    <ChevronsUpDown
      size={13}
      className="opacity-0 group-hover:opacity-60 transition-opacity"
      strokeWidth={2}
    />
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Column Filter Popover
// ─────────────────────────────────────────────────────────────────────────────
function ColumnFilterPopover<TRow>({
  col,
  data,
  filterType,
  activeFilter,
  onApply,
  onClear,
}: {
  col: ColumnDef<TRow>;
  data: TRow[];
  filterType: ColumnFilterType;
  activeFilter: ColumnFilterValue | undefined;
  onApply: (val: ColumnFilterValue) => void;
  onClear: () => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const hasActive = !!activeFilter;

  // Local draft state
  const [textVal, setTextVal] = useState(() =>
    activeFilter?.type === "text" ? activeFilter.value : ""
  );
  const [numMin, setNumMin] = useState(() =>
    activeFilter?.type === "number" ? (activeFilter.min?.toString() ?? "") : ""
  );
  const [numMax, setNumMax] = useState(() =>
    activeFilter?.type === "number" ? (activeFilter.max?.toString() ?? "") : ""
  );
  const [selectedVals, setSelectedVals] = useState<string[]>(() =>
    activeFilter?.type === "select" ? activeFilter.values : []
  );
  const [dateFrom, setDateFrom] = useState(() =>
    activeFilter?.type === "date" ? (activeFilter.from ?? activeFilter.raw ?? "") : ""
  );
  const [dateTo, setDateTo] = useState(() =>
    activeFilter?.type === "date" ? (activeFilter.to ?? "") : ""
  );
  const [boolVal, setBoolVal] = useState<boolean | null>(() =>
    activeFilter?.type === "boolean" ? activeFilter.value : null
  );

  // Reset local state when popover opens
  const openPopover = () => {
    setTextVal(activeFilter?.type === "text" ? activeFilter.value : "");
    setNumMin(activeFilter?.type === "number" ? (activeFilter.min?.toString() ?? "") : "");
    setNumMax(activeFilter?.type === "number" ? (activeFilter.max?.toString() ?? "") : "");
    setSelectedVals(activeFilter?.type === "select" ? activeFilter.values : []);
    setDateFrom(activeFilter?.type === "date" ? (activeFilter.from ?? activeFilter.raw ?? "") : "");
    setDateTo(activeFilter?.type === "date" ? (activeFilter.to ?? "") : "");
    setBoolVal(activeFilter?.type === "boolean" ? activeFilter.value : null);
    setOpen(true);
  };

  const apply = () => {
    switch (filterType) {
      case "text":
        if (textVal.trim()) onApply({ type: "text", value: textVal.trim() });
        else onClear();
        break;
      case "number": {
        const min = numMin !== "" ? parseFloat(numMin) : undefined;
        const max = numMax !== "" ? parseFloat(numMax) : undefined;
        if (min !== undefined || max !== undefined) onApply({ type: "number", min, max });
        else onClear();
        break;
      }
      case "select":
        if (selectedVals.length > 0) onApply({ type: "select", values: selectedVals });
        else onClear();
        break;
      case "date":
        if (dateFrom.trim() || dateTo.trim()) {
          const isNaturalFrom = dateFrom.trim() && !dateFrom.match(/^\d{4}-\d{2}-\d{2}$/);
          onApply({ type: "date", from: dateFrom.trim() || undefined, to: dateTo.trim() || undefined, raw: isNaturalFrom ? dateFrom.trim() : undefined });
        } else onClear();
        break;
      case "boolean":
        if (boolVal !== null) onApply({ type: "boolean", value: boolVal });
        else onClear();
        break;
    }
    setOpen(false);
  };

  const clear = () => {
    onClear();
    setOpen(false);
  };

  const selectOptions = useMemo(() => {
    if (filterType === "select") return deriveSelectOptions(col, data as any[]);
    return [];
  }, [filterType, col, data]);

  const DATE_SUGGESTIONS = [
    "today", "yesterday", "this week", "last week",
    "this month", "last month", "this year", "last year",
    "30 days ago", "90 days ago",
  ];

  return (
    <div className="relative" ref={ref} onClick={(e) => e.stopPropagation()}>
      <button
        data-testid={`filter-btn-${col.key}`}
        onClick={() => open ? setOpen(false) : openPopover()}
        className={cn(
          "p-0.5 rounded transition-colors",
          hasActive
            ? "text-bg-color-filter-focus-border opacity-100"
            : "opacity-0 group-hover:opacity-60 text-bg-text-muted hover:opacity-100"
        )}
        aria-label={`Filter ${String(col.header)}`}
        title={`Filter by ${String(col.header)}`}
      >
        {hasActive ? <FilterX size={12} strokeWidth={2.5} /> : <Filter size={12} strokeWidth={2} />}
      </button>

      {open && (
        <>
          <div className="fixed inset-0 z-30" onClick={() => setOpen(false)} />
          <div
            className="absolute right-0 top-full mt-1.5 z-40 min-w-[240px] rounded-xl shadow-xl overflow-hidden"
            style={{
              background: "var(--bg-color-surface)",
              border: "1px solid var(--bg-color-border)",
            }}
          >
            {/* Header */}
            <div className="px-3 py-2.5 border-b flex items-center justify-between" style={{ borderColor: "var(--bg-color-divider)" }}>
              <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: "var(--bg-color-text-muted)" }}>
                Filter: {String(col.header)}
              </span>
              {hasActive && (
                <button onClick={clear} className="text-xs underline" style={{ color: "var(--bg-color-filter-focus-border)" }}>
                  Clear
                </button>
              )}
            </div>

            {/* Body */}
            <div className="p-3 space-y-2.5">
              {filterType === "text" && (
                <input
                  autoFocus
                  data-testid={`filter-input-${col.key}`}
                  type="text"
                  value={textVal}
                  onChange={(e) => setTextVal(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && apply()}
                  placeholder={`Contains…`}
                  className="w-full text-sm px-3 py-2 rounded-lg focus:outline-none"
                  style={{
                    background: "var(--bg-color-filter-bg)",
                    border: "1px solid var(--bg-color-border)",
                    color: "var(--bg-color-filter-text)",
                  }}
                />
              )}

              {filterType === "number" && (
                <div className="flex items-center gap-2">
                  <input
                    autoFocus
                    data-testid={`filter-min-${col.key}`}
                    type="number"
                    value={numMin}
                    onChange={(e) => setNumMin(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && apply()}
                    placeholder="Min"
                    className="w-full text-sm px-3 py-2 rounded-lg focus:outline-none"
                    style={{
                      background: "var(--bg-color-filter-bg)",
                      border: "1px solid var(--bg-color-border)",
                      color: "var(--bg-color-filter-text)",
                    }}
                  />
                  <span style={{ color: "var(--bg-color-text-muted)" }} className="text-xs">–</span>
                  <input
                    data-testid={`filter-max-${col.key}`}
                    type="number"
                    value={numMax}
                    onChange={(e) => setNumMax(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && apply()}
                    placeholder="Max"
                    className="w-full text-sm px-3 py-2 rounded-lg focus:outline-none"
                    style={{
                      background: "var(--bg-color-filter-bg)",
                      border: "1px solid var(--bg-color-border)",
                      color: "var(--bg-color-filter-text)",
                    }}
                  />
                </div>
              )}

              {filterType === "select" && (
                <div className="space-y-1 max-h-48 overflow-y-auto">
                  {selectOptions.map((opt) => {
                    const checked = selectedVals.includes(opt);
                    return (
                      <label
                        key={opt}
                        data-testid={`filter-option-${col.key}-${opt}`}
                        className="flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer text-sm transition-colors"
                        style={{
                          color: "var(--bg-color-text)",
                          background: checked ? "var(--bg-color-row-selected)" : "transparent",
                        }}
                      >
                        <span
                          className="w-4 h-4 rounded border flex items-center justify-center flex-shrink-0"
                          style={{
                            borderColor: checked ? "var(--bg-color-filter-focus-border)" : "var(--bg-color-border)",
                            background: checked ? "var(--bg-color-filter-focus-border)" : "transparent",
                          }}
                        >
                          {checked && <Check size={10} strokeWidth={3} style={{ color: "var(--bg-color-surface)" }} />}
                        </span>
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() =>
                            setSelectedVals((prev) =>
                              checked ? prev.filter((v) => v !== opt) : [...prev, opt]
                            )
                          }
                          className="sr-only"
                        />
                        {opt}
                      </label>
                    );
                  })}
                </div>
              )}

              {filterType === "date" && (
                <div className="space-y-2">
                  <div>
                    <label className="text-xs mb-1 block" style={{ color: "var(--bg-color-text-muted)" }}>
                      From (or natural language)
                    </label>
                    <input
                      autoFocus
                      data-testid={`filter-date-from-${col.key}`}
                      type="text"
                      value={dateFrom}
                      onChange={(e) => setDateFrom(e.target.value)}
                      onKeyDown={(e) => e.key === "Enter" && apply()}
                      placeholder="e.g. Jan 2024, last month, 2024-01-15"
                      className="w-full text-sm px-3 py-2 rounded-lg focus:outline-none"
                      style={{
                        background: "var(--bg-color-filter-bg)",
                        border: "1px solid var(--bg-color-border)",
                        color: "var(--bg-color-filter-text)",
                      }}
                    />
                  </div>
                  <div>
                    <label className="text-xs mb-1 block" style={{ color: "var(--bg-color-text-muted)" }}>To (optional)</label>
                    <input
                      data-testid={`filter-date-to-${col.key}`}
                      type="text"
                      value={dateTo}
                      onChange={(e) => setDateTo(e.target.value)}
                      onKeyDown={(e) => e.key === "Enter" && apply()}
                      placeholder="e.g. today, 2024-12-31"
                      className="w-full text-sm px-3 py-2 rounded-lg focus:outline-none"
                      style={{
                        background: "var(--bg-color-filter-bg)",
                        border: "1px solid var(--bg-color-border)",
                        color: "var(--bg-color-filter-text)",
                      }}
                    />
                  </div>
                  <div className="flex flex-wrap gap-1 pt-1">
                    {DATE_SUGGESTIONS.map((s) => (
                      <button
                        key={s}
                        onClick={() => { setDateFrom(s); setDateTo(""); }}
                        className="text-xs px-2 py-0.5 rounded-full border transition-colors"
                        style={{
                          borderColor: dateFrom === s ? "var(--bg-color-filter-focus-border)" : "var(--bg-color-border)",
                          color: dateFrom === s ? "var(--bg-color-filter-focus-border)" : "var(--bg-color-text-muted)",
                          background: dateFrom === s ? "var(--bg-color-row-selected)" : "transparent",
                        }}
                      >
                        {s}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {filterType === "boolean" && (
                <div className="flex gap-2">
                  {([true, false] as const).map((v) => (
                    <button
                      key={String(v)}
                      data-testid={`filter-bool-${col.key}-${v}`}
                      onClick={() => setBoolVal(boolVal === v ? null : v)}
                      className="flex-1 text-sm py-2 rounded-lg border transition-colors"
                      style={{
                        borderColor: boolVal === v ? "var(--bg-color-filter-focus-border)" : "var(--bg-color-border)",
                        background: boolVal === v ? "var(--bg-color-row-selected)" : "var(--bg-color-filter-bg)",
                        color: boolVal === v ? "var(--bg-color-filter-focus-border)" : "var(--bg-color-text)",
                      }}
                    >
                      {v ? "✓ True" : "✗ False"}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Footer */}
            <div className="px-3 py-2.5 flex gap-2 border-t" style={{ borderColor: "var(--bg-color-divider)" }}>
              <button
                onClick={clear}
                className="flex-1 text-sm py-1.5 rounded-lg border transition-colors"
                style={{
                  borderColor: "var(--bg-color-border)",
                  color: "var(--bg-color-text-muted)",
                  background: "transparent",
                }}
              >
                Clear
              </button>
              <button
                data-testid={`filter-apply-${col.key}`}
                onClick={apply}
                className="flex-1 text-sm py-1.5 rounded-lg font-medium transition-colors"
                style={{
                  background: "var(--bg-color-filter-focus-border)",
                  color: "var(--bg-color-surface)",
                  border: "none",
                }}
              >
                Apply
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Column visibility toggle popover
// ─────────────────────────────────────────────────────────────────────────────
function ColumnToggle<TRow>({
  columns,
  hidden,
  onToggle,
}: {
  columns: ColumnDef<TRow>[];
  hidden: Set<string>;
  onToggle: (key: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  return (
    <div className="relative" ref={ref}>
      <button
        data-testid="button-column-toggle"
        onClick={() => setOpen((p) => !p)}
        className="bg-col-toggle-btn flex items-center gap-1.5 px-3 py-1.5 rounded text-sm font-medium transition-colors"
        aria-label="Toggle columns"
      >
        <Columns3 size={14} />
        <span>Columns</span>
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-10" onClick={() => setOpen(false)} />
          <div className="bg-col-toggle-panel absolute right-0 top-full mt-1.5 z-20 min-w-[160px] rounded-lg border shadow-lg py-1 overflow-hidden">
            {columns.map((col) => {
              const isHidden = hidden.has(col.key);
              return (
                <button
                  key={col.key}
                  data-testid={`button-col-toggle-${col.key}`}
                  onClick={() => onToggle(col.key)}
                  className="bg-col-toggle-item flex w-full items-center gap-2 px-3 py-2 text-sm text-left transition-colors"
                >
                  <span className="w-4 h-4 flex items-center justify-center">
                    {!isHidden && <Check size={13} strokeWidth={2.5} />}
                  </span>
                  <span>{col.header}</span>
                </button>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Status Badge
// ─────────────────────────────────────────────────────────────────────────────
export function StatusBadge({ status }: { status: string }) {
  const upper = status.toUpperCase();
  let cls = "bg-badge-inactive text-badge-inactive";
  if (upper === "ACTIVE")  cls = "bg-badge-active text-badge-active";
  if (upper === "PENDING") cls = "bg-badge-pending text-badge-pending";

  return (
    <span
      className={cn(
        cls,
        "inline-flex items-center px-2.5 py-0.5 text-xs font-semibold tracking-wider uppercase rounded-badge"
      )}
      data-testid={`status-badge-${status}`}
    >
      {upper}
    </span>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Pagination
// ─────────────────────────────────────────────────────────────────────────────
function Pagination({
  page,
  totalPages,
  pageSize,
  pageSizes,
  totalRows,
  onPage,
  onPageSize,
}: {
  page: number;
  totalPages: number;
  pageSize: number;
  pageSizes: number[];
  totalRows: number;
  onPage: (p: number) => void;
  onPageSize: (s: number) => void;
}) {
  const start = totalRows === 0 ? 0 : (page - 1) * pageSize + 1;
  const end   = Math.min(page * pageSize, totalRows);

  const pages: (number | "...")[] = [];
  if (totalPages <= 7) {
    for (let i = 1; i <= totalPages; i++) pages.push(i);
  } else {
    pages.push(1);
    if (page > 3) pages.push("...");
    for (let i = Math.max(2, page - 1); i <= Math.min(totalPages - 1, page + 1); i++)
      pages.push(i);
    if (page < totalPages - 2) pages.push("...");
    pages.push(totalPages);
  }

  return (
    <div className="bg-pagination flex flex-wrap items-center justify-between gap-3 px-4 py-3 border-t border-bg-divider text-sm">
      <div className="flex items-center gap-3 text-bg-text-muted">
        <span data-testid="pagination-count">
          {start}–{end} of {totalRows}
        </span>
        <select
          data-testid="select-page-size"
          value={pageSize}
          onChange={(e) => onPageSize(Number(e.target.value))}
          className="bg-pag-select border border-bg-pag-border rounded px-2 py-1 text-xs focus:outline-none cursor-pointer"
        >
          {pageSizes.map((s) => (
            <option key={s} value={s}>{s} per page</option>
          ))}
        </select>
      </div>
      <div className="flex items-center gap-1">
        <button
          data-testid="button-prev-page"
          onClick={() => onPage(page - 1)}
          disabled={page === 1}
          className="bg-pag-btn w-8 h-8 flex items-center justify-center rounded disabled:opacity-30 disabled:cursor-not-allowed"
          aria-label="Previous page"
        >
          <ChevronLeft size={15} />
        </button>
        {pages.map((p, i) =>
          p === "..." ? (
            <span key={`ellipsis-${i}`} className="px-1 text-bg-text-muted">…</span>
          ) : (
            <button
              key={p}
              data-testid={`button-page-${p}`}
              onClick={() => onPage(p as number)}
              className={cn(
                "bg-pag-num w-8 h-8 flex items-center justify-center rounded text-sm font-medium transition-colors",
                p === page ? "bg-pag-active text-pag-active-text" : ""
              )}
              aria-current={p === page ? "page" : undefined}
            >
              {p}
            </button>
          )
        )}
        <button
          data-testid="button-next-page"
          onClick={() => onPage(page + 1)}
          disabled={page === totalPages || totalPages === 0}
          className="bg-pag-btn w-8 h-8 flex items-center justify-center rounded disabled:opacity-30 disabled:cursor-not-allowed"
          aria-label="Next page"
        >
          <ChevronRight size={15} />
        </button>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: resolve row key
// ─────────────────────────────────────────────────────────────────────────────
function resolveRowKey<TRow>(
  row: TRow,
  rowKey: BestGridProps<TRow>["rowKey"],
  index: number
): string | number {
  if (!rowKey) return index;
  if (typeof rowKey === "function") return rowKey(row);
  return row[rowKey] as string | number;
}

// ─────────────────────────────────────────────────────────────────────────────
// BestGrid — main component
// ─────────────────────────────────────────────────────────────────────────────
export function BestGrid<TRow = Record<string, unknown>>({
  data,
  columns: columnsProp,
  actions,
  rowKey,
  title,
  showCount = true,
  filterable = true,
  filterPlaceholder = "Filter…",
  columnFilters = true,
  sortable: sortableGlobal = true,
  selectable = false,
  striped = false,
  hoverable = true,
  columnToggle = true,
  paginate = true,
  pageSizes = [10, 25, 50, 100],
  defaultPageSize = 10,
  virtualScroll = false,
  virtualScrollHeight = "600px",
  onSelectionChange,
  emptyMessage = "No data to display.",
  theme,
  darkMode,
  className,
  style,
}: BestGridProps<TRow>) {
  // ── Dark mode ────────────────────────────────────────────────────────────
  const [osDark, setOsDark] = useState(() =>
    typeof window !== "undefined"
      ? window.matchMedia("(prefers-color-scheme: dark)").matches
      : false
  );
  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => setOsDark(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  const [manualDark, setManualDark] = useState<boolean | undefined>(darkMode);
  // If prop changes externally, sync
  useEffect(() => { if (darkMode !== undefined) setManualDark(darkMode); }, [darkMode]);

  const isDark = manualDark !== undefined ? manualDark : osDark;

  const resolvedTheme  = theme ?? PerfectionTheme;
  const resolvedTokens = resolveTokens(resolvedTheme, isDark);
  const cssVars        = themeToCSS(resolvedTokens);

  // ── State ────────────────────────────────────────────────────────────────
  const [filterQuery, setFilterQuery]   = useState("");
  const [colFilters, setColFilters]     = useState<Record<string, ColumnFilterValue>>({});
  const [sortKey, setSortKey]           = useState<string | null>(null);
  const [sortDir, setSortDir]           = useState<SortDirection>(null);
  const [page, setPage]                 = useState(1);
  const [pageSize, setPageSize]         = useState(defaultPageSize);
  const [selectedKeys, setSelectedKeys] = useState<Set<string | number>>(new Set());
  const [hiddenCols, setHiddenCols]     = useState<Set<string>>(
    new Set(columnsProp.filter((c) => c.hidden).map((c) => c.key))
  );

  // ── Virtual scroll refs ──────────────────────────────────────────────────
  const scrollRef       = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const ROW_HEIGHT_PX = useMemo(() => {
    const v = resolvedTokens.rowMinHeight;
    if (v.endsWith("rem")) return parseFloat(v) * 16;
    if (v.endsWith("px"))  return parseFloat(v);
    return 48;
  }, [resolvedTokens.rowMinHeight]);
  const BUFFER = 5;

  // ── Derived columns ──────────────────────────────────────────────────────
  const visibleColumns = useMemo(
    () => columnsProp.filter((c) => !hiddenCols.has(c.key)),
    [columnsProp, hiddenCols]
  );

  // ── Per-column filter types (auto-detected) ───────────────────────────────
  const colFilterTypes = useMemo(() => {
    const map: Record<string, ColumnFilterType> = {};
    for (const col of columnsProp) {
      if (col.filterable !== false) {
        map[col.key] = detectFilterType(col, data as any[]);
      }
    }
    return map;
  }, [columnsProp, data]);

  // ── Active filter count ──────────────────────────────────────────────────
  const activeFilterCount = Object.keys(colFilters).length + (filterQuery.trim() ? 1 : 0);

  // ── Filter ───────────────────────────────────────────────────────────────
  const filtered = useMemo(() => {
    let rows = data;

    // Global search
    if (filterQuery.trim()) {
      const q = filterQuery.toLowerCase();
      rows = rows.filter((row) =>
        columnsProp
          .filter((c) => c.filterable !== false)
          .some((col) => {
            const val = col.accessor ? col.accessor(row) : (row as Record<string, unknown>)[col.key];
            // For object values (avatar_name, link), stringify recursively
            const str = typeof val === "object" && val !== null
              ? JSON.stringify(val).toLowerCase()
              : String(val ?? "").toLowerCase();
            return str.includes(q);
          })
      );
    }

    // Per-column filters
    for (const [key, filter] of Object.entries(colFilters)) {
      const col = columnsProp.find((c) => c.key === key);
      if (!col) continue;
      rows = rows.filter((row) => {
        const val = col.accessor ? col.accessor(row) : (row as Record<string, unknown>)[key];
        return matchesColumnFilter(val, filter);
      });
    }

    return rows;
  }, [data, filterQuery, colFilters, columnsProp]);

  // ── Sort ─────────────────────────────────────────────────────────────────
  const sorted = useMemo(() => {
    if (!sortKey || !sortDir) return filtered;
    const col = columnsProp.find((c) => c.key === sortKey);
    if (!col) return filtered;
    return [...filtered].sort((a, b) => {
      const aVal = col.accessor ? col.accessor(a) : (a as Record<string, unknown>)[sortKey];
      const bVal = col.accessor ? col.accessor(b) : (b as Record<string, unknown>)[sortKey];
      const cmp =
        typeof aVal === "number" && typeof bVal === "number"
          ? aVal - bVal
          : String(aVal ?? "").localeCompare(String(bVal ?? ""), undefined, { numeric: true });
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [filtered, sortKey, sortDir, columnsProp]);

  // ── Pagination ────────────────────────────────────────────────────────────
  const totalPages = paginate && !virtualScroll ? Math.max(1, Math.ceil(sorted.length / pageSize)) : 1;
  const pageData   = paginate && !virtualScroll ? sorted.slice((page - 1) * pageSize, page * pageSize) : sorted;

  // ── Virtual windowing ─────────────────────────────────────────────────────
  const visibleRows = useMemo(() => {
    if (!virtualScroll) return pageData;
    const containerH = parseFloat(virtualScrollHeight);
    const visibleCount = Math.ceil(containerH / ROW_HEIGHT_PX);
    const startIdx = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT_PX) - BUFFER);
    const endIdx   = Math.min(pageData.length, startIdx + visibleCount + BUFFER * 2);
    return pageData.slice(startIdx, endIdx);
  }, [virtualScroll, pageData, scrollTop, ROW_HEIGHT_PX, virtualScrollHeight]);

  const virtualPaddingTop = useMemo(() => {
    if (!virtualScroll) return 0;
    const startIdx = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT_PX) - BUFFER);
    return startIdx * ROW_HEIGHT_PX;
  }, [virtualScroll, scrollTop, ROW_HEIGHT_PX]);

  const virtualPaddingBottom = useMemo(() => {
    if (!virtualScroll) return 0;
    const containerH    = parseFloat(virtualScrollHeight);
    const visibleCount  = Math.ceil(containerH / ROW_HEIGHT_PX);
    const startIdx      = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT_PX) - BUFFER);
    const endIdx        = Math.min(pageData.length, startIdx + visibleCount + BUFFER * 2);
    return Math.max(0, (pageData.length - endIdx) * ROW_HEIGHT_PX);
  }, [virtualScroll, pageData.length, scrollTop, ROW_HEIGHT_PX, virtualScrollHeight]);

  // ── Handlers ─────────────────────────────────────────────────────────────
  const handleSort = useCallback((key: string) => {
    if (sortKey !== key) { setSortKey(key); setSortDir("asc"); }
    else if (sortDir === "asc") setSortDir("desc");
    else { setSortKey(null); setSortDir(null); }
    setPage(1);
  }, [sortKey, sortDir]);

  const handleFilter = useCallback((v: string) => {
    setFilterQuery(v);
    setPage(1);
  }, []);

  const handlePage = useCallback((p: number) =>
    setPage(Math.max(1, Math.min(p, totalPages))),
    [totalPages]
  );

  const handlePageSize = useCallback((s: number) => {
    setPageSize(s);
    setPage(1);
  }, []);

  const applyColFilter = useCallback((key: string, val: ColumnFilterValue) => {
    setColFilters((prev) => ({ ...prev, [key]: val }));
    setPage(1);
  }, []);

  const clearColFilter = useCallback((key: string) => {
    setColFilters((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
    setPage(1);
  }, []);

  const clearAllFilters = useCallback(() => {
    setFilterQuery("");
    setColFilters({});
    setPage(1);
  }, []);

  // ── Selection ─────────────────────────────────────────────────────────────
  const allPageKeys = useMemo(
    () => pageData.map((row, i) => resolveRowKey(row, rowKey, i)),
    [pageData, rowKey]
  );
  const allPageSelected =
    allPageKeys.length > 0 && allPageKeys.every((k) => selectedKeys.has(k));

  const toggleSelectAll = useCallback(() => {
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (allPageSelected) { allPageKeys.forEach((k) => next.delete(k)); }
      else                 { allPageKeys.forEach((k) => next.add(k)); }
      if (onSelectionChange) {
        const sel = data.filter((row, i) => next.has(resolveRowKey(row, rowKey, i)));
        onSelectionChange(sel);
      }
      return next;
    });
  }, [allPageSelected, allPageKeys, data, rowKey, onSelectionChange]);

  const toggleSelectRow = useCallback((key: string | number) => {
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      if (onSelectionChange) {
        const sel = data.filter((row, i) => next.has(resolveRowKey(row, rowKey, i)));
        onSelectionChange(sel);
      }
      return next;
    });
  }, [data, rowKey, onSelectionChange]);

  const handleColToggle = useCallback((key: string) => {
    setHiddenCols((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  }, []);

  // ──────────────────────────────────────────────────────────────────────────
  // Render
  // ──────────────────────────────────────────────────────────────────────────
  return (
    <div
      className={cn("bg-grid-root", className)}
      style={{ ...cssVars, ...style } as CSSProperties}
      data-testid="best-grid"
    >
      <div className="bg-grid-card">

        {/* ── Title bar ── */}
        {(title || filterable || columnToggle) && (
          <div className="bg-title-bar flex items-center justify-between gap-3 flex-wrap">
            <div className="flex items-center gap-2 flex-wrap">
              {title && (
                <h2 className="bg-title-text" data-testid="grid-title">
                  {title}
                  {showCount && (
                    <span className="bg-title-count ml-2">({sorted.length})</span>
                  )}
                </h2>
              )}
              {/* Active filter pill */}
              {activeFilterCount > 0 && (
                <button
                  data-testid="button-clear-all-filters"
                  onClick={clearAllFilters}
                  className="flex items-center gap-1 px-2 py-0.5 text-xs rounded-full border transition-colors"
                  style={{
                    borderColor: "var(--bg-color-filter-focus-border)",
                    color: "var(--bg-color-filter-focus-border)",
                    background: "var(--bg-color-row-selected)",
                  }}
                  title="Clear all filters"
                >
                  <FilterX size={10} strokeWidth={2.5} />
                  {activeFilterCount} filter{activeFilterCount !== 1 ? "s" : ""} active
                </button>
              )}
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              {filterable && (
                <div className="bg-filter-wrap relative">
                  <Search
                    size={14}
                    className="absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
                    style={{ color: "var(--bg-color-text-muted)" }}
                  />
                  <input
                    data-testid="input-filter"
                    type="text"
                    value={filterQuery}
                    onChange={(e) => handleFilter(e.target.value)}
                    placeholder={filterPlaceholder}
                    className="bg-filter-input pl-8 pr-8 py-1.5 text-sm rounded-input w-48 focus:w-64 focus:outline-none transition-all"
                    aria-label="Filter rows"
                  />
                  {filterQuery && (
                    <button
                      data-testid="button-clear-filter"
                      onClick={() => handleFilter("")}
                      className="absolute right-2 top-1/2 -translate-y-1/2 transition-colors"
                      style={{ color: "var(--bg-color-text-muted)" }}
                      aria-label="Clear filter"
                    >
                      <X size={13} />
                    </button>
                  )}
                </div>
              )}
              {/* Dark mode toggle */}
              <button
                data-testid="button-dark-mode"
                onClick={() => setManualDark((prev) => !(prev ?? isDark))}
                className="bg-col-toggle-btn flex items-center gap-1.5 px-2.5 py-1.5 rounded text-sm font-medium transition-colors"
                aria-label={isDark ? "Switch to light mode" : "Switch to dark mode"}
                title={isDark ? "Light mode" : "Dark mode"}
              >
                {isDark ? <Sun size={14} /> : <Moon size={14} />}
              </button>
              {columnToggle && (
                <ColumnToggle
                  columns={columnsProp}
                  hidden={hiddenCols}
                  onToggle={handleColToggle}
                />
              )}
            </div>
          </div>
        )}

        {/* ── Table (with optional virtual scroll wrapper) ── */}
        <div
          ref={scrollRef}
          className="bg-table-scroll overflow-x-auto"
          style={virtualScroll ? { height: virtualScrollHeight, overflowY: "auto" } : {}}
          onScroll={virtualScroll ? (e) => setScrollTop((e.target as HTMLDivElement).scrollTop) : undefined}
        >
          <table className="bg-table w-full" role="grid">
            <thead className="bg-thead" style={{ position: "sticky", top: 0, zIndex: 2 }}>
              <tr>
                {selectable && (
                  <th className="bg-th w-10 text-center">
                    <input
                      data-testid="checkbox-select-all"
                      type="checkbox"
                      checked={allPageSelected}
                      onChange={toggleSelectAll}
                      className="rounded"
                      aria-label="Select all rows"
                    />
                  </th>
                )}
                {visibleColumns.map((col) => {
                  const isSorted    = sortKey === col.key;
                  const isSortable  = sortableGlobal && col.sortable !== false;
                  const isFilterable = columnFilters && col.filterable !== false;
                  const filterType  = colFilterTypes[col.key] ?? "text";
                  const activeFilter = colFilters[col.key];

                  return (
                    <th
                      key={col.key}
                      data-testid={`th-${col.key}`}
                      className={cn("bg-th group", isSortable && "cursor-pointer select-none", col.className)}
                      style={{ minWidth: col.minWidth, textAlign: col.align ?? "left" }}
                      onClick={isSortable ? () => handleSort(col.key) : undefined}
                      aria-sort={
                        isSorted && sortDir === "asc"  ? "ascending"  :
                        isSorted && sortDir === "desc" ? "descending" : "none"
                      }
                    >
                      <span className="flex items-center gap-1.5">
                        {col.header}
                        {isSortable && <SortIcon dir={isSorted ? sortDir : null} />}
                        {isFilterable && (
                          <ColumnFilterPopover
                            col={col}
                            data={data as any[]}
                            filterType={filterType}
                            activeFilter={activeFilter}
                            onApply={(val) => applyColFilter(col.key, val)}
                            onClear={() => clearColFilter(col.key)}
                          />
                        )}
                      </span>
                    </th>
                  );
                })}
                {actions && actions.length > 0 && (
                  <th className="bg-th" data-testid="th-actions">ACTIONS</th>
                )}
              </tr>
            </thead>
            <tbody>
              {/* Virtual scroll top spacer */}
              {virtualScroll && virtualPaddingTop > 0 && (
                <tr style={{ height: virtualPaddingTop }}>
                  <td colSpan={visibleColumns.length + (selectable ? 1 : 0) + (actions?.length ? 1 : 0)} />
                </tr>
              )}

              {visibleRows.length === 0 ? (
                <tr>
                  <td
                    colSpan={visibleColumns.length + (selectable ? 1 : 0) + (actions?.length ? 1 : 0)}
                    className="bg-td-empty text-center py-16 text-bg-text-muted text-sm"
                    data-testid="grid-empty"
                  >
                    {activeFilterCount > 0
                      ? <span>No results match the active filters. <button onClick={clearAllFilters} className="underline ml-1" style={{ color: "var(--bg-color-filter-focus-border)" }}>Clear all</button></span>
                      : emptyMessage
                    }
                  </td>
                </tr>
              ) : (
                visibleRows.map((row, rowIndex) => {
                  // For virtual scroll, rowIndex needs to account for the offset
                  const absoluteIndex = virtualScroll
                    ? Math.max(0, Math.floor(scrollTop / ROW_HEIGHT_PX) - BUFFER) + rowIndex
                    : rowIndex;
                  const key        = resolveRowKey(row, rowKey, absoluteIndex);
                  const isSelected = selectedKeys.has(key);
                  const isStripe   = striped && absoluteIndex % 2 === 1;

                  return (
                    <tr
                      key={key}
                      data-testid={`row-${key}`}
                      className={cn(
                        "bg-tr",
                        hoverable  && "bg-tr-hoverable",
                        isSelected && "bg-tr-selected",
                        isStripe   && "bg-tr-stripe"
                      )}
                      style={virtualScroll ? { height: ROW_HEIGHT_PX } : undefined}
                      onClick={selectable ? () => toggleSelectRow(key) : undefined}
                    >
                      {selectable && (
                        <td className="bg-td text-center">
                          <input
                            data-testid={`checkbox-row-${key}`}
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => toggleSelectRow(key)}
                            onClick={(e) => e.stopPropagation()}
                            className="rounded"
                            aria-label={`Select row ${key}`}
                          />
                        </td>
                      )}
                      {visibleColumns.map((col) => {
                        const rawVal = col.accessor
                          ? col.accessor(row)
                          : (row as Record<string, unknown>)[col.key];
                        const rendered = col.cell
                          ? col.cell(row, rawVal)
                          : rawVal === null || rawVal === undefined
                          ? <span className="text-bg-text-muted">—</span>
                          : String(rawVal);
                        return (
                          <td
                            key={col.key}
                            data-testid={`td-${col.key}-${key}`}
                            className={cn("bg-td", col.className)}
                            style={{ textAlign: col.align ?? "left" }}
                          >
                            {rendered}
                          </td>
                        );
                      })}
                      {actions && actions.length > 0 && (
                        <td className="bg-td bg-td-actions" data-testid={`td-actions-${key}`}>
                          <div className="flex items-center gap-1 flex-nowrap">
                            {actions
                              .filter((a) => !a.visible || a.visible(row))
                              .map((action) => {
                                const isDisabled = action.disabled?.(row) ?? false;
                                const variantCls =
                                  action.variant === "danger"  ? "bg-action-danger"  :
                                  action.variant === "primary" ? "bg-action-primary" : "bg-action-ghost";
                                return (
                                  <button
                                    key={action.key}
                                    data-testid={`button-action-${action.key}-${key}`}
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      if (!isDisabled) action.onClick(row);
                                    }}
                                    disabled={isDisabled}
                                    className={cn(
                                      "bg-action-btn flex items-center gap-1.5 px-2.5 py-1 rounded-button text-sm disabled:opacity-40 disabled:cursor-not-allowed",
                                      variantCls
                                    )}
                                    aria-label={action.label}
                                  >
                                    {action.icon}
                                    <span>{action.label}</span>
                                  </button>
                                );
                              })}
                          </div>
                        </td>
                      )}
                    </tr>
                  );
                })
              )}

              {/* Virtual scroll bottom spacer */}
              {virtualScroll && virtualPaddingBottom > 0 && (
                <tr style={{ height: virtualPaddingBottom }}>
                  <td colSpan={visibleColumns.length + (selectable ? 1 : 0) + (actions?.length ? 1 : 0)} />
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* ── Pagination ── */}
        {paginate && !virtualScroll && (
          <Pagination
            page={page}
            totalPages={totalPages}
            pageSize={pageSize}
            pageSizes={pageSizes}
            totalRows={sorted.length}
            onPage={handlePage}
            onPageSize={handlePageSize}
          />
        )}

        {/* ── Virtual scroll footer ── */}
        {virtualScroll && (
          <div
            className="flex items-center justify-between px-4 py-2.5 border-t text-xs"
            style={{
              borderColor: "var(--bg-color-divider)",
              color: "var(--bg-color-text-muted)",
            }}
          >
            <span>{sorted.length} row{sorted.length !== 1 ? "s" : ""}</span>
            <span>Scroll to load more</span>
          </div>
        )}

      </div>
    </div>
  );
}

export default BestGrid;
