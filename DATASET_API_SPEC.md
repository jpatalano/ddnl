# Dataset API Specification
**Platform:** DDNL Platform  
**Version:** 1.0  
**Purpose:** Contract that any instance must satisfy must satisfy — either natively or via a server-side adapter — to be fully supported by the dashboard and reporting platform.

---

## Overview

The platform communicates with one canonical BI API surface. New instances either implement this API natively (like FCC does) or register an **adapter** in `server.js` that translates their upstream API into this shape at the proxy layer. The frontend never talks to upstream APIs directly.

```
Browser → /api/bi/* (server.js proxy) → upstream API (native or adapted)
```

---

## Authentication

Authentication between the browser and the platform proxy is handled by the platform (Basic Auth today, Entra JWT later). The proxy layer is responsible for adding any upstream credentials (API keys, Bearer tokens, etc.) before forwarding requests. The frontend never holds upstream credentials.

---

## Endpoints

### 1. List Datasets
**`GET /api/bi/datasets`**

Returns all datasets available to this instance. Used to populate the Data Explorer list, Report Builder dataset selector, and tile picker.

**Response**
```json
{
  "success": true,
  "data": {
    "datasets": [
      {
        "name": "Jobs_By_Status",
        "description": "Jobs by Sales Person and Status",
        "segmentCount": 8,
        "metricCount": 3,
        "isActive": true
      }
    ]
  }
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | ✅ | Unique identifier, used in all subsequent calls |
| `description` | string | ✅ | Human-readable label shown in the UI |
| `segmentCount` | integer | — | Shown as badge; omit and it won't display |
| `metricCount` | integer | — | Shown as badge; omit and it won't display |
| `isActive` | boolean | — | Inactive datasets are hidden from pickers |

---

### 2. Dataset Detail
**`GET /api/bi/datasets/:name`**

Returns the full schema for one dataset — its segments (dimensions) and metrics. Used by the Report Builder, Data Explorer, and tile picker to render field lists.

**Response**
```json
{
  "success": true,
  "data": {
    "dataset": {
      "name": "Jobs_By_Status",
      "description": "Jobs by Sales Person and Status",
      "datasetSegments": [ /* same array as segments below */ ]
    },
    "segments": [
      {
        "segment": {
          "segmentName": "SalesPerson",
          "segmentType": "string",
          "category": "Entity",
          "isFilterable": true,
          "isGroupable": true
        },
        "columnName": "SalesPersonName",
        "displayAlias": "Sales Person"
      }
    ],
    "metrics": [
      {
        "metricName": "JobCount",
        "description": "Total number of jobs",
        "aggregationType": "COUNT",
        "displayFormat": "number",
        "decimalPlaces": 2,
        "prefix": null,
        "suffix": null,
        "isActive": true
      }
    ]
  }
}
```

#### Segment object

| Field | Type | Required | Notes |
|---|---|---|---|
| `segment.segmentName` | string | ✅ | Used as the field key in query requests and results |
| `segment.segmentType` | string | ✅ | `string` \| `number` \| `date` — drives filter UI and formatting |
| `segment.category` | string | — | e.g. `Entity`, `Date`, `Status` — used for grouping in the field picker |
| `segment.isFilterable` | boolean | — | Defaults `true`; hides from filter builder if `false` |
| `segment.isGroupable` | boolean | — | Defaults `true`; hides from group-by picker if `false` |
| `columnName` | string | — | Actual column name if different from `segmentName` |
| `displayAlias` | string | ✅ | Human-readable label shown in the UI |

> **Note:** `datasetSegments` inside the `dataset` object must be the same array as the top-level `segments` array. The Data Explorer panel reads from `dataset.datasetSegments`; the Report Builder reads from the top-level `segments`. Both must be present.

#### Metric object

| Field | Type | Required | Notes |
|---|---|---|---|
| `metricName` | string | ✅ | Key used in query requests and results |
| `aggregationType` | string | ✅ | `SUM` \| `COUNT` \| `AVG` \| `MIN` \| `MAX` \| `COUNT_DISTINCT` |
| `displayFormat` | string | — | `number` \| `currency` \| `percent` \| `auto` |
| `decimalPlaces` | integer | — | Decimal precision for display |
| `prefix` | string | — | e.g. `$` |
| `suffix` | string | — | e.g. `%` |
| `isActive` | boolean | — | Inactive metrics are hidden from pickers |

---

### 3. Query
**`POST /api/bi/query`**

The core data fetch. Used by the Report Builder, Data Explorer charts/grids, and dashboard tiles.

**Request**
```json
{
  "datasetName": "Jobs_By_Status",
  "groupBySegments": ["SalesPerson", "Status"],
  "metrics": [
    {
      "metricName": "JobCount",
      "aggregation": "SUM",
      "alias": "JobCount"
    }
  ],
  "filters": [
    {
      "segmentName": "SalesPerson",
      "operator": "in",
      "value": ["Curran, Jeff", "Curran, Larry"]
    },
    {
      "segmentName": "InvoiceDate",
      "operator": "gte",
      "value": "2026-01-01"
    },
    {
      "segmentName": "InvoiceDate",
      "operator": "lt",
      "value": "2026-04-17"
    }
  ],
  "orderBy": [
    { "field": "SalesPerson", "direction": "ASC" },
    { "field": "JobCount", "direction": "DESC" }
  ],
  "pagination": {
    "page": 1,
    "pageSize": 1000
  }
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `datasetName` | string | ✅ | |
| `groupBySegments` | string[] | — | Empty array = aggregate all rows into one |
| `metrics` | object[] | ✅ | At least one metric required |
| `metrics[].metricName` | string | ✅ | Must match a `metricName` from the dataset schema |
| `metrics[].aggregation` | string | — | Overrides dataset default; use dataset `aggregationType` if omitted |
| `metrics[].alias` | string | — | Key used in result rows; defaults to `metricName` |
| `filters` | object[] | — | Omit or pass `[]` — **do not send empty filter objects** |
| `filters[].operator` | string | ✅ | See operators table below |
| `orderBy` | object[] | — | |
| `pagination.pageSize` | integer | — | Max **1000** |

#### Filter operators

| Operator | Value type | Notes |
|---|---|---|
| `eq` | scalar | Exact match |
| `neq` | scalar | Not equal |
| `in` | array | Value must be an array even for one item |
| `not_in` | array | |
| `gte` | scalar | ≥ |
| `lte` | scalar | ≤ |
| `lt` | scalar | < |
| `gt` | scalar | > |
| `contains` | string | Substring match |
| `starts` | string | Prefix match |
| `is_null` | — | No value field needed |
| `not_null` | — | No value field needed |

> **Date range convention:** Use `gte startDate` AND `lt dayAfterEndDate` (midnight boundary). Do not use a `between` operator for dates.

**Response**
```json
{
  "success": true,
  "data": {
    "data": [
      { "SalesPerson": "Curran, Larry", "JobCount": 42 }
    ],
    "metadata": {
      "totalRows": 42,
      "executionTimeMs": 65
    }
  }
}
```

| Field | Type | Notes |
|---|---|---|
| `data.data` | object[] | Result rows; keys are `segmentName` / metric `alias` |
| `data.metadata` | object | Optional; used for debug panel |

**Error response**
```json
{
  "success": false,
  "error": { "message": "Metric 'X' not available for dataset 'Y'" },
  "timestamp": "2026-04-16T18:46:07.000Z"
}
```

---

### 4. KPIs
**`POST /api/bi/kpis`**

Returns aggregated totals for one or more metrics across the full dataset (optionally filtered). Used by dashboard KPI tiles and the Report Builder KPI bar.

**Request** — same shape as `/bi/query` but `groupBySegments` is typically empty.

**Response**
```json
{
  "success": true,
  "data": {
    "kpis": [
      { "name": "JobCount", "value": 1284 },
      { "name": "UniqueCustomers", "value": 312 }
    ]
  }
}
```

| Field | Type | Notes |
|---|---|---|
| `kpis[].name` | string | Matches metric `alias` from the request |
| `kpis[].value` | number | Aggregated scalar |

> **Adapter note:** If the upstream has no dedicated KPI endpoint, derive by running a `groupBySegments: []` query and summing each metric column across all result rows.

---

### 5. Segment Values
**`GET /api/bi/segment-values?datasetName=:name&segmentName=:segment`**

Returns the distinct values for a segment, used to populate filter dropdowns with type-ahead selects in the Report Builder and Data Explorer.

**Response**
```json
{
  "success": true,
  "data": {
    "values": [
      { "value": "Curran, Jeff", "displayValue": "Curran, Jeff" },
      { "value": "Curran, Larry", "displayValue": "Curran, Larry" }
    ]
  }
}
```

| Field | Type | Notes |
|---|---|---|
| `value` | string | Raw value sent in filter requests |
| `displayValue` | string | Label shown in the dropdown (can differ, e.g. ID vs name) |

> **Adapter note:** If the upstream has no dedicated endpoint, derive by running a grouped query on the segment with a `COUNT` metric and extracting the distinct values from the result rows.

---

## Adapter Checklist

When wiring up a new instance, verify each endpoint returns the correct shape:

- [ ] `GET /api/bi/datasets` → `{ success, data: { datasets: [{name, description}] } }`
- [ ] `GET /api/bi/datasets/:name` → `{ success, data: { dataset: { datasetSegments }, segments, metrics } }`
  - [ ] `segments[].segment.segmentName` present
  - [ ] `segments[].segment.segmentType` is `string` | `number` | `date`
  - [ ] `segments[].displayAlias` present
  - [ ] `metrics[].metricName` present
  - [ ] `metrics[].aggregationType` present
  - [ ] `dataset.datasetSegments` is the same array as top-level `segments`
- [ ] `POST /api/bi/query` → `{ success, data: { data: [...rows] } }`
  - [ ] Row keys match `segmentName` and metric `alias`
  - [ ] Empty `filters: []` does not cause a 400 (strip on adapter side if needed)
- [ ] `POST /api/bi/kpis` → `{ success, data: { kpis: [{name, value}] } }`
- [ ] `GET /api/bi/segment-values` → `{ success, data: { values: [{value, displayValue}] } }`

---

## Supported segmentTypes

| Type | Filter UI | Display |
|---|---|---|
| `string` | Type-ahead multi-select (from segment-values) | Left-aligned text |
| `number` | Range inputs (gte / lte) | Right-aligned, formatted per `displayFormat` |
| `date` | Date range picker (gte + lt boundary) | Formatted per column config |

---

## Known Instance Notes

### Example: fcc-adapter (crane/rental client)
- Native implementation — no adapter needed
- `filters: []` causes a 400 — the proxy strips empty filter arrays before forwarding
- `pageSize` max: 1000
- Segment-values uses `datasetName` + `segmentName` query params (not `dataset` + `segment`)

### Example: insight-adapter (dry cleaning client)
- Adapter in `server.js` (`adapter: 'insight'`)
- Auth: raw token in `Authorization` header (no `Bearer` prefix)
- No dedicated KPI endpoint — derived from grouped query
- No dedicated segment-values endpoint — derived from grouped query with COUNT
- `POST /api/v1/datasets/:name/query` on `dev-api` currently returns 502 (upstream service issue — not a spec problem)
- Dataset list uses a map shape `{ datasetName: { description, segments, metrics } }` — normalized by adapter
