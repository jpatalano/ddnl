const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway.internal')
    ? false  // internal Railway network — no SSL needed
    : { rejectUnauthorized: false }
});

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS clients (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) UNIQUE NOT NULL,
    name        VARCHAR(255),
    created_at  TIMESTAMPTZ DEFAULT NOW()
  );

  CREATE TABLE IF NOT EXISTS users (
    id           SERIAL PRIMARY KEY,
    client_id    VARCHAR(255) NOT NULL,
    entra_oid    VARCHAR(255) UNIQUE NOT NULL,
    email        VARCHAR(255),
    display_name VARCHAR(255),
    role         VARCHAR(64) DEFAULT 'viewer',
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS saved_reports (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL,
    created_by  VARCHAR(255),  -- entra_oid
    name        VARCHAR(255) NOT NULL,
    description TEXT,
    config      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS dashboards (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL,
    created_by  VARCHAR(255),  -- entra_oid
    name        VARCHAR(255) NOT NULL,
    config      JSONB NOT NULL DEFAULT '{}',
    is_default  BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS saved_charts (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL,
    created_by  VARCHAR(255),  -- entra_oid
    name        VARCHAR(255) NOT NULL,
    dataset     VARCHAR(255),
    config      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS schedules (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL,
    report_id   INTEGER REFERENCES saved_reports(id) ON DELETE CASCADE,
    name        VARCHAR(255),
    cron        VARCHAR(128),
    last_run    TIMESTAMPTZ,
    next_run    TIMESTAMPTZ,
    config      JSONB NOT NULL DEFAULT '{}',
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
  );

  -- Seed one client row per instance (instance id is used as client_id)
  INSERT INTO clients (client_id, name) VALUES ('demo',   'Demo')   ON CONFLICT (client_id) DO NOTHING;
  INSERT INTO clients (client_id, name) VALUES ('fcc',    'FCC')    ON CONFLICT (client_id) DO NOTHING;
`;

// Keep SCHEMA clean — additive changes go in MIGRATIONS only
const SCHEMA_APPEND = `
  CREATE TABLE IF NOT EXISTS report_versions (
    id             SERIAL PRIMARY KEY,
    report_id      INTEGER NOT NULL REFERENCES saved_reports(id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL,
    name           VARCHAR(255) NOT NULL,
    description    TEXT,
    config         JSONB NOT NULL DEFAULT '{}',
    saved_by       VARCHAR(255),   -- email or display name
    saved_at       TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE UNIQUE INDEX IF NOT EXISTS report_versions_report_ver
    ON report_versions(report_id, version_number);
`;

// Migrations run in order — safe to add new ones, never edit existing ones
const MIGRATIONS = [
  // 001 — add saved_charts table (may not exist on DBs initialized before this was added)
  `CREATE TABLE IF NOT EXISTS saved_charts (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL,
    created_by  VARCHAR(255),
    name        VARCHAR(255) NOT NULL,
    dataset     VARCHAR(255),
    config      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
  )`,
  // 002 — report versioning
  `CREATE TABLE IF NOT EXISTS report_versions (
    id             SERIAL PRIMARY KEY,
    report_id      INTEGER NOT NULL REFERENCES saved_reports(id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL,
    name           VARCHAR(255) NOT NULL,
    description    TEXT,
    config         JSONB NOT NULL DEFAULT '{}',
    saved_by       VARCHAR(255),
    saved_at       TIMESTAMPTZ DEFAULT NOW()
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS report_versions_report_ver
    ON report_versions(report_id, version_number)`,

  // 003 — instance API keys (per-instance token for ingest)
  `CREATE TABLE IF NOT EXISTS instance_api_keys (
    id           SERIAL PRIMARY KEY,
    client_id    VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    key_hash     VARCHAR(255) NOT NULL UNIQUE,  -- SHA-256 hex of the raw key
    label        VARCHAR(255),                  -- human label e.g. "Produce Stand Sync"
    created_by   VARCHAR(255),
    last_used_at TIMESTAMPTZ,
    revoked      BOOLEAN DEFAULT FALSE,
    created_at   TIMESTAMPTZ DEFAULT NOW()
  )`,

  // 004 — dataset definitions (one row per dataset per instance)
  `CREATE TABLE IF NOT EXISTS dataset_definitions (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,       -- e.g. 'sales'
    label           VARCHAR(255) NOT NULL,       -- e.g. 'Sales Transactions'
    description     TEXT,
    current_version INTEGER NOT NULL DEFAULT 1,
    es_alias        VARCHAR(255),                -- set after first index create
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(client_id, name)
  )`,

  // 005 — dataset schema versions (immutable once published)
  `CREATE TABLE IF NOT EXISTS dataset_schema_versions (
    id              SERIAL PRIMARY KEY,
    dataset_id      INTEGER NOT NULL REFERENCES dataset_definitions(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL,
    fields          JSONB NOT NULL DEFAULT '[]',  -- array of field definitions
    es_index        VARCHAR(255),                 -- actual ES index name for this version
    published_at    TIMESTAMPTZ,
    published_by    VARCHAR(255),
    compat_status   VARCHAR(64) DEFAULT 'unknown', -- 'ok' | 'breaking' | 'additive'
    compat_notes    JSONB DEFAULT '[]',            -- list of change descriptions
    UNIQUE(dataset_id, version)
  )`,

  // 006 — ingest log (one row per bulk push job)
  `CREATE TABLE IF NOT EXISTS ingest_log (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(255) NOT NULL,
    dataset_name    VARCHAR(255) NOT NULL,
    operation       VARCHAR(64) NOT NULL,   -- 'bulk' | 'single' | 'reindex' | 'replace'
    doc_count       INTEGER DEFAULT 0,
    failed_count    INTEGER DEFAULT 0,
    errors          JSONB DEFAULT '[]',
    duration_ms     INTEGER,
    triggered_by    VARCHAR(255),           -- api_key label or 'system'
    created_at      TIMESTAMPTZ DEFAULT NOW()
  )`,

  // 007 — dataset field invalidations (reports/charts referencing removed/changed fields)
  `CREATE TABLE IF NOT EXISTS schema_invalidations (
    id              SERIAL PRIMARY KEY,
    dataset_id      INTEGER NOT NULL REFERENCES dataset_definitions(id) ON DELETE CASCADE,
    from_version    INTEGER NOT NULL,
    to_version      INTEGER NOT NULL,
    resource_type   VARCHAR(64) NOT NULL,  -- 'report' | 'chart' | 'dashboard_tile'
    resource_id     INTEGER NOT NULL,
    resource_name   VARCHAR(255),
    field_name      VARCHAR(255) NOT NULL,
    change_type     VARCHAR(64) NOT NULL,  -- 'removed' | 'type_changed' | 'renamed'
    resolved        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
  )`,

  // 008 — dataset field metadata (Designer layer — display config, formatting, relationships)
  // One row per field per dataset. Persists across schema versions; Designer edits these.
  `CREATE TABLE IF NOT EXISTS dataset_field_metadata (
    id              SERIAL PRIMARY KEY,
    dataset_id      INTEGER NOT NULL REFERENCES dataset_definitions(id) ON DELETE CASCADE,
    field_name      VARCHAR(255) NOT NULL,   -- matches field name in ES / schema_versions
    label           VARCHAR(255),            -- display label override
    display_field   VARCHAR(255),            -- e.g. 'store_id' → display via 'store_name' (top_hits)
    field_type      VARCHAR(64),             -- 'segment' | 'metric' | 'date' | 'computed'
    format          VARCHAR(64),             -- 'currency' | 'number' | 'percent' | 'date' | 'text'
    currency_symbol VARCHAR(16),             -- '$', '€', '£', etc.
    decimal_places  SMALLINT DEFAULT 2,
    date_format     VARCHAR(64),             -- 'MM/DD/YYYY' | 'MMM D' | 'YYYY-MM-DD' etc.
    prefix          VARCHAR(64),             -- prepend to rendered value
    suffix          VARCHAR(64),             -- append to rendered value
    is_hidden       BOOLEAN DEFAULT FALSE,   -- suppress as a column (still usable as filter/group)
    sort_order      INTEGER DEFAULT 0,       -- Designer drag order
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_by      VARCHAR(255),            -- entra_oid or 'system'
    UNIQUE(dataset_id, field_name)
  )`,

  // 009a — fiscal calendar definitions (one per client, configurable in admin)
  `CREATE TABLE IF NOT EXISTS fiscal_calendars (
    id                       SERIAL PRIMARY KEY,
    client_id                VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    name                     VARCHAR(255) NOT NULL DEFAULT 'Default',
    fiscal_year_start_month  SMALLINT NOT NULL DEFAULT 1,   -- 1=Jan, 2=Feb, etc.
    fiscal_year_start_day    SMALLINT NOT NULL DEFAULT 1,
    week_start_day           SMALLINT NOT NULL DEFAULT 1,   -- 1=Mon (ISO), 7=Sun
    week_scheme              VARCHAR(32) NOT NULL DEFAULT 'iso',  -- 'iso' | '4-4-5' | '4-5-4' | '5-4-4'
    description              TEXT,
    is_active                BOOLEAN DEFAULT TRUE,
    created_at               TIMESTAMPTZ DEFAULT NOW(),
    updated_at               TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(client_id, name)
  )`,

  // 009b — fiscal day dimension (one row per calendar date per fiscal_calendar)
  // Pre-computed at seed/admin time. Join key: fiscal_days.calendar_date = sales.date
  `CREATE TABLE IF NOT EXISTS fiscal_days (
    id                    SERIAL PRIMARY KEY,
    calendar_id           INTEGER NOT NULL REFERENCES fiscal_calendars(id) ON DELETE CASCADE,
    calendar_date         DATE NOT NULL,

    -- Calendar basics
    day_of_week           SMALLINT NOT NULL,   -- 1=Mon ... 7=Sun (ISO)
    day_name              VARCHAR(16) NOT NULL, -- 'Monday'
    day_name_short        VARCHAR(4) NOT NULL,  -- 'Mon'
    is_weekend            BOOLEAN NOT NULL DEFAULT FALSE,
    is_holiday            BOOLEAN NOT NULL DEFAULT FALSE,  -- stub; set via admin later
    holiday_name          VARCHAR(255),

    -- ISO calendar week (for week-over-week regardless of fiscal)
    iso_week              SMALLINT NOT NULL,    -- 1-53
    iso_week_year         SMALLINT NOT NULL,    -- year the ISO week belongs to
    iso_week_start        DATE NOT NULL,        -- Monday of that ISO week
    iso_week_label        VARCHAR(16) NOT NULL, -- 'W01 2024'

    -- Fiscal week
    fiscal_week           SMALLINT NOT NULL,    -- 1-52/53
    fiscal_week_label     VARCHAR(16) NOT NULL, -- 'FW01'
    fiscal_week_start     DATE NOT NULL,        -- first day of this fiscal week

    -- Fiscal month
    fiscal_month          SMALLINT NOT NULL,    -- 1-12
    fiscal_month_name     VARCHAR(16) NOT NULL, -- 'January'
    fiscal_month_short    VARCHAR(4) NOT NULL,  -- 'Jan'
    fiscal_month_label    VARCHAR(8) NOT NULL,  -- 'FM01'
    fiscal_month_start    DATE NOT NULL,
    fiscal_month_end      DATE NOT NULL,

    -- Fiscal quarter
    fiscal_quarter        SMALLINT NOT NULL,    -- 1-4
    fiscal_quarter_label  VARCHAR(8) NOT NULL,  -- 'FQ1'
    fiscal_quarter_start  DATE NOT NULL,
    fiscal_quarter_end    DATE NOT NULL,

    -- Fiscal year
    fiscal_year           SMALLINT NOT NULL,    -- 2024, 2025, ...
    fiscal_year_label     VARCHAR(8) NOT NULL,  -- 'FY2024'
    fiscal_year_start     DATE NOT NULL,
    fiscal_year_end       DATE NOT NULL,

    -- Prior-period keys (pre-computed for SWLY / SDLY queries — no runtime math)
    same_day_last_year    DATE,                 -- exact -365d / -366d accounting for leap year
    same_week_start_lyr   DATE,                 -- fiscal_week_start one fiscal year prior
    same_month_start_lyr  DATE,                 -- fiscal_month_start one fiscal year prior
    same_quarter_start_lyr DATE,                -- fiscal_quarter_start one fiscal year prior

    UNIQUE(calendar_id, calendar_date)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_fiscal_days_date      ON fiscal_days(calendar_date)`,
  `CREATE INDEX IF NOT EXISTS idx_fiscal_days_cal_date  ON fiscal_days(calendar_id, calendar_date)`,
  `CREATE INDEX IF NOT EXISTS idx_fiscal_days_fy_fq     ON fiscal_days(calendar_id, fiscal_year, fiscal_quarter)`,
  `CREATE INDEX IF NOT EXISTS idx_fiscal_days_fy_fm     ON fiscal_days(calendar_id, fiscal_year, fiscal_month)`,

  // 010a — lookup dataset definitions
  // A lookup dataset is a small, keyed master-record table managed in Postgres.
  // Examples: Stores, Employees, Products, Vendors.
  // Any ES dataset field can declare a relationship to a lookup via dataset_field_metadata.
  `CREATE TABLE IF NOT EXISTS lookup_datasets (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    name        VARCHAR(255) NOT NULL,   -- machine name e.g. 'stores'
    label       VARCHAR(255) NOT NULL,   -- display name e.g. 'Stores'
    description TEXT,
    key_field   VARCHAR(255) NOT NULL,   -- the join key field name e.g. 'store_id'
    icon        VARCHAR(64),             -- optional lucide icon name for Designer UI
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(client_id, name)
  )`,

  // 010b — lookup dataset field definitions
  // Describes every field the lookup dataset carries (key field + all enrichment fields).
  // Controls what columns appear in the Designer table editor and drill-down panel.
  `CREATE TABLE IF NOT EXISTS lookup_dataset_fields (
    id                SERIAL PRIMARY KEY,
    lookup_dataset_id INTEGER NOT NULL REFERENCES lookup_datasets(id) ON DELETE CASCADE,
    field_name        VARCHAR(255) NOT NULL,  -- e.g. 'store_name', 'city', 'manager'
    label             VARCHAR(255) NOT NULL,  -- e.g. 'Store Name'
    field_type        VARCHAR(64)  NOT NULL DEFAULT 'text',  -- 'text' | 'number' | 'date' | 'boolean' | 'url'
    is_key_field      BOOLEAN DEFAULT FALSE,  -- true for the join key (store_id)
    is_display_field  BOOLEAN DEFAULT FALSE,  -- true for the primary human label (store_name)
    is_required       BOOLEAN DEFAULT FALSE,  -- validation in the UI table editor
    format            VARCHAR(64),            -- same format tokens as dataset_field_metadata
    sort_order        INTEGER DEFAULT 0,
    UNIQUE(lookup_dataset_id, field_name)
  )`,

  // 010c — lookup dataset rows (the actual master records)
  // key_value is always a string (coerced at write time).
  // data JSONB holds all non-key enrichment fields.
  // Rows can be auto-populated by ingest (extract distinct key+display pairs) or
  // manually managed in the Designer table editor, or both — manual wins on conflict.
  `CREATE TABLE IF NOT EXISTS lookup_dataset_rows (
    id                SERIAL PRIMARY KEY,
    lookup_dataset_id INTEGER NOT NULL REFERENCES lookup_datasets(id) ON DELETE CASCADE,
    key_value         VARCHAR(255) NOT NULL,  -- e.g. 'store_001'
    data              JSONB NOT NULL DEFAULT '{}',  -- { store_name, city, region, manager, ... }
    auto_populated    BOOLEAN DEFAULT FALSE,  -- true = written by ingest; false = manually managed
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(lookup_dataset_id, key_value)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_lookup_rows_dataset_key ON lookup_dataset_rows(lookup_dataset_id, key_value)`,

  // 010d — wire field relationships: add lookup columns to dataset_field_metadata
  // These are additive ALTER TABLE statements — safe to re-run (IF NOT EXISTS / DO NOTHING).
  // lookup_dataset_id NULL  → co-indexed (use display_field, same ES doc)
  // lookup_dataset_id SET   → resolve via lookup_dataset_rows JOIN on key_value
  `ALTER TABLE dataset_field_metadata
    ADD COLUMN IF NOT EXISTS lookup_dataset_id INTEGER REFERENCES lookup_datasets(id) ON DELETE SET NULL`,
  // lookup_key_field: field in the lookup dataset to match against (defaults to lookup's key_field if null)
  `ALTER TABLE dataset_field_metadata
    ADD COLUMN IF NOT EXISTS lookup_key_field VARCHAR(255)`,
  // 011 — ingest channels (one row per dataset × channel config)
  // A dataset can have multiple channels simultaneously (e.g. nightly CSV + realtime webhook).
  // Each channel has its own id_field (upsert key), mode, schedule, and method-specific options.
  // Methods: 'api_push' | 'csv' | 'webhook' | 'sftp'
  // Modes:   'batch' (full or delta) | 'realtime' (one doc at a time)
  `CREATE TABLE IF NOT EXISTS ingest_channels (
    id             SERIAL PRIMARY KEY,
    dataset_id     INTEGER NOT NULL REFERENCES dataset_definitions(id) ON DELETE CASCADE,
    client_id      VARCHAR(255) NOT NULL,
    channel_name   VARCHAR(255) NOT NULL,            -- human label e.g. 'Nightly CSV'
    method         VARCHAR(64) NOT NULL,             -- 'api_push' | 'csv' | 'webhook' | 'sftp'
    mode           VARCHAR(32) NOT NULL DEFAULT 'batch', -- 'batch' | 'realtime'
    id_field       VARCHAR(255),                     -- ES _id field for upserts (null = auto-generate)
    is_active      BOOLEAN DEFAULT TRUE,
    options        JSONB NOT NULL DEFAULT '{}',      -- method-specific: sftp_path, webhook_secret, schedule, etc.
    last_run_at    TIMESTAMPTZ,
    last_run_count INTEGER,                          -- docs indexed in last run
    last_run_ok    BOOLEAN,
    webhook_token  VARCHAR(255),                     -- auto-generated token for webhook URL
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(dataset_id, channel_name)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ingest_channels_dataset ON ingest_channels(dataset_id)`,
  `CREATE INDEX IF NOT EXISTS idx_ingest_channels_webhook ON ingest_channels(webhook_token) WHERE webhook_token IS NOT NULL`,
];

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(SCHEMA);
    // Run any additive migrations
    for (const sql of MIGRATIONS) {
      await client.query(sql);
    }
    console.log('DB schema initialized');
  } finally {
    client.release();
  }
}

module.exports = { pool, initDb };
