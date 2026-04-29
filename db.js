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
  -- Clients are created at setup time via the admin API, not seeded here.
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

  // 012 — upsert primary key fields + event types on dataset_definitions
  `ALTER TABLE dataset_definitions
    ADD COLUMN IF NOT EXISTS primary_key_fields JSONB DEFAULT '[]'`,
  `ALTER TABLE dataset_definitions
    ADD COLUMN IF NOT EXISTS event_types JSONB DEFAULT '[]'`,

  // 013 — webhook subscriptions (one row per webhook per dataset)
  `CREATE TABLE IF NOT EXISTS dataset_webhooks (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    dataset_id      INTEGER NOT NULL REFERENCES dataset_definitions(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    endpoint_url    TEXT NOT NULL,
    event_triggers  JSONB NOT NULL DEFAULT '{}',   -- { "created": true, "updated": true, ... }
    field_map       JSONB NOT NULL DEFAULT '{}',   -- { "source_field": "dest_field" }
    auth_header     TEXT,                          -- value for Authorization header (inbound verify)
    hmac_secret     TEXT,                          -- HMAC SHA-256 shared secret
    custom_headers  JSONB NOT NULL DEFAULT '[]',   -- [{ key, value }]
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_dataset_webhooks_client  ON dataset_webhooks(client_id)`,
  `CREATE INDEX IF NOT EXISTS idx_dataset_webhooks_dataset ON dataset_webhooks(dataset_id)`,

  // 014 — webhook delivery log
  `CREATE TABLE IF NOT EXISTS webhook_delivery_log (
    id               SERIAL PRIMARY KEY,
    webhook_id       INTEGER NOT NULL REFERENCES dataset_webhooks(id) ON DELETE CASCADE,
    client_id        VARCHAR(255) NOT NULL,
    received_at      TIMESTAMPTZ DEFAULT NOW(),
    status           VARCHAR(32) NOT NULL,   -- 'processed' | 'rejected' | 'failed' | 'skipped'
    event_type       VARCHAR(128),
    records_in       INTEGER DEFAULT 0,
    records_upserted INTEGER DEFAULT 0,
    records_failed   INTEGER DEFAULT 0,
    error_message    TEXT,
    duration_ms      INTEGER,
    payload_preview  JSONB
  )`,
  `CREATE INDEX IF NOT EXISTS idx_webhook_log_webhook   ON webhook_delivery_log(webhook_id)`,
  `CREATE INDEX IF NOT EXISTS idx_webhook_log_client    ON webhook_delivery_log(client_id)`,
  `CREATE INDEX IF NOT EXISTS idx_webhook_log_status    ON webhook_delivery_log(status)`,
  `CREATE INDEX IF NOT EXISTS idx_webhook_log_received  ON webhook_delivery_log(received_at DESC)`,

  // 015 — _status system field support
  // Stores which source column maps to _status during file import.
  `ALTER TABLE dataset_definitions
    ADD COLUMN IF NOT EXISTS status_source_field VARCHAR(255) DEFAULT NULL`,

  // dataset_versions table for file-import define flow
  `CREATE TABLE IF NOT EXISTS dataset_versions (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(255) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    label           VARCHAR(255),
    version         INTEGER NOT NULL DEFAULT 1,
    fields          JSONB NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_dataset_versions_client_name ON dataset_versions(client_id, name)`,

  // 016 — dataset_type column on dataset_definitions
  // 'client' = user-created, 'provided' = system-provided (weather, fiscal, etc.)
  `ALTER TABLE dataset_definitions
    ADD COLUMN IF NOT EXISTS dataset_type VARCHAR(32) NOT NULL DEFAULT 'client'
    CHECK (dataset_type IN ('client','provided'))`,

  // 017 — show_on_explorer flag: controls whether dataset appears in the Explorer
  `ALTER TABLE dataset_definitions
    ADD COLUMN IF NOT EXISTS show_on_explorer BOOLEAN NOT NULL DEFAULT true`,

  // 018 — Dashboard Wizard tables
  // wizard_domain: top-level domain cards (Q2C, Equipment Usage, ...)
  `CREATE TABLE IF NOT EXISTS wizard_domain (
    id            SERIAL PRIMARY KEY,
    key           TEXT UNIQUE NOT NULL,
    label         TEXT NOT NULL,
    tagline       TEXT,
    icon          TEXT,
    instance_scope TEXT[] NOT NULL DEFAULT '{}',
    sort_order    INT NOT NULL DEFAULT 0,
    is_active     BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`,

  // wizard_stage: one canvas node per stage
  `CREATE TABLE IF NOT EXISTS wizard_stage (
    id                  SERIAL PRIMARY KEY,
    domain_id           INT NOT NULL REFERENCES wizard_domain(id) ON DELETE CASCADE,
    key                 TEXT NOT NULL,
    label               TEXT NOT NULL,
    summary             TEXT,
    narrative           TEXT,
    sort_order          INT NOT NULL DEFAULT 0,
    required_datasets   TEXT[] NOT NULL DEFAULT '{}',
    persona_tags        TEXT[] NOT NULL DEFAULT '{}',
    failure_mode_keys   TEXT[] NOT NULL DEFAULT '{}',
    UNIQUE(domain_id, key)
  )`,

  // wizard_tile_template: reusable tile definitions
  `CREATE TABLE IF NOT EXISTS wizard_tile_template (
    id                  SERIAL PRIMARY KEY,
    key                 TEXT UNIQUE NOT NULL,
    label               TEXT NOT NULL,
    tile_type           TEXT NOT NULL,
    dataset_key         TEXT NOT NULL,
    metric_expression   JSONB NOT NULL,
    group_by            TEXT[] NOT NULL DEFAULT '{}',
    filters             JSONB,
    time_window         TEXT,
    compare_to          TEXT,
    persona_tags        TEXT[] NOT NULL DEFAULT '{}',
    size                TEXT NOT NULL DEFAULT '1x1',
    narrative           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`,

  // wizard_stage_tile: junction — which tiles belong to which stage
  `CREATE TABLE IF NOT EXISTS wizard_stage_tile (
    stage_id            INT NOT NULL REFERENCES wizard_stage(id) ON DELETE CASCADE,
    tile_template_id    INT NOT NULL REFERENCES wizard_tile_template(id) ON DELETE CASCADE,
    sort_order          INT NOT NULL DEFAULT 0,
    is_primary          BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (stage_id, tile_template_id)
  )`,

  // wizard_run: audit trail of every wizard session
  `CREATE TABLE IF NOT EXISTS wizard_run (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           TEXT NOT NULL,
    instance_id         TEXT NOT NULL,
    domain_id           INT REFERENCES wizard_domain(id),
    selected_stage_ids  INT[] NOT NULL DEFAULT '{}',
    dashboard_id        TEXT,
    tile_count          INT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'in_progress'
      CHECK (status IN ('in_progress','completed','abandoned')),
    notes               TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_wizard_run_tenant ON wizard_run(tenant_id, instance_id, started_at DESC)`,

  // 019a — date_field as a first-class column on dataset_definitions
  // Previously only tracked inside the schema fields JSONB array.
  // Now promoted so the ingest API and query layer can read it directly.
  `ALTER TABLE dataset_definitions
    ADD COLUMN IF NOT EXISTS date_field VARCHAR(255) DEFAULT NULL`,

  // 019b — dataset_relations
  // Declares relationships between datasets within the same client.
  //
  // relation_type:
  //   belongs_to  — source has a FK pointing at one record in target (many-to-one)
  //   has_many    — source is the parent; target records FK back to source (one-to-many)
  //
  // pull_fields:
  //   JSONB array of field names from the target dataset to auto-stamp onto the
  //   source record at upsert time. Only applies to belongs_to relations.
  //   e.g. ["CustomerName", "Region", "SalesTerritory"]
  //
  // Example: jobs belongs_to customers on CustomerId, pull CustomerName + Region
  `CREATE TABLE IF NOT EXISTS dataset_relations (
    id                  SERIAL PRIMARY KEY,
    client_id           VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    source_dataset_id   INTEGER NOT NULL REFERENCES dataset_definitions(id) ON DELETE CASCADE,
    source_field        VARCHAR(255) NOT NULL,   -- FK field on the source record
    target_dataset      VARCHAR(255) NOT NULL,   -- name (key) of the target dataset
    target_field        VARCHAR(255) NOT NULL,   -- PK field on the target record to match
    relation_type       VARCHAR(32)  NOT NULL DEFAULT 'belongs_to'
      CHECK (relation_type IN ('belongs_to', 'has_many')),
    pull_fields         JSONB        NOT NULL DEFAULT '[]',  -- fields to stamp at upsert time
    label               VARCHAR(255),            -- optional display name, e.g. "Customer"
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE(source_dataset_id, source_field, target_dataset)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_dataset_relations_source ON dataset_relations(source_dataset_id)`,
  `CREATE INDEX IF NOT EXISTS idx_dataset_relations_client ON dataset_relations(client_id)`,

  // 020a — client_tags: tag registry per client
  // tag_type: 'system' (rule-driven, admin-managed) | 'user' (manually assigned, never touched by system)
  // target_dataset: which dataset this tag applies to (e.g. 'customers', 'employees')
  `CREATE TABLE IF NOT EXISTS client_tags (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,    -- machine key, e.g. 'lost_tuna'
    label           VARCHAR(255) NOT NULL,    -- display name, e.g. 'Lost Tuna'
    color           VARCHAR(32)  DEFAULT '#6366f1', -- hex color for UI pills
    tag_type        VARCHAR(16)  NOT NULL DEFAULT 'user'
      CHECK (tag_type IN ('system', 'user')),
    target_dataset  VARCHAR(255) NOT NULL,    -- dataset name key this tag applies to
    description     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(client_id, name, target_dataset)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_client_tags_client  ON client_tags(client_id)`,
  `CREATE INDEX IF NOT EXISTS idx_client_tags_dataset ON client_tags(client_id, target_dataset)`,

  // 020b — client_tag_assignments: record-level tag assignments
  // record_id: the ES _id of the tagged record
  // assigned_by: 'system' for rule-driven, or a user identifier for user tags
  // Postgres is the source of truth; __tags on the ES doc is the denormalized query copy.
  `CREATE TABLE IF NOT EXISTS client_tag_assignments (
    id              SERIAL PRIMARY KEY,
    client_id       VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    tag_id          INTEGER NOT NULL REFERENCES client_tags(id) ON DELETE CASCADE,
    dataset_name    VARCHAR(255) NOT NULL,
    record_id       VARCHAR(255) NOT NULL,    -- ES _id
    assigned_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by     VARCHAR(255) NOT NULL DEFAULT 'system',
    UNIQUE(tag_id, record_id)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_tag_assignments_client  ON client_tag_assignments(client_id)`,
  `CREATE INDEX IF NOT EXISTS idx_tag_assignments_tag     ON client_tag_assignments(tag_id)`,
  `CREATE INDEX IF NOT EXISTS idx_tag_assignments_record  ON client_tag_assignments(dataset_name, record_id)`,

  // 020c — system_tag_rules: apply/remove rules for system tags
  // One row per rule per tag. A tag can have multiple rules (e.g. one apply + one remove).
  //
  // trigger_type:
  //   'schedule'   — evaluated on a nightly/scheduled run
  //   'on_ingest'  — evaluated after records are ingested into trigger_dataset
  //
  // rule_action: 'apply' | 'remove'
  //
  // conditions: array of { segmentName, operator, value } filter objects.
  //   Applied to the target_dataset records (i.e. the records being tagged).
  //   Pre-computed fields expected on the record (e.g. TotalSpend, LastJobDate).
  //
  // trigger_dataset: for on_ingest rules, the dataset whose ingest fires this rule.
  //   e.g. for Lost Tuna remove: trigger_dataset='jobs', conditions check CustomerId match.
  `CREATE TABLE IF NOT EXISTS system_tag_rules (
    id              SERIAL PRIMARY KEY,
    tag_id          INTEGER NOT NULL REFERENCES client_tags(id) ON DELETE CASCADE,
    client_id       VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    rule_action     VARCHAR(16)  NOT NULL CHECK (rule_action IN ('apply', 'remove')),
    trigger_type    VARCHAR(16)  NOT NULL CHECK (trigger_type IN ('schedule', 'on_ingest')),
    trigger_dataset VARCHAR(255),             -- required when trigger_type = 'on_ingest'
    conditions      JSONB NOT NULL DEFAULT '[]', -- [{ segmentName, operator, value }]
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at     TIMESTAMPTZ,
    last_run_count  INTEGER DEFAULT 0,        -- records affected on last run
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_system_tag_rules_tag     ON system_tag_rules(tag_id)`,
  `CREATE INDEX IF NOT EXISTS idx_system_tag_rules_client  ON system_tag_rules(client_id)`,
  `CREATE INDEX IF NOT EXISTS idx_system_tag_rules_trigger ON system_tag_rules(trigger_type, trigger_dataset)`,

  // 021 — visible_to_ai flag on dataset_definitions
  // Controls whether a dataset is included in the AI Chat schema context + Advise queries.
  `ALTER TABLE dataset_definitions ADD COLUMN IF NOT EXISTS visible_to_ai BOOLEAN NOT NULL DEFAULT FALSE`,
  // 021 backfill — enable AI visibility for all existing datasets
  `UPDATE dataset_definitions SET visible_to_ai = TRUE WHERE visible_to_ai = FALSE`,

  // 021a — advise_snapshots: one row per scheduled advise run per client
  // Stores the full findings array as JSONB so the UI reads from cache, not live ES.
  `CREATE TABLE IF NOT EXISTS advise_snapshots (
    id            SERIAL PRIMARY KEY,
    client_id     VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    ran_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finding_count INTEGER      NOT NULL DEFAULT 0,
    duration_ms   INTEGER,
    status        VARCHAR(32)  NOT NULL DEFAULT 'ok'  -- 'ok' | 'partial' | 'error'
  )`,
  `CREATE INDEX IF NOT EXISTS idx_advise_snapshots_client ON advise_snapshots(client_id, ran_at DESC)`,

  // 021b — advise_findings: individual finding rows linked to a snapshot
  //
  // category   : 'equipment' | 'yard' | 'customer' | 'financial'
  // severity   : 'critical' | 'watch' | 'opportunity'
  // metric_key : machine key matching a KPI tile type (e.g. 'utilization_rate', 'idle_days')
  //              Used by the UI to show badge icons on matching tiles.
  // entity_type: what the finding is about (e.g. 'equipment', 'yard', 'customer')
  // entity_id  : the ES _id or record identifier
  // entity_label: human-readable name (e.g. 'Crane #4412', 'Denver Yard')
  // data_json  : raw numbers/context used to generate the recommendation
  // recommendation: LLM-generated text
  `CREATE TABLE IF NOT EXISTS advise_findings (
    id              SERIAL PRIMARY KEY,
    snapshot_id     INTEGER      NOT NULL REFERENCES advise_snapshots(id) ON DELETE CASCADE,
    client_id       VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    category        VARCHAR(32)  NOT NULL,
    severity        VARCHAR(16)  NOT NULL DEFAULT 'watch'
      CHECK (severity IN ('critical', 'watch', 'opportunity')),
    metric_key      VARCHAR(128) NOT NULL,
    entity_type     VARCHAR(64),
    entity_id       VARCHAR(255),
    entity_label    VARCHAR(255),
    data_json       JSONB        NOT NULL DEFAULT '{}',
    recommendation  TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_advise_findings_snapshot  ON advise_findings(snapshot_id)`,
  `CREATE INDEX IF NOT EXISTS idx_advise_findings_client    ON advise_findings(client_id)`,
  `CREATE INDEX IF NOT EXISTS idx_advise_findings_category  ON advise_findings(client_id, category)`,
  `CREATE INDEX IF NOT EXISTS idx_advise_findings_metric    ON advise_findings(client_id, metric_key)`,

  // 022 — ai_conversations: saved chat sessions per client/user
  `CREATE TABLE IF NOT EXISTS ai_conversations (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    created_by  VARCHAR(255),           -- username or entra_oid
    title       VARCHAR(255) NOT NULL DEFAULT 'New conversation',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ai_conversations_client ON ai_conversations(client_id, updated_at DESC)`,

  // 022b — ai_messages: individual messages within a conversation
  //
  // role       : 'user' | 'assistant' | 'tool'
  // content    : message text (nullable for pure tool-call messages)
  // tool_calls : JSONB — AI-emitted tool calls (query intent, tag actions, etc.)
  // tool_result: JSONB — result of executing a tool call
  // action_payload: JSONB — staged confirmation card data (tag apply/remove pending user confirm)
  `CREATE TABLE IF NOT EXISTS ai_messages (
    id              SERIAL PRIMARY KEY,
    conversation_id INTEGER      NOT NULL REFERENCES ai_conversations(id) ON DELETE CASCADE,
    role            VARCHAR(16)  NOT NULL CHECK (role IN ('user', 'assistant', 'tool')),
    content         TEXT,
    tool_calls      JSONB        DEFAULT NULL,
    tool_result     JSONB        DEFAULT NULL,
    action_payload  JSONB        DEFAULT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_ai_messages_conversation ON ai_messages(conversation_id, created_at ASC)`,

  // 023b — instance_settings: key/value store for per-instance config (LLM keys, etc.)
  //         key   = namespaced string, e.g. 'ai.provider', 'ai.api_key', 'ai.model'
  //         value = TEXT (encrypted at app layer for sensitive values)
  `CREATE TABLE IF NOT EXISTS instance_settings (
    id           SERIAL PRIMARY KEY,
    client_id    VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    key          VARCHAR(255) NOT NULL,
    value        TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(client_id, key)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_instance_settings_client ON instance_settings(client_id)`,

  // 023a — version_field: optional column name in each doc that carries a version/timestamp
  //         version_type: 'timestamp' (ISO 8601 string comparison) or 'integer'
  //         At upsert time, incoming row is skipped (ctx.op='none') if stored value >= incoming.
  `ALTER TABLE dataset_definitions ADD COLUMN IF NOT EXISTS version_field VARCHAR(255) DEFAULT NULL`,
  `ALTER TABLE dataset_definitions ADD COLUMN IF NOT EXISTS version_type  VARCHAR(20)  NOT NULL DEFAULT 'timestamp'`,

  // 024 — pinned findings: users can pin individual Advise findings to the
  //        Recommendations tab so they persist across snapshot runs.
  `ALTER TABLE advise_findings ADD COLUMN IF NOT EXISTS pinned BOOLEAN NOT NULL DEFAULT FALSE`,
  `ALTER TABLE advise_findings ADD COLUMN IF NOT EXISTS pinned_at TIMESTAMPTZ DEFAULT NULL`,
  `ALTER TABLE advise_findings ADD COLUMN IF NOT EXISTS pinned_note TEXT DEFAULT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_advise_findings_pinned ON advise_findings(client_id, pinned) WHERE pinned = TRUE`,

  // 025a — advise_rule_templates: global canned rule definitions (read-only to instances).
  //   rule_type    : machine key matching an engine handler (idle_days, compliance_expiry, etc.)
  //   dataset_hint : which dataset name the rule prefers (engine looks for it in client datasets)
  //   config_schema: JSON Schema describing configurable params (thresholds, field mappings)
  //   default_config: default param values used when no instance override exists
  //   is_active    : soft-disable a global rule without deleting it
  `CREATE TABLE IF NOT EXISTS advise_rule_templates (
    id             SERIAL PRIMARY KEY,
    rule_type      VARCHAR(64)  NOT NULL UNIQUE,
    label          VARCHAR(255) NOT NULL,
    description    TEXT,
    category       VARCHAR(32)  NOT NULL,
    dataset_hint   VARCHAR(255),
    config_schema  JSONB        NOT NULL DEFAULT '{}',
    default_config JSONB        NOT NULL DEFAULT '{}',
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
    sort_order     INTEGER      NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
  )`,

  // 025b — advise_rules: per-instance rule overrides + custom rules.
  //   global_template_id: NULL = custom rule, non-null = override of a global template
  //   enabled           : instance can disable a global template or their own rules
  //   config            : merged on top of template default_config at run time
  `CREATE TABLE IF NOT EXISTS advise_rules (
    id                  SERIAL PRIMARY KEY,
    client_id           VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    global_template_id  INTEGER      REFERENCES advise_rule_templates(id) ON DELETE SET NULL,
    rule_type           VARCHAR(64)  NOT NULL,
    label               VARCHAR(255) NOT NULL,
    description         TEXT,
    category            VARCHAR(32)  NOT NULL,
    dataset_hint        VARCHAR(255),
    config              JSONB        NOT NULL DEFAULT '{}',
    enabled             BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE(client_id, rule_type)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_advise_rules_client ON advise_rules(client_id)`,
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
