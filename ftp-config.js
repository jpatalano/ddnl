/**
 * ftp-config.js  —  DDNL FTP pipeline configuration
 *
 * Central config for both the generator and the poller.
 * Edit this file to tune behavior per-store.
 *
 * STORE CHAOS SETTINGS
 * ─────────────────────
 * skip_rate         0.0–1.0  Probability this store's file is missing on any given day
 * streak_days       integer  If >1, when a skip occurs it lasts this many consecutive days
 *                            (simulates a store that goes offline for days at a time)
 * defect_rate       0.0–1.0  Fraction of rows that get a defect injected
 * dupe_rate         0.0–1.0  Fraction of rows duplicated within the file
 * filename_pattern  string   Pattern for this store's filename. Tokens:
 *                              {store_id}   store identifier
 *                              {date}       YYYY-MM-DD
 *                              {yyyymmdd}   YYYYMMDD (no dashes)
 *                            Set to null to use the default: sales_{store_id}_{date}.csv
 * store_id_in_file  boolean  If true, store_id is reliable in the data rows
 *                            If false, poller must infer store from filename pattern
 * id_field_reliable boolean  If true, sales_pk in data rows is trustworthy
 *                            If false, poller rebuilds sales_pk from date+store+plu
 *
 * POLLER SETTINGS (global)
 * ─────────────────────────
 * poll_interval_ms  How often the poller checks FTP for new files
 * batch_size        Docs per push API call
 * max_retries       Retry attempts per file on transient errors
 * retry_delay_ms    Delay between retries
 * missed_file_threshold  Days a file can be missing before alert fires
 * high_error_rate_pct    Defect % that triggers an alert (0–100)
 */

'use strict';

module.exports = {

  // ── Global poller settings ─────────────────────────────────────────────────
  poller: {
    poll_interval_ms:       30_000,   // check FTP every 30s
    batch_size:             500,      // docs per /push call
    max_retries:            3,
    retry_delay_ms:         5_000,
    missed_file_threshold:  2,        // alert if store file missing 2+ consecutive days
    high_error_rate_pct:    15,       // alert if >15% of rows rejected
    state_file:             'ftp-poller-state.json',   // tracks processed files
    log_file:               'logs/poller.log.jsonl',
    api_key:                process.env.INGEST_API_KEY || 'ik_ef3ed1ba7c466fb4ef44b930a06788abbc6066c14960dcf9ac8a7138fd0aa921',
    api_base:               process.env.INGEST_API_BASE || 'https://produce-analytics-production.up.railway.app',
    dataset:                'sales',
  },

  // ── FTP connection ─────────────────────────────────────────────────────────
  ftp: {
    host:    process.env.FTP_HOST || '127.0.0.1',
    port:    parseInt(process.env.FTP_PORT || '2121'),
    user:    process.env.FTP_USER || 'ftpuser',
    pass:    process.env.FTP_PASS || 'ddnl!',
    remote_dir: '/',    // FTP root = the sales/ drop folder
  },

  // ── Per-store configuration ────────────────────────────────────────────────
  stores: {

    store_001: {
      name:               'Downtown Market',
      // Very reliable flagship — rarely misses, clean data
      skip_rate:          0.03,
      streak_days:        1,
      defect_rate:        0.02,
      dupe_rate:          0.01,
      filename_pattern:   'sales_{store_id}_{date}.csv',   // default pattern
      store_id_in_file:   true,
      id_field_reliable:  true,
    },

    store_002: {
      name:               'Westside Fresh',
      // Solid but their POS vendor sends a non-standard filename
      skip_rate:          0.05,
      streak_days:        1,
      defect_rate:        0.04,
      dupe_rate:          0.02,
      filename_pattern:   'westside_{yyyymmdd}_export.csv',  // custom pattern
      store_id_in_file:   true,
      id_field_reliable:  true,
    },

    store_003: {
      name:               'Cedar Park Produce',
      // Old POS — omits store_id from rows, poller must infer from filename
      skip_rate:          0.06,
      streak_days:        1,
      defect_rate:        0.08,
      dupe_rate:          0.03,
      filename_pattern:   'cedarpark_{date}.csv',
      store_id_in_file:   false,   // ← poller infers store_id from filename
      id_field_reliable:  false,   // ← poller rebuilds sales_pk
    },

    store_004: {
      name:               'Round Rock Market',
      // Decent but goes offline for multi-day streaks a few times a month
      skip_rate:          0.12,
      streak_days:        3,        // when it misses, it misses 3 days in a row
      defect_rate:        0.06,
      dupe_rate:          0.02,
      filename_pattern:   'sales_{store_id}_{date}.csv',
      store_id_in_file:   true,
      id_field_reliable:  true,
    },

    store_005: {
      name:               'South Congress',
      // Messy — high defect rate, often sends duplicate rows, PKs unreliable
      skip_rate:          0.08,
      streak_days:        1,
      defect_rate:        0.18,     // worst data quality
      dupe_rate:          0.08,
      filename_pattern:   'sales_{store_id}_{date}.csv',
      store_id_in_file:   true,
      id_field_reliable:  false,    // their POS mangles the PK — rebuild from data
    },

    store_006: {
      name:               'Lakeway Fresh',
      // Reliable but filename has no store_id — just a date
      skip_rate:          0.04,
      streak_days:        1,
      defect_rate:        0.03,
      dupe_rate:          0.01,
      filename_pattern:   'lakeway_{yyyymmdd}.csv',
      store_id_in_file:   true,
      id_field_reliable:  true,
    },

    store_007: {
      name:               'Pflugerville Pick',
      // Rural store on a slow connection — highest skip rate, long outage streaks
      skip_rate:          0.20,
      streak_days:        5,        // when it goes down it's usually out Mon–Fri
      defect_rate:        0.12,
      dupe_rate:          0.05,
      filename_pattern:   'pfluger_{date}_sales.csv',
      store_id_in_file:   true,
      id_field_reliable:  true,
    },

  },

  // ── Filename → store_id pattern registry ──────────────────────────────────
  // The poller uses this to identify which store a file belongs to.
  // Entries are checked in order — first match wins.
  // Regex capture group 1 (if present) is used as store_id.
  // If no group, the store_id comes from the matched config key.
  filename_patterns: [
    // { regex, store_id }  — static mappings
    { regex: /^sales_(store_\d+)_(\d{4}-\d{2}-\d{2})\.csv$/,    store_id: null,        date_group: 2, id_group: 1 },
    { regex: /^westside_(\d{8})_export\.csv$/,                   store_id: 'store_002', date_group: 1, id_group: null, date_fmt: 'yyyymmdd' },
    { regex: /^cedarpark_(\d{4}-\d{2}-\d{2})\.csv$/,             store_id: 'store_003', date_group: 1, id_group: null },
    { regex: /^lakeway_(\d{8})\.csv$/,                           store_id: 'store_006', date_group: 1, id_group: null, date_fmt: 'yyyymmdd' },
    { regex: /^pfluger_(\d{4}-\d{2}-\d{2})_sales\.csv$/,        store_id: 'store_007', date_group: 1, id_group: null },
  ],
};
