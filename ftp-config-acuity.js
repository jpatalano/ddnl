/**
 * ftp-config-acuity.js  —  DDNL FTP pipeline config for Acuity instance
 *
 * Stores and dataset schema are TBD — will be built out when client
 * provides their data files. For now this config is ready to receive files.
 *
 * FTP port: 2122 (produce uses 2121)
 * Drop dir: ./ftp-drop-acuity/
 */

'use strict';

module.exports = {

  // ── Global poller settings ─────────────────────────────────────────────────
  poller: {
    poll_interval_ms:      30_000,
    batch_size:            500,
    max_retries:           3,
    retry_delay_ms:        5_000,
    missed_file_threshold: 2,
    high_error_rate_pct:   15,
    state_file:            'ftp-poller-acuity-state.json',
    log_file:              'logs/poller-acuity.log.jsonl',
    api_key:               process.env.ACUITY_INGEST_API_KEY || 'ik_047fd183429d6314bfca00e579f9cf993863d7e1b1639ca4946325c67bdad4db',
    api_base:              process.env.ACUITY_API_BASE       || 'https://acuity-analytics-production.up.railway.app',
    dataset:               null,   // ← set when dataset is defined
  },

  // ── FTP connection ─────────────────────────────────────────────────────────
  ftp: {
    host:       process.env.ACUITY_FTP_HOST || '127.0.0.1',
    port:       parseInt(process.env.ACUITY_FTP_PORT || '2122'),
    user:       process.env.ACUITY_FTP_USER || 'ftpuser',
    pass:       process.env.ACUITY_FTP_PASS || 'ddnl!',
    remote_dir: '/',
  },

  // ── Per-store configuration ────────────────────────────────────────────────
  // Populated when client provides data files and store list.
  stores: {},

  // ── Filename → store_id pattern registry ──────────────────────────────────
  // Populated when filename conventions are known.
  filename_patterns: [],

};
