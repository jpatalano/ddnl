/**
 * adminRoutes.js  —  /api/admin/* endpoints
 *
 * All routes require basic-auth (same ddnl/ddnl! as the app).
 * These power the /admin UI: ingest overview, import history,
 * FTP file browser, quarantine viewer, and generator history.
 */

const express  = require('express');
const fs       = require('fs');
const path     = require('path');
const readline = require('readline');
const { pool } = require('./db');

const router = express.Router();

// ─── Helpers ─────────────────────────────────────────────────────────────────

const ROOT = __dirname;

function logPath(name)  { return path.join(ROOT, 'logs', name); }
function statePath(name){ return path.join(ROOT, name); }

/** Read a JSONL file; returns array of parsed objects (bad lines skipped). */
function readJsonl(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const lines = fs.readFileSync(filePath, 'utf8').split('\n').filter(Boolean);
  const out = [];
  for (const l of lines) {
    try { out.push(JSON.parse(l)); } catch { /* skip */ }
  }
  return out;
}

/** Read JSON state file safely. */
function readJson(filePath, fallback = {}) {
  try { return JSON.parse(fs.readFileSync(filePath, 'utf8')); }
  catch { return fallback; }
}

/** Stat a file safely; returns null if not found. */
function safeStatSync(p) {
  try { return fs.statSync(p); } catch { return null; }
}

// ─── GET /api/admin/ingest/overview ──────────────────────────────────────────
// Channels per dataset + last ingest_log entry per dataset
router.get('/ingest/overview', async (req, res) => {
  try {
    const datasets = await pool.query(`
      SELECT id, name, label, is_active
      FROM dataset_definitions
      ORDER BY name
    `).catch(() => ({ rows: [] }));

    const channels = await pool.query(`
      SELECT ic.id, ic.dataset_id, ic.channel_name, ic.method, ic.mode,
             ic.id_field, ic.is_active, ic.created_at,
             d.name AS dataset_name, d.label AS dataset_label
      FROM ingest_channels ic
      JOIN dataset_definitions d ON d.id = ic.dataset_id
      ORDER BY d.name, ic.channel_name
    `).catch(() => ({ rows: [] }));

    const lastLog = await pool.query(`
      SELECT DISTINCT ON (dataset_name)
             dataset_name, operation, doc_count, failed_count,
             duration_ms, triggered_by, created_at
      FROM ingest_log
      ORDER BY dataset_name, created_at DESC
    `).catch(() => ({ rows: [] }));

    // Poller state summary
    const pollerState = readJson(statePath('ftp-poller-state.json'));
    const processed   = pollerState.processed || {};
    const ftpSummary  = {
      files_processed: Object.keys(processed).length,
      last_poll: pollerState.last_poll || null,
      stores: {}
    };
    for (const [file, info] of Object.entries(processed)) {
      const sid = info.store_id || 'unknown';
      if (!ftpSummary.stores[sid]) ftpSummary.stores[sid] = { files: 0, upserted: 0, rejected: 0 };
      ftpSummary.stores[sid].files++;
      ftpSummary.stores[sid].upserted  += (info.upserted  || 0);
      ftpSummary.stores[sid].rejected  += (info.rows_rejected || 0);
    }

    res.json({
      success: true,
      datasets: datasets.rows,
      channels: channels.rows,
      last_ingest: lastLog.rows,
      ftp_summary: ftpSummary
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── GET /api/admin/ingest/history ───────────────────────────────────────────
// Poller run log grouped by poll_id.
// Log schema uses flat top-level fields (poll_id, run_id, msg) — NOT event/data nesting.
router.get('/ingest/history', (req, res) => {
  try {
    const pollerLog   = readJsonl(logPath('poller.log.jsonl'));
    const pollerState = readJson(statePath('ftp-poller-state.json'));
    const processed   = pollerState.processed || {};

    // Group by poll_id (top-level field on every entry)
    const polls = {};
    for (const entry of pollerLog) {
      const pid = entry.poll_id;
      if (!pid) continue;
      if (!polls[pid]) polls[pid] = { poll_id: pid, entries: [], fileEntries: [] };
      polls[pid].entries.push(entry);
      // "File complete" entries carry rows_total/rows_valid/rows_rejected/upserted at top level
      if (entry.msg && entry.msg.startsWith('File complete:')) {
        polls[pid].fileEntries.push(entry);
      }
    }

    const runs = Object.values(polls)
      .map(p => {
        const started   = p.entries.find(e => e.msg === 'Poll started');
        const completed = p.entries.find(e => e.msg === 'Poll complete');
        const alerts    = p.entries.filter(e => e.level === 'warn' || e.level === 'error' || e.level === 'alert');

        return {
          poll_id:         p.poll_id,
          started_at:      started?.ts    || null,
          completed_at:    completed?.ts  || null,
          files_processed: completed?.processed ?? p.fileEntries.length,
          files_skipped:   completed?.skipped   ?? 0,
          files_errored:   completed?.errors    ?? 0,
          total_upserted:  p.fileEntries.reduce((s, f) => s + (f.upserted      || 0), 0),
          total_rejected:  p.fileEntries.reduce((s, f) => s + (f.rows_rejected || 0), 0),
          files: p.fileEntries.map(f => ({
            filename:      f.filename,
            store_id:      f.store_id,
            date:          f.date,
            rows_total:    f.rows_total,
            rows_valid:    f.rows_valid,
            rows_rejected: f.rows_rejected,
            upserted:      f.upserted,
            failed:        f.failed,
            duration_ms:   f.duration_ms,
            status:        'complete'
          })),
          alerts: alerts.map(e => ({ ts: e.ts, level: e.level, msg: e.msg, run_id: e.run_id, store_id: e.store_id, filename: e.filename }))
        };
      })
      .sort((a, b) => (b.started_at || '') > (a.started_at || '') ? 1 : -1);

    res.json({ success: true, runs, processed_files: Object.keys(processed).length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── GET /api/admin/ingest/history/:pollId/logs ─────────────────────────────
// All log entries for a specific poll run, optionally filtered by filename.
// Returns raw log entries so the UI can search/sort/filter client-side.
router.get('/ingest/history/:pollId/logs', (req, res) => {
  try {
    const { pollId } = req.params;
    const { filename } = req.query;
    const allEntries = readJsonl(logPath('poller.log.jsonl'));
    let entries = allEntries.filter(e => e.poll_id === pollId);
    if (filename) entries = entries.filter(e => e.filename === filename);
    // Newest-first within the run
    entries = entries.slice().sort((a, b) => (a.ts || '') > (b.ts || '') ? 1 : -1);
    res.json({ success: true, poll_id: pollId, filename: filename || null, count: entries.length, entries });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── GET /api/admin/ingest/files ─────────────────────────────────────────────
// FTP drop folder listing + per-file poller state
router.get('/ingest/files', (req, res) => {
  try {
    const dropDir    = path.join(ROOT, 'ftp-drop', 'sales');
    const pollerState = readJson(statePath('ftp-poller-state.json'));
    const processed  = pollerState.processed || {};

    let files = [];
    if (fs.existsSync(dropDir)) {
      files = fs.readdirSync(dropDir)
        .filter(f => f.endsWith('.csv') || f.endsWith('.txt'))
        .map(fname => {
          const stat  = safeStatSync(path.join(dropDir, fname));
          const state = processed[fname] || null;
          return {
            filename:      fname,
            size_bytes:    stat?.size || 0,
            modified_at:   stat?.mtime?.toISOString() || null,
            processed:     !!state,
            processed_at:  state?.processed_at || null,
            store_id:      state?.store_id || null,
            date:          state?.date || null,
            upserted:      state?.upserted || null,
            rows_valid:    state?.rows_valid || null,
            rows_rejected: state?.rows_rejected || null,
            poll_id:       state?.poll_id || null
          };
        })
        .sort((a,b) => (b.modified_at||'') > (a.modified_at||'') ? 1 : -1);
    }

    // Also show processed files that no longer exist in the drop folder
    for (const [fname, state] of Object.entries(processed)) {
      if (!files.find(f => f.filename === fname)) {
        files.push({
          filename:      fname,
          size_bytes:    null,
          modified_at:   null,
          processed:     true,
          processed_at:  state.processed_at || null,
          store_id:      state.store_id || null,
          date:          state.date || null,
          upserted:      state.upserted || null,
          rows_valid:    state.rows_valid || null,
          rows_rejected: state.rows_rejected || null,
          poll_id:       state.poll_id || null,
          removed:       true   // no longer in drop folder
        });
      }
    }

    res.json({
      success: true,
      drop_dir: 'ftp-drop/sales/',
      files,
      last_poll: pollerState.last_poll || null
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── GET /api/admin/ingest/quarantine ────────────────────────────────────────
// Rejected rows with reasons, filterable by store/date
router.get('/ingest/quarantine', (req, res) => {
  try {
    const { store, date, limit: lim = '500' } = req.query;
    let rows = readJsonl(logPath('quarantine.log.jsonl'));

    if (store) rows = rows.filter(r => r.store_id === store || r.row?.store_id === store);
    if (date)  rows = rows.filter(r => r.date === date  || r.row?.date === date);

    // Newest first
    rows = rows.slice(-Number(lim)).reverse();

    // Reason summary
    const reasons = {};
    for (const r of rows) {
      const key = r.reason || (r.row?._errors || []).join(', ') || 'unknown';
      reasons[key] = (reasons[key] || 0) + 1;
    }

    res.json({ success: true, total: rows.length, reason_summary: reasons, rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── GET /api/admin/ingest/generator ─────────────────────────────────────────
// Generator run history from generator.log.jsonl
router.get('/ingest/generator', (req, res) => {
  try {
    const entries = readJsonl(logPath('generator.log.jsonl'));

    // Each run_complete is the summary; attach the per-store events from the same run
    const runMap = {};
    for (const e of entries) {
      const rid = e.run_id || e.data?.run_id;
      if (!rid) continue;
      if (!runMap[rid]) runMap[rid] = { run_id: rid, events: [] };
      runMap[rid].events.push(e);
      if (e.event === 'run_complete') {
        Object.assign(runMap[rid], e);
      }
    }

    const runs = Object.values(runMap)
      .sort((a,b) => (b.ts||'') > (a.ts||'') ? 1 : -1)
      .map(r => ({
        run_id:       r.run_id,
        date:         r.date || r.data?.date,
        ts:           r.ts,
        ok:           r.ok || r.data?.ok,
        skipped:      r.skipped || r.data?.skipped,
        failed:       r.failed || r.data?.failed,
        stores:       r.stores || r.data?.stores || {},
        events:       r.events.filter(e => e.event !== 'run_complete')
      }));

    res.json({ success: true, runs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── GET /api/admin/ingest/alerts ────────────────────────────────────────────
// All warn/error entries from poller + generator logs (newest first)
router.get('/ingest/alerts', (req, res) => {
  try {
    const pollerAlerts = readJsonl(logPath('poller.log.jsonl'))
      .filter(e => e.level === 'warn' || e.level === 'error')
      .map(e => ({ ...e, source: 'poller' }));

    const genAlerts = readJsonl(logPath('generator.log.jsonl'))
      .filter(e => e.level === 'warn' || e.level === 'error')
      .map(e => ({ ...e, source: 'generator' }));

    const all = [...pollerAlerts, ...genAlerts]
      .sort((a,b) => (b.ts||'') > (a.ts||'') ? 1 : -1)
      .slice(0, 200);

    res.json({ success: true, alerts: all });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

module.exports = router;
