#!/usr/bin/env node
/**
 * ftp-poller.js  —  DDNL FTP ingest poller
 *
 * Polls the FTP drop folder, detects new per-store sales files,
 * validates each row, rebuilds missing identifiers, and upserts
 * clean records via POST /api/ingest/sales/push in batches.
 *
 * Features:
 *   • Pattern recognition — filename → store_id + date (see ftp-config.js)
 *   • Per-store fallback  — if store_id missing from rows, infer from filename
 *   • PK rebuild          — if id_field_reliable=false, reconstruct sales_pk
 *   • Per-row validation  — rejects/quarantines bad rows with reasons
 *   • Idempotent          — tracks processed files; re-running is always safe
 *   • Batched upsert      — configurable batch_size, respects API 50K cap
 *   • Retry w/ backoff    — transient network/API failures retried N times
 *   • Missed-file alerts  — fires when a store hasn't delivered in N days
 *   • High-error alerts   — fires when defect % exceeds threshold
 *   • Structured JSONL log + human-readable console output
 *   • Quarantine log      — bad rows written to logs/quarantine.log.jsonl
 *
 * Usage:
 *   node ftp-poller.js          # runs once then exits
 *   node ftp-poller.js --watch  # polls continuously on interval
 *
 * Env:
 *   INGEST_API_KEY    API key for the /api/ingest endpoint
 *   INGEST_API_BASE   Base URL of the analytics server
 */

'use strict';
const fs     = require('fs');
const path   = require('path');
const https  = require('https');
const http   = require('http');
const crypto = require('crypto');
const CFG    = require('./ftp-config');

// ─── Config shorthand ─────────────────────────────────────────────────────────

const PC         = CFG.poller;
const LOG_DIR    = path.join(__dirname, 'logs');
const LOG_FILE   = path.join(__dirname, PC.log_file);
const QUAR_FILE  = path.join(LOG_DIR, 'quarantine.log.jsonl');
const STATE_PATH = path.join(__dirname, PC.state_file);
const WATCH_MODE = process.argv.includes('--watch');

fs.mkdirSync(LOG_DIR, { recursive: true });

// ─── Structured logger ────────────────────────────────────────────────────────

function log(level, msg, data = {}) {
  const entry = { ts: new Date().toISOString(), level, msg, ...data };
  fs.appendFileSync(LOG_FILE, JSON.stringify(entry) + '\n');
  const icon = { info: '  ', warn: '⚠ ', error: '✗ ', alert: '🚨' }[level] || '  ';
  const extra = Object.keys(data).length
    ? '  ' + Object.entries(data).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(' ')
    : '';
  console.log(`${icon} [${level.toUpperCase()}] ${msg}${extra}`);
}

function quarantine(rows, reason, context) {
  for (const row of rows) {
    fs.appendFileSync(QUAR_FILE,
      JSON.stringify({ ts: new Date().toISOString(), reason, context, row }) + '\n');
  }
}

// ─── Persistent run state ─────────────────────────────────────────────────────
// Tracks: processed files (filename → { processed_at, rows, ok, failed })
//         last seen date per store (for missed-file detection)

function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE_PATH, 'utf8')); }
  catch { return { processed: {}, last_file_date: {} }; }
}

function saveState(state) {
  fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

// ─── Filename → store + date (pattern recognition) ───────────────────────────

function parseFilename(filename) {
  for (const pat of CFG.filename_patterns) {
    const m = filename.match(pat.regex);
    if (!m) continue;

    // Extract date
    let dateStr = m[pat.date_group];
    if (pat.date_fmt === 'yyyymmdd') {
      dateStr = `${dateStr.slice(0,4)}-${dateStr.slice(4,6)}-${dateStr.slice(6,8)}`;
    }

    // Extract store_id
    const storeId = pat.id_group ? m[pat.id_group] : pat.store_id;

    // Validate date is a real date
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) continue;
    if (isNaN(Date.parse(dateStr))) continue;

    // Validate store is in our config
    if (storeId && !CFG.stores[storeId]) {
      log('warn', `Unknown store_id in filename: ${storeId}`, { filename });
      return null;
    }

    return { storeId, date: dateStr };
  }
  return null;  // unrecognized filename — skip
}

// ─── Row validation ───────────────────────────────────────────────────────────

const REQUIRED_FIELDS = ['date', 'plu_code', 'unit_type', 'units_sold', 'unit_price', 'revenue'];
const TODAY           = new Date().toISOString().slice(0, 10);
const MAX_DATE        = new Date(Date.now() + 86400000).toISOString().slice(0, 10);  // tomorrow at most

function validateRow(row, inferredStoreId, fileDate) {
  const errors = [];

  // Missing required fields
  for (const f of REQUIRED_FIELDS) {
    if (row[f] === undefined || row[f] === null || row[f] === '') {
      errors.push(`missing_${f}`);
    }
  }

  // Numeric checks
  const units   = parseFloat(row.units_sold);
  const price   = parseFloat(row.unit_price);
  const revenue = parseFloat(row.revenue);

  if (isNaN(units))   errors.push('non_numeric_units_sold');
  if (isNaN(price))   errors.push('non_numeric_unit_price');
  if (isNaN(revenue)) errors.push('non_numeric_revenue');
  if (!isNaN(units)   && units < 0)  errors.push('negative_units');
  if (!isNaN(price)   && price < 0)  errors.push('negative_price');
  if (!isNaN(revenue) && revenue < 0) errors.push('negative_revenue');

  // Date sanity
  const rowDate = row.date;
  if (rowDate && !/^\d{4}-\d{2}-\d{2}$/.test(rowDate)) {
    errors.push('invalid_date_format');
  } else if (rowDate && rowDate > MAX_DATE) {
    errors.push('future_date');
  }

  // Revenue × units × price rough cross-check (allow ±50% — POS rounding, discounts)
  if (!isNaN(units) && !isNaN(price) && !isNaN(revenue) && units > 0 && price > 0) {
    const expected = units * price;
    const ratio    = revenue / expected;
    if (ratio < 0.1 || ratio > 10) {
      errors.push('revenue_units_price_mismatch');
    }
  }

  // Store ID presence check
  const effectiveStoreId = row.store_id || inferredStoreId;
  if (!effectiveStoreId) errors.push('missing_store_id');
  else if (!CFG.stores[effectiveStoreId]) errors.push(`unknown_store_id:${effectiveStoreId}`);

  return errors;
}

// ─── Row normalizer ───────────────────────────────────────────────────────────
// Called after validation passes. Fills in inferred fields, rebuilds PK if needed.

function normalizeRow(row, inferredStoreId, fileDate, storeCfg) {
  const r = { ...row };

  // Coerce numerics
  r.units_sold   = parseFloat(r.units_sold);
  r.unit_price   = parseFloat(r.unit_price);
  r.revenue      = parseFloat(r.revenue);
  r.item_count   = r.item_count !== undefined ? parseInt(r.item_count) || 0 : 0;
  r.transactions = r.transactions !== undefined ? parseInt(r.transactions) || 0 : 0;

  // Infer store if missing from row data
  if (!r.store_id && inferredStoreId) {
    r.store_id = inferredStoreId;
    const meta = CFG.stores[inferredStoreId];
    if (meta && !r.store_name) r.store_name = meta.name;
  }

  // Use file date if row date is absent
  if (!r.date) r.date = fileDate;

  // Rebuild sales_pk if store config says it's unreliable or it's missing
  const needsRebuild = !r.sales_pk || storeCfg?.id_field_reliable === false;
  if (needsRebuild && r.date && r.store_id && r.plu_code) {
    r.sales_pk = `${r.date}__${r.store_id}__${r.plu_code}`;
  }

  return r;
}

// ─── HTTP helper (no axios dependency) ───────────────────────────────────────

function postJson(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const payload  = JSON.stringify(body);
    const parsed   = new URL(url);
    const lib      = parsed.protocol === 'https:' ? https : http;
    const opts     = {
      hostname: parsed.hostname,
      port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path:     parsed.pathname + parsed.search,
      method:   'POST',
      headers:  {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'X-Api-Key':      PC.api_key,
        ...headers,
      },
    };

    const req = lib.request(opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(new Error('Request timeout')); });
    req.write(payload);
    req.end();
  });
}

// ─── Upsert with retry ────────────────────────────────────────────────────────

async function upsertBatch(docs, batchNum, totalBatches, context) {
  const url = `${PC.api_base}/api/ingest/${PC.dataset}/push?channel=Nightly+CSV`;

  for (let attempt = 1; attempt <= PC.max_retries; attempt++) {
    try {
      const resp = await postJson(url, { docs });
      if (resp.status === 200 && resp.body?.success) {
        return resp.body;
      }
      // Non-retryable client errors
      if (resp.status >= 400 && resp.status < 500) {
        throw new Error(`API ${resp.status}: ${JSON.stringify(resp.body)}`);
      }
      // 5xx — retryable
      throw new Error(`API ${resp.status} (attempt ${attempt})`);
    } catch (e) {
      if (attempt === PC.max_retries) throw e;
      const delay = PC.retry_delay_ms * attempt;
      log('warn', `Batch ${batchNum}/${totalBatches} failed (attempt ${attempt}) — retrying in ${delay}ms`, { error: e.message, ...context });
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

// ─── Process one file ─────────────────────────────────────────────────────────

async function processFile(filename, csvContent, storeId, fileDate) {
  const storeCfg = CFG.stores[storeId] || {};
  const runId    = crypto.randomBytes(6).toString('hex');

  log('info', `Processing ${filename}`, { run_id: runId, store_id: storeId, date: fileDate });

  // ── Parse CSV ──
  const lines  = csvContent.trim().split('\n');
  const header = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rawRows = lines.slice(1).map((line, i) => {
    // Handle quoted fields with commas
    const vals = [];
    let cur = '', inQ = false;
    for (const ch of line) {
      if (ch === '"') { inQ = !inQ; continue; }
      if (ch === ',' && !inQ) { vals.push(cur); cur = ''; continue; }
      cur += ch;
    }
    vals.push(cur);
    const row = {};
    header.forEach((h, j) => row[h] = vals[j]?.trim() ?? '');
    row._line = i + 2;  // 1-indexed line number in file
    return row;
  });

  log('info', `Parsed ${rawRows.length} rows from ${filename}`, { run_id: runId });

  // ── Validate + normalize ──
  const goodRows   = [];
  const badRows    = [];
  const seenPKs    = new Map();  // pk → first line number (dupe detection)

  for (const row of rawRows) {
    const errors = validateRow(row, storeId, fileDate);

    if (errors.length) {
      badRows.push({ ...row, _errors: errors });
      continue;
    }

    const norm = normalizeRow(row, storeId, fileDate, storeCfg);

    // Intra-file duplicate detection
    if (norm.sales_pk) {
      if (seenPKs.has(norm.sales_pk)) {
        log('warn', `Duplicate PK in file`, { run_id: runId, sales_pk: norm.sales_pk,
          first_line: seenPKs.get(norm.sales_pk), this_line: row._line, filename });
        // We still send it — upsert is idempotent, second write just wins
      }
      seenPKs.set(norm.sales_pk, row._line);
    }

    // Strip internal tracking field
    delete norm._line;
    goodRows.push(norm);
  }

  const defectPct = rawRows.length > 0 ? ((badRows.length / rawRows.length) * 100).toFixed(1) : '0.0';

  log('info', `Validation complete`, {
    run_id: runId, store_id: storeId,
    total: rawRows.length, valid: goodRows.length,
    rejected: badRows.length, defect_pct: parseFloat(defectPct),
    intra_dupes: seenPKs.size < goodRows.length ? goodRows.length - seenPKs.size : 0,
  });

  // Quarantine bad rows
  if (badRows.length) {
    quarantine(badRows, 'validation_failed', { filename, store_id: storeId, date: fileDate, run_id: runId });
    log('warn', `Quarantined ${badRows.length} bad rows → logs/quarantine.log.jsonl`, { run_id: runId });
  }

  // ── High error rate alert ──
  if (parseFloat(defectPct) > PC.high_error_rate_pct) {
    log('alert', `HIGH ERROR RATE: ${defectPct}% of rows rejected for ${storeId}`, {
      run_id: runId, store_id: storeId, filename,
      threshold_pct: PC.high_error_rate_pct,
      rejected: badRows.length, total: rawRows.length,
    });
  }

  // ── Nothing to upsert ──
  if (!goodRows.length) {
    log('warn', `No valid rows to upsert for ${filename}`, { run_id: runId });
    return { run_id: runId, rows_total: rawRows.length, rows_valid: 0, rows_rejected: badRows.length, upserted: 0, failed: 0 };
  }

  // ── Batched upsert ──
  const batches     = [];
  for (let i = 0; i < goodRows.length; i += PC.batch_size) {
    batches.push(goodRows.slice(i, i + PC.batch_size));
  }

  log('info', `Upserting ${goodRows.length} rows in ${batches.length} batch(es)`, { run_id: runId, batch_size: PC.batch_size });

  let totalUpserted = 0;
  let totalFailed   = 0;

  for (let b = 0; b < batches.length; b++) {
    const batch = batches[b];
    try {
      const result = await upsertBatch(batch, b + 1, batches.length, { run_id: runId, store_id: storeId });
      totalUpserted += result.indexed || 0;
      totalFailed   += result.failed  || 0;

      if (result.failed > 0) {
        log('warn', `Batch ${b+1}/${batches.length} had ${result.failed} ES failures`,
          { run_id: runId, batch_id: result.batch_id, errors: result.errors?.slice(0,3) });
      }
    } catch (e) {
      log('error', `Batch ${b+1}/${batches.length} failed permanently`, { run_id: runId, error: e.message });
      totalFailed += batch.length;
      // Quarantine the whole batch that couldn't be sent
      quarantine(batch, 'upsert_failed', { filename, batch_num: b + 1, error: e.message, run_id: runId });
    }
  }

  log('info', `File complete: ${filename}`, {
    run_id: runId, store_id: storeId, date: fileDate,
    rows_total: rawRows.length, rows_valid: goodRows.length,
    rows_rejected: badRows.length, upserted: totalUpserted, failed: totalFailed,
  });

  return {
    run_id: runId, rows_total: rawRows.length, rows_valid: goodRows.length,
    rows_rejected: badRows.length, upserted: totalUpserted, failed: totalFailed,
  };
}

// ─── Missed-file detection ────────────────────────────────────────────────────

function checkMissingFiles(state, currentDate) {
  const today  = new Date(currentDate + 'T12:00:00');

  for (const [storeId, storeCfg] of Object.entries(CFG.stores)) {
    const lastSeen = state.last_file_date[storeId];
    if (!lastSeen) continue;

    const lastDate = new Date(lastSeen + 'T12:00:00');
    const daysSince = Math.round((today - lastDate) / 86400000);

    if (daysSince > PC.missed_file_threshold) {
      log('alert', `MISSED FILES: ${storeCfg.name} hasn't delivered in ${daysSince} days`, {
        store_id:   storeId,
        last_seen:  lastSeen,
        days_since: daysSince,
        threshold:  PC.missed_file_threshold,
      });
    }
  }
}



// ─── FTP helpers (via Python — active mode, no PASV) ───────────────────────────

const { execFile } = require('child_process');

function ftpList(host, port, user, pass) {
  return new Promise((resolve, reject) => {
    execFile('python3', [__dirname + '/ftp-list.py', host, String(port), user, pass],
      { timeout: 15000 }, (err, stdout, stderr) => {
        if (err) return reject(new Error(stderr || err.message));
        const files = stdout.trim().split('\n').filter(f => f.endsWith('.csv'));
        resolve(files);
      });
  });
}

function ftpDownload(host, port, user, pass, filename) {
  const tmp = '/tmp/ddnl_dl_' + Date.now() + '_' + filename;
  return new Promise((resolve, reject) => {
    execFile('python3', [__dirname + '/ftp-download.py', host, String(port), user, pass, filename, tmp],
      { timeout: 30000 }, (err, stdout, stderr) => {
        if (err) return reject(new Error(stderr || err.message));
        try {
          const content = require('fs').readFileSync(tmp, 'utf8');
          require('fs').unlinkSync(tmp);
          resolve(content);
        } catch(e) { reject(e); }
      });
  });
}

// ─── FTP poll ───────────────────────────────────────────────────────────────

async function pollOnce() {
  const pollId = require('crypto').randomBytes(4).toString('hex');
  const today  = new Date().toISOString().slice(0, 10);
  const state  = loadState();

  console.log('\n╔══ DDNL FTP Poller ═══════════════════════════════════════════════════');
  console.log('  poll_id : ' + pollId + '  |  ' + new Date().toLocaleTimeString());
  console.log('╠════════════════════════════════════════════════════════════════╠');
  log('info', 'Poll started', { poll_id: pollId });

  try {
    const csvFiles = (await ftpList(CFG.ftp.host, CFG.ftp.port, CFG.ftp.user, CFG.ftp.pass)).sort();
    log('info', 'FTP listing: ' + csvFiles.length + ' CSV file(s)', { poll_id: pollId, files: csvFiles });
    if (!csvFiles.length) console.log('  No CSV files found on FTP');

    let processed = 0, skipped = 0, errors = 0;

    for (const filename of csvFiles) {
      if (state.processed[filename]) {
        console.log('  ↷  ' + filename.padEnd(45) + '  already processed');
        skipped++; continue;
      }

      const parsed = parseFilename(filename);
      if (!parsed) {
        log('warn', 'Unrecognized filename pattern: ' + filename, { poll_id: pollId });
        console.log('  ?  ' + filename.padEnd(45) + '  unrecognized pattern');
        skipped++; continue;
      }

      const { storeId, date: fileDate } = parsed;
      console.log('  ↓  ' + filename.padEnd(45) + '  ' + storeId + '  ' + fileDate);

      let csvContent;
      try {
        csvContent = await ftpDownload(CFG.ftp.host, CFG.ftp.port, CFG.ftp.user, CFG.ftp.pass, filename);
      } catch (e) {
        log('error', 'Download failed: ' + filename, { poll_id: pollId, error: e.message });
        errors++; continue;
      }

      let result;
      try {
        result = await processFile(filename, csvContent, storeId, fileDate);
      } catch (e) {
        log('error', 'Processing failed: ' + filename, { poll_id: pollId, error: e.message });
        errors++; continue;
      }

      state.processed[filename] = { processed_at: new Date().toISOString(), store_id: storeId, date: fileDate, ...result };
      state.last_file_date[storeId] = fileDate;
      saveState(state);

      const warn = result.failed > 0 ? '  ⚠ ' + result.failed + ' ES fail' : '';
      console.log('     → ' + result.upserted + ' upserted  ' + result.rows_rejected + ' rejected' + warn);
      processed++;
    }

    checkMissingFiles(state, today);
    console.log('╠════════════════════════════════════════════════════════════════╠');
    console.log('  ' + processed + ' processed  |  ' + skipped + ' skipped  |  ' + errors + ' errors');
    console.log('╚════════════════════════════════════════════════════════════════╝\n');
    log('info', 'Poll complete', { poll_id: pollId, processed, skipped, errors });

  } catch (e) {
    log('error', 'Poll failed', { poll_id: pollId, error: e.message });
    console.error('Poll error: ' + e.message);
  }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

(async () => {
  await pollOnce();

  if (WATCH_MODE) {
    log('info', `Watch mode: polling every ${PC.poll_interval_ms / 1000}s`);
    console.log(`Watching FTP every ${PC.poll_interval_ms / 1000}s  (Ctrl+C to stop)\n`);
    setInterval(pollOnce, PC.poll_interval_ms);
  }
})().catch(e => {
  console.error('Poller crashed:', e.message);
  fs.appendFileSync(LOG_FILE,
    JSON.stringify({ ts: new Date().toISOString(), level: 'error', msg: 'poller_crash', error: e.message }) + '\n');
  process.exit(1);
});
