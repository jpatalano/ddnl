/**
 * adviseRouter.js — REST API for the Advise module
 *
 * Mounted at /api/advise in server.js (behind global auth middleware).
 *
 * Endpoints:
 *   GET  /api/advise/snapshot              — latest snapshot + all findings for client
 *   GET  /api/advise/snapshot/:snapshotId  — specific snapshot
 *   GET  /api/advise/findings              — query findings (?category=&severity=&metric_key=)
 *   POST /api/advise/run                   — trigger an on-demand run for this client
 *   GET  /api/advise/badges                — metric_key → finding counts (for tile badges)
 */

'use strict';

const express = require('express');
const router  = express.Router();
const { pool }         = require('./db');
const { runForClient } = require('./adviseEngine');

// ── Resolve client_id from request ────────────────────────────────────────────
// Mirrors the pattern used in ingestRouter / server.js

function resolveClientId(req) {
  // Injected by global auth middleware via req.resolvedClientId, or fall back to INSTANCE
  if (req.resolvedClientId) return req.resolvedClientId;
  const inst = req.app.locals.INSTANCE;
  return inst?.clientId || inst?.id || 'demo';
}

// ── GET /api/advise/snapshot ──────────────────────────────────────────────────
// Returns the most recent snapshot with all its findings.
// Optional ?category= filter.

router.get('/snapshot', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const category = req.query.category || null;

    // Latest snapshot for this client
    const snapRes = await pool.query(`
      SELECT id, client_id, ran_at, finding_count, duration_ms, status
      FROM advise_snapshots
      WHERE client_id = $1
      ORDER BY ran_at DESC
      LIMIT 1
    `, [clientId]);

    if (!snapRes.rows.length) {
      return res.json({ success: true, snapshot: null, findings: [] });
    }

    const snapshot = snapRes.rows[0];

    let findingsQuery = `
      SELECT id, category, severity, metric_key,
             entity_type, entity_id, entity_label,
             data_json, recommendation, created_at,
             pinned, pinned_at, pinned_note
      FROM advise_findings
      WHERE snapshot_id = $1
    `;
    const params = [snapshot.id];

    if (category) {
      findingsQuery += ` AND category = $2`;
      params.push(category);
    }

    findingsQuery += ` ORDER BY
      CASE severity WHEN 'critical' THEN 1 WHEN 'watch' THEN 2 ELSE 3 END,
      created_at ASC`;

    const findRes = await pool.query(findingsQuery, params);

    res.json({
      success:  true,
      snapshot: {
        id:           snapshot.id,
        ran_at:       snapshot.ran_at,
        finding_count: snapshot.finding_count,
        duration_ms:  snapshot.duration_ms,
        status:       snapshot.status
      },
      findings: findRes.rows
    });
  } catch (err) {
    console.error('[adviseRouter] GET /snapshot error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── GET /api/advise/snapshot/:snapshotId ──────────────────────────────────────

router.get('/snapshot/:snapshotId', async (req, res) => {
  try {
    const clientId   = resolveClientId(req);
    const snapshotId = parseInt(req.params.snapshotId, 10);
    if (isNaN(snapshotId)) return res.status(400).json({ success: false, error: 'Invalid snapshotId' });

    const snapRes = await pool.query(`
      SELECT id, client_id, ran_at, finding_count, duration_ms, status
      FROM advise_snapshots
      WHERE id = $1 AND client_id = $2
    `, [snapshotId, clientId]);

    if (!snapRes.rows.length) return res.status(404).json({ success: false, error: 'Snapshot not found' });

    const findRes = await pool.query(`
      SELECT id, category, severity, metric_key,
             entity_type, entity_id, entity_label,
             data_json, recommendation, created_at
      FROM advise_findings
      WHERE snapshot_id = $1
      ORDER BY
        CASE severity WHEN 'critical' THEN 1 WHEN 'watch' THEN 2 ELSE 3 END,
        created_at ASC
    `, [snapshotId]);

    res.json({ success: true, snapshot: snapRes.rows[0], findings: findRes.rows });
  } catch (err) {
    console.error('[adviseRouter] GET /snapshot/:id error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── GET /api/advise/findings ──────────────────────────────────────────────────
// Filter findings from the latest snapshot.
// ?category=equipment|yard|customer|financial
// ?severity=critical|watch|opportunity
// ?metric_key=idle_days|yard_revenue|...

router.get('/findings', async (req, res) => {
  try {
    const clientId  = resolveClientId(req);
    const { category, severity, metric_key } = req.query;

    // Get latest snapshot id
    const snapRes = await pool.query(`
      SELECT id FROM advise_snapshots
      WHERE client_id = $1
      ORDER BY ran_at DESC LIMIT 1
    `, [clientId]);

    if (!snapRes.rows.length) return res.json({ success: true, findings: [] });

    const snapshotId = snapRes.rows[0].id;
    const conditions = ['snapshot_id = $1'];
    const params     = [snapshotId];
    let p = 2;

    if (category)   { conditions.push(`category = $${p++}`);   params.push(category); }
    if (severity)   { conditions.push(`severity = $${p++}`);   params.push(severity); }
    if (metric_key) { conditions.push(`metric_key = $${p++}`); params.push(metric_key); }

    const { rows } = await pool.query(`
      SELECT id, category, severity, metric_key,
             entity_type, entity_id, entity_label,
             data_json, recommendation, created_at
      FROM advise_findings
      WHERE ${conditions.join(' AND ')}
      ORDER BY
        CASE severity WHEN 'critical' THEN 1 WHEN 'watch' THEN 2 ELSE 3 END,
        created_at ASC
    `, params);

    res.json({ success: true, findings: rows });
  } catch (err) {
    console.error('[adviseRouter] GET /findings error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── GET /api/advise/badges ────────────────────────────────────────────────────
// Returns { [metric_key]: { total, critical, watch, opportunity } }
// Used by the UI to render badge counts on KPI tiles + charts.

router.get('/badges', async (req, res) => {
  try {
    const clientId = resolveClientId(req);

    const snapRes = await pool.query(`
      SELECT id FROM advise_snapshots
      WHERE client_id = $1
      ORDER BY ran_at DESC LIMIT 1
    `, [clientId]);

    if (!snapRes.rows.length) return res.json({ success: true, badges: {} });

    const snapshotId = snapRes.rows[0].id;

    const { rows } = await pool.query(`
      SELECT metric_key, severity, COUNT(*) AS cnt
      FROM advise_findings
      WHERE snapshot_id = $1
      GROUP BY metric_key, severity
    `, [snapshotId]);

    const badges = {};
    for (const row of rows) {
      if (!badges[row.metric_key]) {
        badges[row.metric_key] = { total: 0, critical: 0, watch: 0, opportunity: 0 };
      }
      const count = parseInt(row.cnt, 10);
      badges[row.metric_key][row.severity] = count;
      badges[row.metric_key].total += count;
    }

    res.json({ success: true, badges });
  } catch (err) {
    console.error('[adviseRouter] GET /badges error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── POST /api/advise/run ──────────────────────────────────────────────────────
// On-demand trigger — runs advise engine for this client right now.
// Returns the new snapshot summary.

router.post('/run', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const role     = req.body?.role || 'owner';

    console.log(`[adviseRouter] On-demand run triggered for client=${clientId} role=${role}`);
    const result = await runForClient(clientId, role);

    if (!result) {
      return res.json({ success: false, message: 'No visible_to_ai datasets found for this client' });
    }

    res.json({
      success:      true,
      snapshotId:   result.snapshotId,
      findingCount: result.findingCount,
      durationMs:   result.durationMs
    });
  } catch (err) {
    console.error('[adviseRouter] POST /run error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── GET /api/advise/pinned ───────────────────────────────────────────────────
// Returns all pinned findings for this client, newest pin first.
// Optional ?category= filter.

router.get('/pinned', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const { category } = req.query;

    const conditions = ['f.client_id = $1', 'f.pinned = TRUE'];
    const params     = [clientId];
    let p = 2;

    if (category) { conditions.push(`f.category = $${p++}`); params.push(category); }

    const { rows } = await pool.query(`
      SELECT f.id, f.category, f.severity, f.metric_key,
             f.entity_type, f.entity_id, f.entity_label,
             f.data_json, f.recommendation,
             f.pinned_at, f.pinned_note, f.created_at,
             s.ran_at AS snapshot_ran_at
      FROM advise_findings f
      JOIN advise_snapshots s ON s.id = f.snapshot_id
      WHERE ${conditions.join(' AND ')}
      ORDER BY
        CASE f.severity WHEN 'critical' THEN 1 WHEN 'watch' THEN 2 ELSE 3 END,
        f.pinned_at DESC NULLS LAST
    `, params);

    res.json({ success: true, findings: rows });
  } catch (err) {
    console.error('[adviseRouter] GET /pinned error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── POST /api/advise/findings/:id/pin ────────────────────────────────────────
// Pin a finding to the Recommendations tab.
// Body: { note?: string }

router.post('/findings/:id/pin', async (req, res) => {
  try {
    const clientId  = resolveClientId(req);
    const findingId = parseInt(req.params.id, 10);
    if (isNaN(findingId)) return res.status(400).json({ success: false, error: 'Invalid id' });

    const note = req.body?.note || null;

    const { rows } = await pool.query(`
      UPDATE advise_findings
      SET pinned = TRUE, pinned_at = NOW(), pinned_note = $1
      WHERE id = $2 AND client_id = $3
      RETURNING id, pinned, pinned_at, pinned_note
    `, [note, findingId, clientId]);

    if (!rows.length) return res.status(404).json({ success: false, error: 'Finding not found' });
    res.json({ success: true, finding: rows[0] });
  } catch (err) {
    console.error('[adviseRouter] POST /findings/:id/pin error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── POST /api/advise/findings/:id/unpin ──────────────────────────────────────
// Remove a finding from the Recommendations tab.

router.post('/findings/:id/unpin', async (req, res) => {
  try {
    const clientId  = resolveClientId(req);
    const findingId = parseInt(req.params.id, 10);
    if (isNaN(findingId)) return res.status(400).json({ success: false, error: 'Invalid id' });

    const { rows } = await pool.query(`
      UPDATE advise_findings
      SET pinned = FALSE, pinned_at = NULL, pinned_note = NULL
      WHERE id = $1 AND client_id = $2
      RETURNING id, pinned
    `, [findingId, clientId]);

    if (!rows.length) return res.status(404).json({ success: false, error: 'Finding not found' });
    res.json({ success: true, finding: rows[0] });
  } catch (err) {
    console.error('[adviseRouter] POST /findings/:id/unpin error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── GET /api/advise/rules ──────────────────────────────────────────────────
// Returns merged rule set for this client:
//   - All active global templates (with is_global:true)
//   - Per-instance overrides merged on top
//   - Custom instance-only rules (is_global:false)

router.get('/rules', async (req, res) => {
  try {
    const clientId = resolveClientId(req);

    const { rows: templates } = await pool.query(`
      SELECT id, rule_type, label, description, category, dataset_hint,
             config_schema, default_config, is_active, sort_order
      FROM advise_rule_templates
      ORDER BY sort_order
    `);

    const { rows: instanceRules } = await pool.query(`
      SELECT id, global_template_id, rule_type, label, description,
             category, dataset_hint, config, enabled, created_at, updated_at
      FROM advise_rules
      WHERE client_id = $1
      ORDER BY created_at
    `, [clientId]);

    const instanceByType = {};
    for (const r of instanceRules) instanceByType[r.rule_type] = r;

    // Build the full view: globals + any custom instance-only rules
    const result = [];

    for (const t of templates) {
      const override = instanceByType[t.rule_type] || null;
      result.push({
        source:            'global',
        template_id:       t.id,
        instance_rule_id:  override?.id || null,
        rule_type:         t.rule_type,
        label:             override?.label       || t.label,
        description:       override?.description || t.description,
        category:          override?.category    || t.category,
        dataset_hint:      override?.dataset_hint || t.dataset_hint,
        config_schema:     t.config_schema,
        default_config:    t.default_config,
        config:            override?.config      || {},
        effective_config:  Object.assign({}, t.default_config, override?.config || {}),
        enabled:           override ? override.enabled : t.is_active,
        is_overridden:     !!override,
        updated_at:        override?.updated_at || null,
      });
    }

    // Custom instance-only rules
    for (const r of instanceRules) {
      if (r.global_template_id !== null) continue;
      result.push({
        source:           'custom',
        template_id:      null,
        instance_rule_id: r.id,
        rule_type:        r.rule_type,
        label:            r.label,
        description:      r.description,
        category:         r.category,
        dataset_hint:     r.dataset_hint,
        config_schema:    {},
        default_config:   {},
        config:           r.config || {},
        effective_config: r.config || {},
        enabled:          r.enabled,
        is_overridden:    false,
        updated_at:       r.updated_at,
      });
    }

    res.json({ success: true, rules: result });
  } catch (err) {
    console.error('[adviseRouter] GET /rules error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── PUT /api/advise/rules/:rule_type ───────────────────────────────────────
// Create or update an instance rule for this client.
// For global template overrides: pass rule_type matching the template.
// For custom rules: pass a unique rule_type + handler must exist in RULE_HANDLERS.
// Body: { label?, description?, category?, dataset_hint?, config?, enabled? }

router.put('/rules/:rule_type', async (req, res) => {
  try {
    const clientId  = resolveClientId(req);
    const ruleType  = req.params.rule_type;
    const { label, description, category, dataset_hint, config, enabled } = req.body || {};

    // Look up global template (if any)
    const tmplRes = await pool.query(
      `SELECT id, label, description, category, dataset_hint FROM advise_rule_templates WHERE rule_type = $1`,
      [ruleType]
    );
    const template = tmplRes.rows[0] || null;

    const { rows } = await pool.query(`
      INSERT INTO advise_rules
        (client_id, global_template_id, rule_type, label, description,
         category, dataset_hint, config, enabled, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
      ON CONFLICT (client_id, rule_type) DO UPDATE SET
        label        = COALESCE(EXCLUDED.label,        advise_rules.label),
        description  = COALESCE(EXCLUDED.description,  advise_rules.description),
        category     = COALESCE(EXCLUDED.category,     advise_rules.category),
        dataset_hint = COALESCE(EXCLUDED.dataset_hint, advise_rules.dataset_hint),
        config       = COALESCE(EXCLUDED.config,       advise_rules.config),
        enabled      = COALESCE(EXCLUDED.enabled,      advise_rules.enabled),
        updated_at   = NOW()
      RETURNING *
    `, [
      clientId,
      template?.id || null,
      ruleType,
      label       || template?.label       || ruleType,
      description || template?.description || null,
      category    || template?.category    || 'financial',
      dataset_hint || template?.dataset_hint || null,
      config ? JSON.stringify(config) : '{}',
      enabled !== undefined ? enabled : true,
    ]);

    res.json({ success: true, rule: rows[0] });
  } catch (err) {
    console.error('[adviseRouter] PUT /rules/:rule_type error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── DELETE /api/advise/rules/:rule_type ───────────────────────────────────
// Remove instance override (reverts global template to defaults)
// or deletes a custom rule entirely.

router.delete('/rules/:rule_type', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const ruleType = req.params.rule_type;

    const { rowCount } = await pool.query(
      `DELETE FROM advise_rules WHERE client_id = $1 AND rule_type = $2`,
      [clientId, ruleType]
    );

    if (!rowCount) return res.status(404).json({ success: false, error: 'Rule not found' });
    res.json({ success: true });
  } catch (err) {
    console.error('[adviseRouter] DELETE /rules/:rule_type error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── GET /api/advise/rule-templates ──────────────────────────────────────────
// Returns all global templates (for admin display).

router.get('/rule-templates', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT id, rule_type, label, description, category, dataset_hint,
             config_schema, default_config, is_active, sort_order
      FROM advise_rule_templates
      ORDER BY sort_order
    `);
    res.json({ success: true, templates: rows });
  } catch (err) {
    console.error('[adviseRouter] GET /rule-templates error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

module.exports = router;
