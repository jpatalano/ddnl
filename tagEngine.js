/**
 * tagEngine.js — System tag rule evaluation engine
 *
 * Handles two trigger types:
 *   schedule   — run nightly for all clients (runScheduledTriggers)
 *   on_ingest  — run async after a dataset ingest completes (runOnIngestTriggers)
 *
 * Both resolve to the same core flow:
 *   1. Load matching system_tag_rules
 *   2. For each rule, query ES for records that match the conditions
 *   3. Apply or remove the tag on matching records
 *   4. Sync __tags array on ES docs (update-by-query)
 *   5. Update client_tag_assignments in Postgres
 *   6. Log run stats back to system_tag_rules.last_run_at / last_run_count
 *
 * Design principles:
 *   - Never throws — all errors are caught and logged
 *   - Never blocks ingest — always called fire-and-forget from ingest routes
 *   - Contained — no imports from ingestRouter or server.js
 *   - Condition shape: { segmentName, operator, value } — same as everywhere else
 */

'use strict';

const { pool } = require('./db');
const es       = require('./esClient');

// ── Condition → ES query clause ───────────────────────────────────────────────

/**
 * Convert a single { segmentName, operator, value } condition to an ES must clause.
 * Returns null for unknown operators (caller skips it with a warning).
 */
function conditionToClause(cond) {
  const { segmentName, operator, value } = cond;
  switch (operator) {
    case 'eq':     return { term:  { [segmentName]: value } };
    case 'neq':    return { bool:  { must_not: [{ term: { [segmentName]: value } }] } };
    case 'in':     return { terms: { [segmentName]: Array.isArray(value) ? value : [value] } };
    case 'not_in': return { bool:  { must_not: [{ terms: { [segmentName]: Array.isArray(value) ? value : [value] } }] } };
    case 'gt':     return { range: { [segmentName]: { gt: value } } };
    case 'gte':    return { range: { [segmentName]: { gte: value } } };
    case 'lt':     return { range: { [segmentName]: { lt: value } } };
    case 'lte':    return { range: { [segmentName]: { lte: value } } };
    case 'exists': return { exists: { field: segmentName } };
    case 'missing':return { bool: { must_not: [{ exists: { field: segmentName } }] } };
    default:
      console.warn(`[tagEngine] Unknown operator '${operator}' in condition — skipping`);
      return null;
  }
}

/**
 * Build a full ES bool query from a conditions array.
 * All conditions are AND'd together (must clauses).
 * Always excludes soft-deleted records.
 */
function buildEsQuery(conditions) {
  const must = [
    // Never tag deleted/archived records
    { bool: { must_not: [{ terms: { _status: ['deleted', 'archived'] } }] } }
  ];
  for (const cond of (conditions || [])) {
    const clause = conditionToClause(cond);
    if (clause) must.push(clause);
  }
  return { bool: { must } };
}

// ── ES __tags sync ────────────────────────────────────────────────────────────

/**
 * Add a tag label to __tags on all ES records matching recordIds.
 * Uses update-by-query with a Painless script — atomic, no race conditions.
 */
async function esAddTag(alias, recordIds, tagLabel) {
  if (!recordIds.length) return 0;
  const client = es.getClient();
  const r = await client.updateByQuery({
    index: alias,
    refresh: false,
    body: {
      query: { ids: { values: recordIds } },
      script: {
        lang: 'painless',
        source: `
          if (ctx._source.__tags == null) { ctx._source.__tags = []; }
          if (!ctx._source.__tags.contains(params.tag)) { ctx._source.__tags.add(params.tag); }
          else { ctx.op = 'noop'; }
        `,
        params: { tag: tagLabel }
      }
    }
  });
  return r.updated ?? 0;
}

/**
 * Remove a tag label from __tags on all ES records matching recordIds.
 */
async function esRemoveTag(alias, recordIds, tagLabel) {
  if (!recordIds.length) return 0;
  const client = es.getClient();
  const r = await client.updateByQuery({
    index: alias,
    refresh: false,
    body: {
      query: { ids: { values: recordIds } },
      script: {
        lang: 'painless',
        source: `
          if (ctx._source.__tags != null && ctx._source.__tags.contains(params.tag)) {
            ctx._source.__tags.remove(ctx._source.__tags.indexOf(params.tag));
          } else { ctx.op = 'noop'; }
        `,
        params: { tag: tagLabel }
      }
    }
  });
  return r.updated ?? 0;
}

// ── Core rule runner ──────────────────────────────────────────────────────────

/**
 * Execute a single system_tag_rule against its target dataset.
 *
 * For 'apply' rules:
 *   - Query target dataset for records matching conditions
 *   - Upsert assignments in Postgres for new matches
 *   - Add tag label to __tags in ES
 *
 * For 'remove' rules:
 *   - Find records that currently have this tag assigned
 *   - Of those, find ones that now match the remove conditions
 *   - Delete their assignments from Postgres
 *   - Remove tag label from __tags in ES
 *
 * Returns { applied, removed, errors }
 */
async function executeRule(rule, tag) {
  const { clientId } = rule;
  const targetDataset = tag.target_dataset;
  const alias = es.aliasName(clientId, targetDataset);
  const esClient = es.getClient();
  const stats = { applied: 0, removed: 0, errors: [] };

  try {
    if (rule.rule_action === 'apply') {
      // ── Apply: find all records matching conditions ──────────────────────
      const query = buildEsQuery(rule.conditions);

      // Scroll through all matching records (could be large)
      const matchingIds = [];
      const resp = await esClient.search({
        index: alias,
        size: 1000,
        query,
        _source: false,
        track_total_hits: true
      });

      for (const hit of resp.hits.hits) matchingIds.push(hit._id);

      // Handle pagination if > 1000 results
      let total = resp.hits.total?.value ?? resp.hits.hits.length;
      if (total > 1000) {
        // Use search_after for deep pagination
        let lastSort = resp.hits.hits[resp.hits.hits.length - 1]?.sort;
        while (matchingIds.length < total && lastSort) {
          const page = await esClient.search({
            index: alias, size: 1000, query, _source: false,
            sort: [{ _id: 'asc' }], search_after: lastSort
          });
          for (const hit of page.hits.hits) matchingIds.push(hit._id);
          lastSort = page.hits.hits[page.hits.hits.length - 1]?.sort;
          if (!page.hits.hits.length) break;
        }
      }

      if (!matchingIds.length) return stats;

      // Upsert assignments in Postgres (ignore already-assigned)
      const newIds = [];
      for (const recordId of matchingIds) {
        const { rowCount } = await pool.query(
          `INSERT INTO client_tag_assignments (client_id, tag_id, dataset_name, record_id, assigned_by)
           VALUES ($1, $2, $3, $4, 'system')
           ON CONFLICT (tag_id, record_id) DO NOTHING`,
          [clientId, tag.id, targetDataset, recordId]
        );
        if (rowCount) newIds.push(recordId);
      }

      // Sync ES __tags for newly assigned records only
      if (newIds.length) {
        stats.applied = await esAddTag(alias, newIds, tag.label);
      }

    } else if (rule.rule_action === 'remove') {
      // ── Remove: find currently-tagged records that match remove conditions ─
      // Get all record_ids currently assigned this tag
      const { rows: assigned } = await pool.query(
        `SELECT record_id FROM client_tag_assignments
         WHERE tag_id = $1 AND dataset_name = $2`,
        [tag.id, targetDataset]
      );
      if (!assigned.length) return stats;

      const assignedIds = assigned.map(r => r.record_id);

      // Of those, find which ones match the remove conditions
      const removeQuery = {
        bool: {
          must: [
            { ids: { values: assignedIds } },
            ...buildEsQuery(rule.conditions).bool.must
          ]
        }
      };

      const resp = await esClient.search({
        index: alias, size: 1000, query: removeQuery, _source: false
      });
      const toRemove = resp.hits.hits.map(h => h._id);
      if (!toRemove.length) return stats;

      // Delete assignments from Postgres
      await pool.query(
        `DELETE FROM client_tag_assignments
         WHERE tag_id = $1 AND record_id = ANY($2::text[])`,
        [tag.id, toRemove]
      );

      // Sync ES __tags
      stats.removed = await esRemoveTag(alias, toRemove, tag.label);
    }
  } catch (e) {
    console.error(`[tagEngine] Rule ${rule.id} (${tag.label} ${rule.rule_action}) failed:`, e.message);
    stats.errors.push(e.message);
  }

  // Update last_run stats on the rule
  try {
    const count = stats.applied + stats.removed;
    await pool.query(
      `UPDATE system_tag_rules SET last_run_at = NOW(), last_run_count = $1 WHERE id = $2`,
      [count, rule.id]
    );
  } catch (e) { /* non-fatal */ }

  return stats;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Run all schedule-triggered system tag rules for a client.
 * Called by nightly cron.
 */
async function runScheduledTriggers(clientId) {
  console.log(`[tagEngine] Running scheduled triggers for client=${clientId}`);
  let totalApplied = 0, totalRemoved = 0;

  try {
    const { rows: rules } = await pool.query(
      `SELECT r.*, t.label, t.target_dataset, t.id as tag_real_id, t.is_active as tag_active
       FROM system_tag_rules r
       JOIN client_tags t ON t.id = r.tag_id
       WHERE r.client_id = $1
         AND r.trigger_type = 'schedule'
         AND r.is_active = TRUE
         AND t.is_active = TRUE`,
      [clientId]
    );

    for (const rule of rules) {
      const tag = {
        id: rule.tag_real_id,
        label: rule.label,
        target_dataset: rule.target_dataset
      };
      const stats = await executeRule({ ...rule, clientId }, tag);
      totalApplied += stats.applied;
      totalRemoved += stats.removed;
      console.log(`[tagEngine] Rule ${rule.id} (${rule.label} ${rule.rule_action}): applied=${stats.applied} removed=${stats.removed}`);
    }
  } catch (e) {
    console.error(`[tagEngine] runScheduledTriggers failed for client=${clientId}:`, e.message);
  }

  console.log(`[tagEngine] Scheduled run complete for client=${clientId}: applied=${totalApplied} removed=${totalRemoved}`);
  return { totalApplied, totalRemoved };
}

/**
 * Run all on_ingest-triggered system tag rules for a given dataset.
 * Called async after bulk/single ingest completes — never awaited by ingest routes.
 *
 * @param {string} clientId
 * @param {string} datasetName — the dataset that was just ingested
 */
async function runOnIngestTriggers(clientId, datasetName) {
  try {
    const { rows: rules } = await pool.query(
      `SELECT r.*, t.label, t.target_dataset, t.id as tag_real_id
       FROM system_tag_rules r
       JOIN client_tags t ON t.id = r.tag_id
       WHERE r.client_id = $1
         AND r.trigger_type = 'on_ingest'
         AND r.trigger_dataset = $2
         AND r.is_active = TRUE
         AND t.is_active = TRUE`,
      [clientId, datasetName]
    );

    if (!rules.length) return;

    console.log(`[tagEngine] on_ingest trigger: client=${clientId} dataset=${datasetName} rules=${rules.length}`);

    for (const rule of rules) {
      const tag = {
        id: rule.tag_real_id,
        label: rule.label,
        target_dataset: rule.target_dataset
      };
      const stats = await executeRule({ ...rule, clientId }, tag);
      console.log(`[tagEngine] Rule ${rule.id} (${rule.label} ${rule.rule_action}): applied=${stats.applied} removed=${stats.removed}`);
    }
  } catch (e) {
    // Never propagate — this is always called fire-and-forget
    console.error(`[tagEngine] runOnIngestTriggers failed for client=${clientId} dataset=${datasetName}:`, e.message);
  }
}

/**
 * Manually re-evaluate all system tag rules for a single record.
 * Useful after a user manually changes a record's field values.
 * Not used in the normal ingest flow.
 */
async function retagRecord(clientId, datasetName, recordId) {
  try {
    const { rows: rules } = await pool.query(
      `SELECT r.*, t.label, t.target_dataset, t.id as tag_real_id
       FROM system_tag_rules r
       JOIN client_tags t ON t.id = r.tag_id
       WHERE r.client_id = $1
         AND t.target_dataset = $2
         AND r.is_active = TRUE
         AND t.is_active = TRUE`,
      [clientId, datasetName]
    );
    for (const rule of rules) {
      const tag = { id: rule.tag_real_id, label: rule.label, target_dataset: rule.target_dataset };
      await executeRule({ ...rule, clientId }, tag);
    }
  } catch (e) {
    console.error(`[tagEngine] retagRecord failed for ${clientId}/${datasetName}/${recordId}:`, e.message);
  }
}

module.exports = { runScheduledTriggers, runOnIngestTriggers, retagRecord, esAddTag, esRemoveTag };
