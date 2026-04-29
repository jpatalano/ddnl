/**
 * aiQueryEngine.js — AI Chat query engine
 *
 * Responsibilities:
 *   1. Schema injection — builds a compact schema summary of all visible_to_ai
 *      datasets for the LLM system prompt
 *   2. Tool call execution — parses AI tool calls and executes ES queries
 *   3. Tag action staging — validates + stages tag apply/remove as confirmation payloads
 *   4. Result formatting — converts ES hits/aggs into concise LLM-digestible summaries
 *
 * Tool definitions (sent to LLM):
 *   query_dataset   — search/aggregate a dataset
 *   apply_tag       — stage a tag apply action (returns confirmation card)
 *   remove_tag      — stage a tag remove action (returns confirmation card)
 *
 * All functions are pure or DB/ES-bound — no Express imports.
 */

'use strict';

const { pool } = require('./db');
const es        = require('./esClient');

// ── Schema builder ────────────────────────────────────────────────────────────

/**
 * Load all visible_to_ai datasets for a client and build a compact schema
 * summary string to inject into the LLM system prompt.
 *
 * Returns { schemaSummary: string, datasets: Array }
 */
async function buildSchemaContext(clientId) {
  const { rows } = await pool.query(`
    SELECT dd.name, dd.label, dd.es_alias,
           dsv.fields
    FROM dataset_definitions dd
    LEFT JOIN dataset_schema_versions dsv
      ON dsv.dataset_id = dd.id AND dsv.version = dd.current_version
    WHERE dd.client_id = $1
      AND dd.visible_to_ai = TRUE
      AND dd.is_active = TRUE
    ORDER BY dd.label
  `, [clientId]);

  if (!rows.length) return { schemaSummary: 'No datasets available.', datasets: [] };

  const lines = ['Available datasets:'];
  for (const ds of rows) {
    const fields = Array.isArray(ds.fields) ? ds.fields : [];
    const fieldList = fields
      .slice(0, 30) // cap to avoid prompt bloat
      .map(f => `${f.name}(${f.type || 'string'})`)
      .join(', ');
    lines.push(`- ${ds.label} [key: ${ds.name}]: ${fieldList || 'no fields published yet'}`);
  }

  return { schemaSummary: lines.join('\n'), datasets: rows };
}

// ── Tool definitions ──────────────────────────────────────────────────────────

/**
 * Returns the tools array to pass to the LLM (OpenAI function-calling format).
 * Anthropic uses the same schema.
 */
function getToolDefinitions() {
  return [
    {
      name: 'query_dataset',
      description:
        'Search or aggregate a dataset to answer questions. ' +
        'Use this whenever the user asks about data — counts, averages, lists, comparisons, etc. ' +
        'You can filter, group, sort, and aggregate. ' +
        'Returns a concise result set you should summarize for the user.',
      input_schema: {
        type: 'object',
        required: ['dataset_name', 'intent'],
        properties: {
          dataset_name: {
            type: 'string',
            description: 'The dataset key (e.g. "customers", "invoices_20260101_123456")'
          },
          intent: {
            type: 'string',
            description: 'Plain-language description of what you are looking for'
          },
          filters: {
            type: 'array',
            description: 'Array of { segmentName, operator, value } filter objects',
            items: {
              type: 'object',
              properties: {
                segmentName: { type: 'string' },
                operator:    { type: 'string', enum: ['eq','neq','in','not_in','gt','gte','lt','lte','exists','missing','contains'] },
                value:       {}
              },
              required: ['segmentName', 'operator']
            }
          },
          group_by: {
            type: 'string',
            description: 'Field name to group/aggregate by'
          },
          metric_field: {
            type: 'string',
            description: 'Numeric field to sum/avg/count'
          },
          metric_op: {
            type: 'string',
            enum: ['sum', 'avg', 'min', 'max', 'count'],
            description: 'Aggregation operation on metric_field'
          },
          sort_by: {
            type: 'string',
            description: 'Field to sort results by'
          },
          sort_dir: {
            type: 'string',
            enum: ['asc', 'desc'],
            description: 'Sort direction'
          },
          size: {
            type: 'integer',
            description: 'Max records to return (default 20, max 100)',
            default: 20
          },
          return_fields: {
            type: 'array',
            items: { type: 'string' },
            description: 'Specific fields to include in results'
          }
        }
      }
    },
    {
      name: 'apply_tag',
      description:
        'Stage a tag-apply action for user confirmation. ' +
        'Use when the user asks to tag, flag, or label records matching certain criteria. ' +
        'Returns a confirmation card — the user must confirm before the tag is applied.',
      input_schema: {
        type: 'object',
        required: ['dataset_name', 'tag_name', 'filters', 'reason'],
        properties: {
          dataset_name: { type: 'string', description: 'Dataset key' },
          tag_name:     { type: 'string', description: 'Tag name or label to apply' },
          filters: {
            type: 'array',
            description: 'Filter conditions identifying the records to tag',
            items: {
              type: 'object',
              properties: {
                segmentName: { type: 'string' },
                operator:    { type: 'string' },
                value:       {}
              },
              required: ['segmentName', 'operator']
            }
          },
          reason: { type: 'string', description: 'Plain-language reason for tagging' }
        }
      }
    },
    {
      name: 'remove_tag',
      description:
        'Stage a tag-remove action for user confirmation. ' +
        'Returns a confirmation card — the user must confirm before the tag is removed.',
      input_schema: {
        type: 'object',
        required: ['dataset_name', 'tag_name', 'filters', 'reason'],
        properties: {
          dataset_name: { type: 'string', description: 'Dataset key' },
          tag_name:     { type: 'string', description: 'Tag name or label to remove' },
          filters: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                segmentName: { type: 'string' },
                operator:    { type: 'string' },
                value:       {}
              },
              required: ['segmentName', 'operator']
            }
          },
          reason: { type: 'string', description: 'Plain-language reason for removing tag' }
        }
      }
    }
  ];
}

// ── Filter → ES clause ────────────────────────────────────────────────────────

function filterToClause(f) {
  const { segmentName, operator, value } = f;
  switch (operator) {
    case 'eq':       return { term:  { [segmentName]: value } };
    case 'neq':      return { bool:  { must_not: [{ term: { [segmentName]: value } }] } };
    case 'in':       return { terms: { [segmentName]: Array.isArray(value) ? value : [value] } };
    case 'not_in':   return { bool:  { must_not: [{ terms: { [segmentName]: Array.isArray(value) ? value : [value] } }] } };
    case 'gt':       return { range: { [segmentName]: { gt: value } } };
    case 'gte':      return { range: { [segmentName]: { gte: value } } };
    case 'lt':       return { range: { [segmentName]: { lt: value } } };
    case 'lte':      return { range: { [segmentName]: { lte: value } } };
    case 'exists':   return { exists: { field: segmentName } };
    case 'missing':  return { bool: { must_not: [{ exists: { field: segmentName } }] } };
    case 'contains': return { match_phrase: { [segmentName]: value } };
    default:         return null;
  }
}

function buildEsQuery(filters) {
  const must = [{ bool: { must_not: [{ term: { _status: 'deleted' } }] } }];
  if (Array.isArray(filters)) {
    for (const f of filters) {
      const clause = filterToClause(f);
      if (clause) must.push(clause);
    }
  }
  return { bool: { must } };
}

// ── Tool executor ─────────────────────────────────────────────────────────────

/**
 * Execute a query_dataset tool call against ES.
 * Returns a plain object suitable for JSON serialization and LLM consumption.
 */
async function executeQueryDataset(clientId, params) {
  const {
    dataset_name, intent, filters, group_by,
    metric_field, metric_op, sort_by, sort_dir,
    size = 20, return_fields
  } = params;

  // Resolve ES alias for this client+dataset
  const dsRes = await pool.query(`
    SELECT es_alias FROM dataset_definitions
    WHERE client_id = $1 AND name = $2 AND is_active = TRUE
  `, [clientId, dataset_name]);

  if (!dsRes.rows.length || !dsRes.rows[0].es_alias) {
    return { error: `Dataset "${dataset_name}" not found or has no published index.` };
  }

  const alias    = dsRes.rows[0].es_alias;
  const esClient = es.getClient();
  const query    = buildEsQuery(filters);
  const cappedSize = Math.min(size, 100);

  try {
    // Aggregation mode
    if (group_by || metric_field) {
      const aggs = {};
      if (group_by) {
        const subAggs = {};
        if (metric_field && metric_op && metric_op !== 'count') {
          subAggs[`${metric_op}_${metric_field}`] = { [metric_op]: { field: metric_field } };
        }
        aggs.groups = {
          terms: {
            field: group_by,
            size:  50,
            ...(sort_by === group_by || !sort_by
              ? (sort_dir === 'asc' ? { order: { _key: 'asc' } } : { order: { _count: 'desc' } })
              : {})
          },
          aggs: subAggs
        };
      }
      if (metric_field && !group_by) {
        const op = metric_op || 'sum';
        aggs[`${op}_result`] = { [op]: { field: metric_field } };
      }
      if (metric_op === 'count' || (!metric_field && !group_by)) {
        aggs.total = { value_count: { field: '_id' } };
      }

      const resp = await esClient.search({ index: alias, size: 0, query, aggs });
      const total = resp.hits?.total?.value || 0;

      // Format aggregation results
      if (resp.aggregations?.groups?.buckets) {
        const rows = resp.aggregations.groups.buckets.map(b => {
          const row = { [group_by]: b.key, count: b.doc_count };
          for (const [k, v] of Object.entries(b)) {
            if (k !== 'doc_count' && k !== 'key' && k !== 'key_as_string' && v?.value !== undefined) {
              row[k] = Math.round(v.value * 100) / 100;
            }
          }
          return row;
        });
        return { type: 'aggregation', total_docs: total, group_by, rows, intent };
      }

      // Single metric result
      const metricResult = {};
      for (const [k, v] of Object.entries(resp.aggregations || {})) {
        if (v?.value !== undefined) metricResult[k] = Math.round(v.value * 100) / 100;
      }
      return { type: 'metric', total_docs: total, result: metricResult, intent };
    }

    // Search mode
    const sortClause = sort_by
      ? [{ [sort_by]: { order: sort_dir || 'desc' } }]
      : [{ _score: 'desc' }];

    const resp = await esClient.search({
      index: alias,
      size:  cappedSize,
      query,
      sort:  sortClause,
      ...(return_fields ? { _source: return_fields } : {})
    });

    const total = resp.hits?.total?.value || 0;
    const hits  = resp.hits?.hits?.map(h => h._source) || [];

    return { type: 'search', total_docs: total, returned: hits.length, rows: hits, intent };
  } catch (err) {
    console.error('[aiQueryEngine] ES query error:', err.message);
    return { error: `Query failed: ${err.message}` };
  }
}

/**
 * Stage a tag action (apply or remove) — does NOT execute it.
 * Returns an action_payload that the UI renders as a confirmation card.
 */
async function stageTagAction(clientId, action, params) {
  const { dataset_name, tag_name, filters, reason } = params;

  // Resolve tag — match by name or label
  const tagRes = await pool.query(`
    SELECT id, name, label, color, tag_type, target_dataset
    FROM client_tags
    WHERE client_id = $1
      AND is_active = TRUE
      AND (LOWER(name) = LOWER($2) OR LOWER(label) = LOWER($2))
    LIMIT 1
  `, [clientId, tag_name]);

  // If tag not found, return a helpful error
  if (!tagRes.rows.length) {
    const allTags = await pool.query(`
      SELECT name, label FROM client_tags
      WHERE client_id = $1 AND is_active = TRUE
      ORDER BY label
    `, [clientId]);
    return {
      error: `Tag "${tag_name}" not found.`,
      available_tags: allTags.rows.map(t => `${t.label} (${t.name})`)
    };
  }

  const tag = tagRes.rows[0];

  // Count matching records
  const dsRes = await pool.query(`
    SELECT es_alias FROM dataset_definitions
    WHERE client_id = $1 AND name = $2 AND is_active = TRUE
  `, [clientId, dataset_name]);

  let matchCount = 0;
  if (dsRes.rows[0]?.es_alias) {
    try {
      const esClient = es.getClient();
      const query = buildEsQuery(filters);
      const resp  = await esClient.count({ index: dsRes.rows[0].es_alias, query });
      matchCount  = resp.count || 0;
    } catch (e) {
      matchCount = -1; // unknown
    }
  }

  return {
    type:        'tag_action',
    action,                      // 'apply' | 'remove'
    tag_id:      tag.id,
    tag_name:    tag.name,
    tag_label:   tag.label,
    tag_color:   tag.color,
    dataset_name,
    filters:     filters || [],
    reason,
    match_count: matchCount,
    // This payload is stored in ai_messages.action_payload and rendered as a confirmation card
    confirmed:   false
  };
}

// ── Tool dispatch ─────────────────────────────────────────────────────────────

/**
 * Execute a single tool call emitted by the LLM.
 * Returns { tool_result, action_payload }
 *   - tool_result  : data to feed back to the LLM for final answer generation
 *   - action_payload: non-null when a confirmation card should be shown
 */
async function executeTool(clientId, toolName, toolInput) {
  switch (toolName) {
    case 'query_dataset': {
      const result = await executeQueryDataset(clientId, toolInput);
      return { tool_result: result, action_payload: null };
    }
    case 'apply_tag': {
      const payload = await stageTagAction(clientId, 'apply', toolInput);
      if (payload.error) return { tool_result: payload, action_payload: null };
      return {
        tool_result: { staged: true, match_count: payload.match_count, tag_label: payload.tag_label },
        action_payload: payload
      };
    }
    case 'remove_tag': {
      const payload = await stageTagAction(clientId, 'remove', toolInput);
      if (payload.error) return { tool_result: payload, action_payload: null };
      return {
        tool_result: { staged: true, match_count: payload.match_count, tag_label: payload.tag_label },
        action_payload: payload
      };
    }
    default:
      return { tool_result: { error: `Unknown tool: ${toolName}` }, action_payload: null };
  }
}

// ── Tag action executor (called when user confirms) ───────────────────────────

/**
 * Execute a confirmed tag action payload.
 * Called by aiChatRouter POST /confirm.
 */
async function executeConfirmedTagAction(clientId, payload) {
  const { action, tag_id, tag_name, tag_label, dataset_name, filters } = payload;

  // Resolve ES alias
  const dsRes = await pool.query(`
    SELECT es_alias FROM dataset_definitions
    WHERE client_id = $1 AND name = $2 AND is_active = TRUE
  `, [clientId, dataset_name]);

  if (!dsRes.rows[0]?.es_alias) {
    throw new Error(`Dataset "${dataset_name}" not found or no ES index`);
  }

  const alias    = dsRes.rows[0].es_alias;
  const esClient = es.getClient();
  const query    = buildEsQuery(filters);

  // Fetch matching record IDs
  const resp = await esClient.search({
    index: alias,
    size:  500,
    query,
    _source: false
  });
  const recordIds = (resp.hits?.hits || []).map(h => h._id);

  if (!recordIds.length) return { applied: 0, removed: 0 };

  // Delegate to tagEngine for Postgres + ES sync
  const tagEngine = require('./tagEngine');
  let count = 0;
  for (const recordId of recordIds) {
    try {
      if (action === 'apply') {
        await tagEngine.esAddTag(clientId, dataset_name, alias, recordId, tag_id, tag_name, 'ai_chat');
      } else {
        await tagEngine.esRemoveTag(clientId, dataset_name, alias, recordId, tag_id, tag_name);
      }
      count++;
    } catch (err) {
      console.error(`[aiQueryEngine] Tag ${action} failed for record ${recordId}:`, err.message);
    }
  }

  return action === 'apply' ? { applied: count, removed: 0 } : { applied: 0, removed: count };
}

module.exports = {
  buildSchemaContext,
  getToolDefinitions,
  executeTool,
  executeConfirmedTagAction
};
