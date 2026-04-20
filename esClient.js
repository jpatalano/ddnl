/**
 * esClient.js — Elasticsearch lifecycle wrapper
 *
 * Responsibilities:
 *  - Single shared client (lazy-init)
 *  - Index naming: {instanceId}__{datasetName}__v{version}  e.g. fcc__sales__v1
 *  - Alias naming: {instanceId}__{datasetName}              e.g. fcc__sales
 *  - create index from dataset schema
 *  - bulk index documents
 *  - atomic alias swap (zero-downtime reindex)
 *  - delete old versions
 *  - health / stats
 */

const { Client } = require('@elastic/elasticsearch');

let _client = null;

function getClient() {
  if (!_client) {
    const url = process.env.ELASTICSEARCH_URL || 'http://elasticsearch.railway.internal:9200';
    _client = new Client({ node: url, requestTimeout: 30000 });
  }
  return _client;
}

// ── Naming helpers ─────────────────────────────────────────────────────────────

function indexName(instanceId, datasetName, version) {
  return `${instanceId}__${datasetName}__v${version}`.toLowerCase();
}

function aliasName(instanceId, datasetName) {
  return `${instanceId}__${datasetName}`.toLowerCase();
}

// ── ES type mapping from our segmentType ──────────────────────────────────────

function toEsType(segmentType, displayFormat) {
  if (segmentType === 'date')   return { type: 'date' };
  if (segmentType === 'number') return { type: 'double' };
  // string — keyword for exact match/agg + text for search
  return { type: 'keyword' };
}

function buildMappings(fields) {
  // fields: [{ name, fieldType('segment'|'metric'), segmentType, displayFormat }]
  const properties = {};
  for (const f of fields) {
    if (f.fieldType === 'metric') {
      properties[f.name] = { type: 'double' };
    } else {
      properties[f.name] = toEsType(f.segmentType, f.displayFormat);
    }
  }
  // Always include internal meta
  properties['__instance_id']  = { type: 'keyword' };
  properties['__ingested_at']  = { type: 'date' };
  return { properties };
}

// ── Index lifecycle ────────────────────────────────────────────────────────────

/**
 * Create a new versioned index for a dataset.
 * Returns the index name created.
 */
async function createIndex(instanceId, datasetName, version, fields) {
  const es   = getClient();
  const name = indexName(instanceId, datasetName, version);

  await es.indices.create({
    index: name,
    body: {
      settings: {
        number_of_shards:   1,
        number_of_replicas: 0,   // single-node dev; bump for prod
        'index.mapping.total_fields.limit': 500
      },
      mappings: buildMappings(fields)
    }
  });

  return name;
}

/**
 * Check if a versioned index exists.
 */
async function indexExists(instanceId, datasetName, version) {
  const es = getClient();
  const name = indexName(instanceId, datasetName, version);
  const exists = await es.indices.exists({ index: name });
  return exists;
}

/**
 * Point alias to a new index version, atomically removing from the old one.
 * If no prior alias exists, just adds it.
 */
async function swapAlias(instanceId, datasetName, newVersion, oldVersion = null) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);
  const newIdx = indexName(instanceId, datasetName, newVersion);

  const actions = [{ add: { index: newIdx, alias } }];

  if (oldVersion != null) {
    const oldIdx = indexName(instanceId, datasetName, oldVersion);
    actions.unshift({ remove: { index: oldIdx, alias } });
  }

  await es.indices.updateAliases({ body: { actions } });
}

/**
 * Delete a specific versioned index (used after rollback window expires).
 */
async function deleteIndex(instanceId, datasetName, version) {
  const es   = getClient();
  const name = indexName(instanceId, datasetName, version);
  await es.indices.delete({ index: name, ignore_unavailable: true });
}

// ── Document ingest ────────────────────────────────────────────────────────────

/**
 * Bulk index documents into the current alias.
 * docs: array of plain objects — we add __instance_id + __ingested_at.
 * Returns { indexed, failed, errors[] }
 */
async function bulkIndex(instanceId, datasetName, docs) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);
  const now   = new Date().toISOString();

  const body = docs.flatMap(doc => [
    { index: { _index: alias } },
    { ...doc, __instance_id: instanceId, __ingested_at: now }
  ]);

  const result = await es.bulk({ refresh: true, body });

  const failed = (result.items || []).filter(i => i.index?.error);
  return {
    indexed: docs.length - failed.length,
    failed:  failed.length,
    errors:  failed.slice(0, 10).map(i => i.index.error)
  };
}

/**
 * Replace all docs for an instance in a dataset (full reindex from source array).
 * Deletes by query first, then bulk inserts.
 * Use for full refresh from a seed script or pull source.
 */
async function replaceAll(instanceId, datasetName, docs) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);

  // Delete existing docs for this instance
  await es.deleteByQuery({
    index: alias,
    refresh: true,
    body: { query: { term: { __instance_id: instanceId } } }
  });

  if (docs.length === 0) return { indexed: 0, failed: 0, errors: [] };
  return bulkIndex(instanceId, datasetName, docs);
}

// ── Query layer ────────────────────────────────────────────────────────────────

/**
 * Build an ES aggregation query from the BI API query shape.
 * Supports: groupBySegments, metrics (SUM/AVG/COUNT/MIN/MAX/COUNT_DISTINCT), filters, orderBy, pagination.
 */
function buildEsQuery(instanceId, { groupBySegments = [], metrics = [], filters = [], orderBy = [], pagination = {} }) {
  const { page = 1, pageSize = 1000 } = pagination;

  // ── Filters → ES query ──
  const mustClauses = [{ term: { __instance_id: instanceId } }];

  for (const f of filters) {
    const { segmentName, operator, value } = f;
    switch (operator) {
      case 'eq':       mustClauses.push({ term:   { [segmentName]: value } }); break;
      case 'neq':      mustClauses.push({ bool:   { must_not: [{ term: { [segmentName]: value } }] } }); break;
      case 'in':       mustClauses.push({ terms:  { [segmentName]: Array.isArray(value) ? value : [value] } }); break;
      case 'not_in':   mustClauses.push({ bool:   { must_not: [{ terms: { [segmentName]: Array.isArray(value) ? value : [value] } }] } }); break;
      case 'gte':      mustClauses.push({ range:  { [segmentName]: { gte: value } } }); break;
      case 'lte':      mustClauses.push({ range:  { [segmentName]: { lte: value } } }); break;
      case 'lt':       mustClauses.push({ range:  { [segmentName]: { lt:  value } } }); break;
      case 'gt':       mustClauses.push({ range:  { [segmentName]: { gt:  value } } }); break;
      case 'contains': mustClauses.push({ wildcard: { [segmentName]: `*${value}*` } }); break;
      case 'starts':   mustClauses.push({ prefix:   { [segmentName]: value } }); break;
      case 'is_null':  mustClauses.push({ bool:   { must_not: [{ exists: { field: segmentName } }] } }); break;
      case 'not_null': mustClauses.push({ exists: { field: segmentName } }); break;
    }
  }

  const esQuery = { bool: { must: mustClauses } };

  // ── No groupBy → single bucket aggregation ──
  if (groupBySegments.length === 0) {
    const aggs = {};
    for (const m of metrics) {
      const aggType = _esAggType(m.aggregation || 'SUM');
      aggs[m.alias || m.metricName] = { [aggType]: { field: m.metricName } };
    }
    return { noGroup: true, esBody: { query: esQuery, aggs, size: 0 }, metrics };
  }

  // ── GroupBy → composite aggregation (handles pagination) ──
  const sources = groupBySegments.map(seg => ({
    [seg]: { terms: { field: seg, missing_bucket: true } }
  }));

  const aggs = {};
  for (const m of metrics) {
    const aggType = _esAggType(m.aggregation || 'SUM');
    if (aggType === 'value_count') {
      aggs[m.alias || m.metricName] = { value_count: { field: m.metricName } };
    } else if (aggType === 'cardinality') {
      aggs[m.alias || m.metricName] = { cardinality: { field: m.metricName } };
    } else {
      aggs[m.alias || m.metricName] = { [aggType]: { field: m.metricName } };
    }
  }

  return {
    noGroup: false,
    esBody: {
      query: esQuery,
      size: 0,
      aggs: {
        results: {
          composite: { size: pageSize, sources },
          aggs
        }
      }
    },
    groupBySegments,
    metrics,
    orderBy,
    page,
    pageSize
  };
}

function _esAggType(agg) {
  switch (agg.toUpperCase()) {
    case 'SUM':            return 'sum';
    case 'AVG':            return 'avg';
    case 'MIN':            return 'min';
    case 'MAX':            return 'max';
    case 'COUNT':          return 'value_count';
    case 'COUNT_DISTINCT': return 'cardinality';
    default:               return 'sum';
  }
}

/**
 * Execute a BI query against ES and return normalized rows.
 * Returns: { data: [...rows], metadata: { totalRows, executionTimeMs } }
 */
async function query(instanceId, datasetName, queryParams) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);
  const t0    = Date.now();

  const plan  = buildEsQuery(instanceId, queryParams);

  const result = await es.search({ index: alias, body: plan.esBody });
  const took   = Date.now() - t0;

  let rows = [];

  if (plan.noGroup) {
    // Single row of aggregated metrics
    const row = {};
    for (const m of plan.metrics) {
      const key = m.alias || m.metricName;
      row[key]  = result.aggregations?.[key]?.value ?? 0;
    }
    rows = [row];
  } else {
    const buckets = result.aggregations?.results?.buckets || [];
    rows = buckets.map(b => {
      const row = { ...b.key };
      for (const m of plan.metrics) {
        const key = m.alias || m.metricName;
        row[key]  = b[key]?.value ?? 0;
      }
      return row;
    });

    // Client-side sort (composite agg doesn't support arbitrary sort)
    if (plan.orderBy?.length) {
      for (const { field, direction } of [...plan.orderBy].reverse()) {
        const dir = direction?.toUpperCase() === 'DESC' ? -1 : 1;
        rows.sort((a, b) => {
          const av = a[field], bv = b[field];
          if (av == null) return 1;
          if (bv == null) return -1;
          return av < bv ? -dir : av > bv ? dir : 0;
        });
      }
    }
  }

  return { data: rows, metadata: { totalRows: rows.length, executionTimeMs: took } };
}

/**
 * Get distinct values for a segment (for filter dropdowns).
 */
async function segmentValues(instanceId, datasetName, segmentName, limit = 200) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);

  const result = await es.search({
    index: alias,
    body: {
      size: 0,
      query: { term: { __instance_id: instanceId } },
      aggs: {
        values: {
          terms: { field: segmentName, size: limit, order: { _key: 'asc' } }
        }
      }
    }
  });

  const buckets = result.aggregations?.values?.buckets || [];
  return buckets.map(b => ({ value: String(b.key), displayValue: String(b.key) }));
}

/**
 * Index health + stats for admin UI.
 */
async function indexStats(instanceId, datasetName) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);

  try {
    const stats = await es.indices.stats({ index: alias });
    const info  = stats._all?.total;
    return {
      docCount:   info?.docs?.count  ?? 0,
      sizeBytes:  info?.store?.size_in_bytes ?? 0,
      status:     'green'
    };
  } catch (e) {
    return { docCount: 0, sizeBytes: 0, status: 'error', error: e.message };
  }
}

/**
 * Fetch raw source documents (no aggregation) filtered by exact dimension values.
 * Used for drilldown-to-records from any aggregated report row.
 * @param {string} instanceId
 * @param {string} datasetName
 * @param {Array}  filters — same shape as BI query filters [{segmentName, operator, value}]
 * @param {number} size    — max docs to return (default 100)
 * @param {Array}  excludeFields — internal fields to strip from _source (e.g. __instance_id)
 * @returns { records: [...], total: N }
 */
async function rawQuery(instanceId, datasetName, filters = [], size = 100, excludeFields = ['__instance_id']) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);

  // Reuse the same filter-building logic as buildEsQuery
  const mustClauses = [{ term: { __instance_id: instanceId } }];
  for (const f of filters) {
    const { segmentName, operator, value } = f;
    switch (operator) {
      case 'eq':       mustClauses.push({ term:    { [segmentName]: value } }); break;
      case 'neq':      mustClauses.push({ bool:    { must_not: [{ term: { [segmentName]: value } }] } }); break;
      case 'in':       mustClauses.push({ terms:   { [segmentName]: Array.isArray(value) ? value : [value] } }); break;
      case 'not_in':   mustClauses.push({ bool:    { must_not: [{ terms: { [segmentName]: Array.isArray(value) ? value : [value] } }] } }); break;
      case 'gte':      mustClauses.push({ range:   { [segmentName]: { gte: value } } }); break;
      case 'lte':      mustClauses.push({ range:   { [segmentName]: { lte: value } } }); break;
      case 'lt':       mustClauses.push({ range:   { [segmentName]: { lt:  value } } }); break;
      case 'gt':       mustClauses.push({ range:   { [segmentName]: { gt:  value } } }); break;
      case 'contains': mustClauses.push({ wildcard: { [segmentName]: `*${value}*` } }); break;
      case 'starts':   mustClauses.push({ prefix:   { [segmentName]: value } }); break;
      case 'is_null':  mustClauses.push({ bool:    { must_not: [{ exists: { field: segmentName } }] } }); break;
      case 'not_null': mustClauses.push({ exists:  { field: segmentName } }); break;
    }
  }

  const result = await es.search({
    index: alias,
    body: {
      query: { bool: { must: mustClauses } },
      size,
      _source: { excludes: excludeFields },
      sort: [{ _score: 'desc' }]
    }
  });

  const hits   = result.hits?.hits || [];
  const total  = result.hits?.total?.value ?? hits.length;
  const records = hits.map(h => h._source);
  return { records, total };
}

/**
 * Ping ES — used for health check on startup.
 */
async function ping() {
  const es = getClient();
  await es.ping();
  return true;
}

module.exports = {
  getClient,
  indexName, aliasName,
  createIndex, indexExists, swapAlias, deleteIndex,
  bulkIndex, replaceAll,
  query, segmentValues, indexStats, rawQuery,
  ping
};
