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

// ── _status system field ─────────────────────────────────────────────────────
//
// Every document carries a `_status` field: 'active' | 'deleted' | 'archived'.
// ALL read paths (queries, aggregations, segment values, raw drilldown) suppress
// non-active records by default at the ES level — callers never need to think
// about it. Ingest paths default to 'active' if the field is absent.
//
// To include deleted/archived records, pass includeDeleted:true to the
// relevant query helpers — used by admin/purge flows only.

const STATUS_FIELD    = '_status';
const STATUS_ACTIVE   = 'active';
const STATUS_DELETED  = 'deleted';
const STATUS_ARCHIVED = 'archived';

/**
 * Returns an ES filter clause that suppresses deleted + archived records.
 * Use inside a bool.filter or bool.must_not.
 * Returns a `bool.must` clause — drop it straight into mustClauses[].
 */
function activeOnlyClause() {
  // must_not: [{terms: {_status: ['deleted','archived']}}]
  // This also matches docs where _status is missing (pre-migration data) —
  // those are treated as active since they predate the field.
  return {
    bool: {
      must_not: [{ terms: { [STATUS_FIELD]: [STATUS_DELETED, STATUS_ARCHIVED] } }]
    }
  };
}

/**
 * Enrich a document before ingest — sets _status to 'active' if not provided.
 * Preserves explicit 'deleted' or 'archived' values from the payload.
 */
function applyStatusDefault(doc) {
  if (doc[STATUS_FIELD] === undefined || doc[STATUS_FIELD] === null || doc[STATUS_FIELD] === '') {
    doc[STATUS_FIELD] = STATUS_ACTIVE;
  }
  return doc;
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
  // Always include internal meta + system fields
  properties['__instance_id']    = { type: 'keyword' };
  properties['__ingested_at']    = { type: 'date' };
  properties['__ingest_version'] = { type: 'integer' };
  properties[STATUS_FIELD]       = { type: 'keyword' };  // 'active' | 'deleted' | 'archived'
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

// Default chunk size for bulk operations. Large payloads are split so no single
// ES bulk request exceeds ~10MB in practice. At ~500 bytes/doc average this is
// well within the default http.max_content_length of 100MB.
const BULK_CHUNK_SIZE = 500;

/**
 * Bulk index documents into the current alias.
 * docs: array of plain objects — we add __instance_id + __ingested_at.
 * idField: optional field name to use as ES _id (enables upsert semantics via 'index' action).
 *   When set, re-indexing the same idField value updates the doc in-place — no duplicates.
 *   When null/undefined, ES auto-generates _id (append-only, fine for immutable event logs).
 * Returns { indexed, failed, errors[] }
 */
async function bulkIndex(instanceId, datasetName, docs, idField = null) {
  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);
  const now   = new Date().toISOString();

  const body = docs.flatMap(doc => {
    const enriched = applyStatusDefault({ ...doc, __instance_id: instanceId, __ingested_at: now });
    const action   = idField && doc[idField] != null
      ? { index: { _index: alias, _id: String(doc[idField]) } }  // upsert by id
      : { index: { _index: alias } };                             // auto-id (append)
    return [action, enriched];
  });

  const result = await es.bulk({ refresh: true, body });

  const failed = (result.items || []).filter(i => i.index?.error);
  return {
    indexed: docs.length - failed.length,
    failed:  failed.length,
    errors:  failed.slice(0, 10).map(i => i.index.error)
  };
}

/**
 * Production-grade bulk upsert — designed for high-volume ingest where the same
 * record may arrive multiple times (nightly CSV re-sends, webhook retries, etc.).
 *
 * Differences from bulkIndex:
 *  - idField is REQUIRED — upsert without an id field is meaningless for dedup.
 *  - Uses ES 'update' action with doc_as_upsert:true so __ingest_version is
 *    incremented atomically via a Painless script on every write.
 *  - Splits large payloads into chunks (BULK_CHUNK_SIZE docs each) so a single
 *    250K-row nightly CSV doesn't blow the ES request size limit.
 *  - Returns a richer result with per-chunk timing for structured logging.
 *
 * Returns: { indexed, failed, errors[], chunks, durationMs }
 */
async function bulkUpsert(instanceId, datasetName, docs, idField) {
  if (!idField) throw new Error('bulkUpsert requires idField — use bulkIndex for append-only ingest');

  const es    = getClient();
  const alias = aliasName(instanceId, datasetName);
  const now   = new Date().toISOString();
  const t0    = Date.now();

  let totalIndexed = 0;
  let totalFailed  = 0;
  const allErrors  = [];
  let chunkCount   = 0;

  // Process in chunks to bound request size
  for (let i = 0; i < docs.length; i += BULK_CHUNK_SIZE) {
    const chunk = docs.slice(i, i + BULK_CHUNK_SIZE);
    chunkCount++;

    const body = chunk.flatMap(doc => {
      const docId = doc[idField];
      if (docId == null) {
        // Fallback: append-only for docs missing the id field
        const enriched = {
          ...doc,
          __instance_id:    instanceId,
          __ingested_at:    now,
          __ingest_version: 1
        };
        return [{ index: { _index: alias } }, enriched];
      }

      // Scripted update: set all fields, increment __ingest_version
      // If doc doesn't exist yet (upsert), create with version=1
      const fields = applyStatusDefault({
        ...doc,
        __instance_id: instanceId,
        __ingested_at: now
      });

      return [
        { update: { _index: alias, _id: String(docId) } },
        {
          script: {
            source: [
              'ctx._source.putAll(params.fields);',
              'ctx._source.__ingest_version = (ctx._source.__ingest_version != null',
              '  ? (int)ctx._source.__ingest_version + 1 : 1);'
            ].join(' '),
            lang:   'painless',
            params: { fields }
          },
          upsert: { ...fields, __ingest_version: 1 }
        }
      ];
    });

    const result = await es.bulk({ refresh: false, body });  // refresh:false for throughput

    const failed = (result.items || []).filter(i => (i.update || i.index)?.error);
    totalIndexed += chunk.length - failed.length;
    totalFailed  += failed.length;
    if (failed.length) {
      allErrors.push(...failed.slice(0, 5).map(i => (i.update || i.index).error));
    }
  }

  // One final refresh so queries see the new data immediately
  if (totalIndexed > 0) {
    try { await es.indices.refresh({ index: alias }); } catch (_) { /* non-fatal */ }
  }

  return {
    indexed:    totalIndexed,
    failed:     totalFailed,
    errors:     allErrors.slice(0, 20),
    chunks:     chunkCount,
    durationMs: Date.now() - t0
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
  // activeOnlyClause() is always prepended — suppresses deleted/archived at ES level.
  const mustClauses = [
    { term: { __instance_id: instanceId } },
    activeOnlyClause()
  ];

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

  // ── No groupBy + no metrics → raw record scan ──
  if (groupBySegments.length === 0 && metrics.length === 0) {
    return { rawScan: true, esBody: { query: esQuery, size: pageSize || 100, from: (page - 1) * (pageSize || 100) || 0 }, metrics };
  }

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

  if (plan.rawScan) {
    // Raw record scan — return source docs directly, strip internal fields
    rows = (result.hits?.hits || []).map(h => {
      const src = { ...h._source };
      delete src.__instance_id;
      delete src.__ingested_at;
      delete src.__ingest_version;
      delete src._status;
      return src;
    });
  } else if (plan.noGroup) {
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
      query: { bool: { must: [{ term: { __instance_id: instanceId } }, activeOnlyClause()] } },
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

  // Reuse the same filter-building logic as buildEsQuery — activeOnlyClause always applied.
  const mustClauses = [
    { term: { __instance_id: instanceId } },
    activeOnlyClause()
  ];
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
  bulkIndex, bulkUpsert, replaceAll,
  query, segmentValues, indexStats, rawQuery,
  ping,
  BULK_CHUNK_SIZE,
  // _status helpers — exported for ingest paths + admin use
  STATUS_FIELD, STATUS_ACTIVE, STATUS_DELETED, STATUS_ARCHIVED,
  activeOnlyClause, applyStatusDefault,
};
