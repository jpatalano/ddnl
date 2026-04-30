/**
 * adviseEngine.js — Scheduled intelligence engine for the Advise module
 *
 * Architecture:
 *   1. At startup (and each run), loads the merged rule set:
 *      - Global templates from advise_rule_templates (seeded once)
 *      - Per-instance overrides/additions from advise_rules
 *      - Instance rules take precedence; instances can disable global rules
 *   2. For each active rule, dispatches to a handler keyed by rule_type
 *   3. Each handler receives (dataset, config) and returns raw findings
 *   4. LLM recommendations generated in parallel, results persisted
 *
 * Adding a new rule type:
 *   1. Add handler to RULE_HANDLERS map below
 *   2. Add entry to GLOBAL_TEMPLATES seed data
 *   3. Re-run seedGlobalTemplates() (called at boot)
 */

'use strict';

const { pool } = require('./db');
const es        = require('./esClient');
const llm       = require('./llmProvider');

// Max concurrent LLM recommendation calls per advise run.
// Balances throughput against provider rate limits.
const LLM_CONCURRENCY = parseInt(process.env.ADVISE_LLM_CONCURRENCY || '4', 10);

// ── Global template seed data ─────────────────────────────────────────────────
// These are upserted into advise_rule_templates at boot.
// Instances see all of these by default; they can disable or override via advise_rules.

const GLOBAL_TEMPLATES = [
  {
    rule_type:    'idle_days',
    label:        'Idle Equipment',
    description:  'Flags equipment units that have been idle beyond a threshold number of days.',
    category:     'equipment',
    dataset_hint: 'equipment',
    sort_order:   10,
    config_schema: {
      warn_days:     { type: 'number', label: 'Warn after (days)',     default: 14 },
      critical_days: { type: 'number', label: 'Critical after (days)', default: 30 },
      field_idle_flag:      { type: 'field', label: 'Idle flag field',          default: 'IdleFlag' },
      field_days_idle:      { type: 'field', label: 'Days since last use field', default: 'DaysSinceLastUse' },
      field_unit_code:      { type: 'field', label: 'Unit identifier field',    default: 'UnitCode' },
      field_yard:           { type: 'field', label: 'Yard field',               default: 'Yard' },
      field_unit_type:      { type: 'field', label: 'Unit type field',          default: 'UnitType' },
      field_current_status: { type: 'field', label: 'Current status field',     default: 'CurrentStatus' },
      field_book_value:     { type: 'field', label: 'Book value field',         default: 'BookValue' },
    },
    default_config: {
      warn_days: 14, critical_days: 30,
      field_idle_flag: 'IdleFlag', field_days_idle: 'DaysSinceLastUse',
      field_unit_code: 'UnitCode', field_yard: 'Yard',
      field_unit_type: 'UnitType', field_current_status: 'CurrentStatus',
      field_book_value: 'BookValue',
    }
  },
  {
    rule_type:    'compliance_expiry',
    label:        'Compliance Expiry',
    description:  'Flags equipment with inspection, insurance, or registration expiring soon.',
    category:     'equipment',
    dataset_hint: 'equipment',
    sort_order:   20,
    config_schema: {
      warn_days:     { type: 'number', label: 'Warn within (days)',     default: 30 },
      critical_days: { type: 'number', label: 'Critical within (days)', default: 7  },
      field_unit_code:      { type: 'field', label: 'Unit identifier field',     default: 'UnitCode' },
      field_yard:           { type: 'field', label: 'Yard field',                default: 'Yard' },
      field_unit_type:      { type: 'field', label: 'Unit type field',           default: 'UnitType' },
      field_days_inspection: { type: 'field', label: 'Days until inspection exp', default: 'DaysUntilInspectionExp' },
      field_days_insurance:  { type: 'field', label: 'Days until insurance exp',  default: 'DaysUntilInsuranceExp' },
      field_days_registration: { type: 'field', label: 'Days until registration exp', default: 'DaysUntilRegistrationExp' },
      field_date_inspection: { type: 'field', label: 'Inspection exp date field', default: 'AnnualInspectionExpDate' },
      field_date_insurance:  { type: 'field', label: 'Insurance exp date field',  default: 'InsuranceExpDate' },
      field_date_registration: { type: 'field', label: 'Registration exp date field', default: 'RegistrationExpDate' },
      field_pm_status:       { type: 'field', label: 'PM status field',           default: 'PMStatus' },
    },
    default_config: {
      warn_days: 30, critical_days: 7,
      field_unit_code: 'UnitCode', field_yard: 'Yard', field_unit_type: 'UnitType',
      field_days_inspection: 'DaysUntilInspectionExp', field_days_insurance: 'DaysUntilInsuranceExp',
      field_days_registration: 'DaysUntilRegistrationExp',
      field_date_inspection: 'AnnualInspectionExpDate', field_date_insurance: 'InsuranceExpDate',
      field_date_registration: 'RegistrationExpDate', field_pm_status: 'PMStatus',
    }
  },
  {
    rule_type:    'yard_revenue',
    label:        'Yard Revenue Underperformance',
    description:  'Compares revenue per yard against the peer average. Flags yards significantly below average.',
    category:     'yard',
    dataset_hint: 'equipment_daily',
    sort_order:   30,
    config_schema: {
      warn_pct:      { type: 'number', label: 'Warn below % of peer avg',     default: 70 },
      critical_pct:  { type: 'number', label: 'Critical below % of peer avg', default: 40 },
      min_doc_count: { type: 'number', label: 'Min records to evaluate yard', default: 10 },
      field_yard:    { type: 'field',  label: 'Yard field',                   default: 'Yard' },
      field_revenue: { type: 'field',  label: 'Revenue field',                default: 'Revenue' },
      field_billable_hours:  { type: 'field', label: 'Billable hours field',  default: 'BillableHours' },
      field_available_hours: { type: 'field', label: 'Available hours field', default: 'AvailableHours' },
    },
    default_config: {
      warn_pct: 70, critical_pct: 40, min_doc_count: 10,
      field_yard: 'Yard', field_revenue: 'Revenue',
      field_billable_hours: 'BillableHours', field_available_hours: 'AvailableHours',
    }
  },
  {
    rule_type:    'customer_concentration',
    label:        'Customer Revenue Concentration',
    description:  'Flags when a single customer accounts for more than a threshold percentage of total revenue.',
    category:     'customer',
    dataset_hint: 'job',
    sort_order:   40,
    config_schema: {
      warn_pct:        { type: 'number', label: 'Warn above % of total revenue',     default: 40 },
      critical_pct:    { type: 'number', label: 'Critical above % of total revenue', default: 55 },
      field_customer:  { type: 'field',  label: 'Customer name field',  default: 'CustomerName' },
      field_revenue:   { type: 'field',  label: 'Revenue field',        default: 'InvoicedNet' },
    },
    default_config: {
      warn_pct: 40, critical_pct: 55,
      field_customer: 'CustomerName', field_revenue: 'InvoicedNet',
    }
  },
  {
    rule_type:    'customer_churn',
    label:        'Customer Churn Signal',
    description:  'Flags customers with no closed job activity beyond a threshold number of days.',
    category:     'customer',
    dataset_hint: 'job',
    sort_order:   50,
    config_schema: {
      warn_days:     { type: 'number', label: 'Warn after (days)',     default: 90 },
      critical_days: { type: 'number', label: 'Critical after (days)', default: 180 },
      field_customer:    { type: 'field', label: 'Customer name field',     default: 'CustomerName' },
      field_closed_date: { type: 'field', label: 'Job closed date field',   default: 'ClosedDate' },
      field_revenue:     { type: 'field', label: 'Revenue field',           default: 'InvoicedNet' },
      field_job_code:    { type: 'field', label: 'Job code field',          default: 'JobCode' },
    },
    default_config: {
      warn_days: 90, critical_days: 180,
      field_customer: 'CustomerName', field_closed_date: 'ClosedDate',
      field_revenue: 'InvoicedNet', field_job_code: 'JobCode',
    }
  },
  {
    rule_type:    'past_due_invoice',
    label:        'Past-Due Invoices',
    description:  'Flags open invoices with outstanding balances past their due date.',
    category:     'financial',
    dataset_hint: 'invoice',
    sort_order:   60,
    config_schema: {
      warn_days:     { type: 'number', label: 'Warn after (days past due)',     default: 15 },
      critical_days: { type: 'number', label: 'Critical after (days past due)', default: 30 },
      field_days_past_due: { type: 'field', label: 'Days past due field',   default: 'DaysPastDue' },
      field_balance_due:   { type: 'field', label: 'Balance due field',     default: 'BalanceDue' },
      field_invoice_net:   { type: 'field', label: 'Invoice net field',     default: 'InvoiceNet' },
      field_customer:      { type: 'field', label: 'Customer name field',   default: 'CustomerName' },
      field_yard:          { type: 'field', label: 'Yard field',            default: 'Yard' },
      field_due_date:      { type: 'field', label: 'Due date field',        default: 'DueDate' },
      field_paid_flag:     { type: 'field', label: 'Paid flag field',       default: 'PaidFlag' },
      field_voided_flag:   { type: 'field', label: 'Voided flag field',     default: 'VoidedFlag' },
      field_invoice_number:{ type: 'field', label: 'Invoice number field',  default: 'InvoiceNumber' },
    },
    default_config: {
      warn_days: 15, critical_days: 30,
      field_days_past_due: 'DaysPastDue', field_balance_due: 'BalanceDue',
      field_invoice_net: 'InvoiceNet', field_customer: 'CustomerName',
      field_yard: 'Yard', field_due_date: 'DueDate',
      field_paid_flag: 'PaidFlag', field_voided_flag: 'VoidedFlag',
      field_invoice_number: 'InvoiceNumber',
    }
  },
  {
    rule_type:    'quote_expiring',
    label:        'Expiring Open Quotes',
    description:  'Flags open quotes approaching their expiration date.',
    category:     'financial',
    dataset_hint: 'quote',
    sort_order:   70,
    config_schema: {
      warn_days:     { type: 'number', label: 'Warn within (days)',     default: 7 },
      critical_days: { type: 'number', label: 'Critical within (days)', default: 2 },
      field_quote_number:  { type: 'field', label: 'Quote number field',       default: 'QuoteNumber' },
      field_customer:      { type: 'field', label: 'Customer field',           default: 'CustomerName' },
      field_sales_rep:     { type: 'field', label: 'Sales rep field',          default: 'SalesRep' },
      field_quoted_value:  { type: 'field', label: 'Quoted value field',       default: 'QuotedValue' },
      field_days_until_exp:{ type: 'field', label: 'Days until expiration field', default: 'DaysUntilExpiration' },
      field_exp_date:      { type: 'field', label: 'Expiration date field',    default: 'QuoteExpirationDate' },
      field_won_flag:      { type: 'field', label: 'Won flag field',           default: 'WonFlag' },
      field_lost_flag:     { type: 'field', label: 'Lost flag field',          default: 'LostFlag' },
      field_expired_flag:  { type: 'field', label: 'Expired flag field',       default: 'ExpiredFlag' },
      field_cancelled_flag:{ type: 'field', label: 'Cancelled flag field',     default: 'CancelledFlag' },
    },
    default_config: {
      warn_days: 7, critical_days: 2,
      field_quote_number: 'QuoteNumber', field_customer: 'CustomerName',
      field_sales_rep: 'SalesRep', field_quoted_value: 'QuotedValue',
      field_days_until_exp: 'DaysUntilExpiration', field_exp_date: 'QuoteExpirationDate',
      field_won_flag: 'WonFlag', field_lost_flag: 'LostFlag',
      field_expired_flag: 'ExpiredFlag', field_cancelled_flag: 'CancelledFlag',
    }
  },
  {
    rule_type:    'quote_win_rate',
    label:        'Quote Win Rate by Rep',
    description:  'Flags sales reps with a win rate below the defined threshold (min 5 closed quotes required).',
    category:     'financial',
    dataset_hint: 'quote',
    sort_order:   80,
    config_schema: {
      warn_pct:      { type: 'number', label: 'Warn below win rate %',     default: 30 },
      critical_pct:  { type: 'number', label: 'Critical below win rate %', default: 15 },
      min_quotes:    { type: 'number', label: 'Minimum closed quotes',     default: 5  },
      field_sales_rep:    { type: 'field', label: 'Sales rep field',    default: 'SalesRep' },
      field_quoted_value: { type: 'field', label: 'Quoted value field', default: 'QuotedValue' },
      field_quote_id:     { type: 'field', label: 'Quote ID field',     default: 'QuoteId' },
      field_won_flag:     { type: 'field', label: 'Won flag field',     default: 'WonFlag' },
      field_lost_flag:    { type: 'field', label: 'Lost flag field',    default: 'LostFlag' },
      field_expired_flag: { type: 'field', label: 'Expired flag field', default: 'ExpiredFlag' },
    },
    default_config: {
      warn_pct: 30, critical_pct: 15, min_quotes: 5,
      field_sales_rep: 'SalesRep', field_quoted_value: 'QuotedValue',
      field_quote_id: 'QuoteId', field_won_flag: 'WonFlag',
      field_lost_flag: 'LostFlag', field_expired_flag: 'ExpiredFlag',
    }
  },
];

// ── Seed global templates ──────────────────────────────────────────────────────
// Called once at boot. Upserts all GLOBAL_TEMPLATES into advise_rule_templates.
// Uses rule_type as the unique key — safe to re-run.

async function seedGlobalTemplates() {
  for (const t of GLOBAL_TEMPLATES) {
    await pool.query(`
      INSERT INTO advise_rule_templates
        (rule_type, label, description, category, dataset_hint,
         config_schema, default_config, is_active, sort_order)
      VALUES ($1,$2,$3,$4,$5,$6,$7,TRUE,$8)
      ON CONFLICT (rule_type) DO UPDATE SET
        label          = EXCLUDED.label,
        description    = EXCLUDED.description,
        category       = EXCLUDED.category,
        dataset_hint   = EXCLUDED.dataset_hint,
        config_schema  = EXCLUDED.config_schema,
        default_config = EXCLUDED.default_config,
        sort_order     = EXCLUDED.sort_order
    `, [
      t.rule_type, t.label, t.description, t.category, t.dataset_hint,
      JSON.stringify(t.config_schema), JSON.stringify(t.default_config), t.sort_order
    ]);
  }
  console.log(`[adviseEngine] ${GLOBAL_TEMPLATES.length} global templates seeded`);
}

// ── Load merged rule set for a client ─────────────────────────────────────────
// Returns the effective rules to run:
//   - All active global templates, using default_config
//   - Overridden by any matching advise_rules row for this client
//   - Plus any custom advise_rules rows (global_template_id IS NULL)
//   - Disabled rules (enabled=FALSE in advise_rules) are excluded

async function loadRulesForClient(clientId) {
  // 1. All active global templates
  const { rows: templates } = await pool.query(`
    SELECT id, rule_type, label, description, category, dataset_hint,
           default_config, config_schema, sort_order
    FROM advise_rule_templates
    WHERE is_active = TRUE
    ORDER BY sort_order
  `);

  // 2. All instance rules for this client
  const { rows: instanceRules } = await pool.query(`
    SELECT id, global_template_id, rule_type, label, description,
           category, dataset_hint, config, enabled
    FROM advise_rules
    WHERE client_id = $1
  `, [clientId]);

  const instanceByType = {};
  for (const r of instanceRules) instanceByType[r.rule_type] = r;

  const merged = [];

  // Apply global templates, respecting instance overrides/disables
  for (const t of templates) {
    const override = instanceByType[t.rule_type];
    if (override && !override.enabled) continue;  // instance disabled this rule

    const config = Object.assign({}, t.default_config, override?.config || {});
    merged.push({
      rule_type:    t.rule_type,
      label:        override?.label        || t.label,
      description:  override?.description  || t.description,
      category:     override?.category     || t.category,
      dataset_hint: override?.dataset_hint || t.dataset_hint,
      config,
      config_schema: t.config_schema,
      source:       'global',
    });
  }

  // Add custom instance-only rules (no global template)
  for (const r of instanceRules) {
    if (r.global_template_id !== null) continue;  // already handled above
    if (!r.enabled) continue;
    merged.push({
      rule_type:    r.rule_type,
      label:        r.label,
      description:  r.description,
      category:     r.category,
      dataset_hint: r.dataset_hint,
      config:       r.config || {},
      config_schema: {},
      source:       'custom',
    });
  }

  return merged;
}

// ── DB helpers ─────────────────────────────────────────────────────────────────

async function loadActiveClients() {
  const { rows } = await pool.query(`
    SELECT DISTINCT dd.client_id
    FROM dataset_definitions dd
    WHERE dd.visible_to_ai = TRUE AND dd.is_active = TRUE
  `);
  return rows.map(r => r.client_id);
}

async function loadClientDatasets(clientId) {
  const { rows } = await pool.query(`
    SELECT dd.name, dd.label, dd.es_alias, dsv.fields
    FROM dataset_definitions dd
    LEFT JOIN dataset_schema_versions dsv
      ON dsv.dataset_id = dd.id AND dsv.version = dd.current_version
    WHERE dd.client_id = $1
      AND dd.visible_to_ai = TRUE
      AND dd.is_active = TRUE
    ORDER BY dd.label
  `, [clientId]);
  return rows;
}

function findDataset(datasets, hint) {
  if (!hint) return null;
  return datasets.find(d => d.name === hint || d.name.startsWith(hint)) || null;
}

// ── ES helpers ─────────────────────────────────────────────────────────────────

async function esAgg(aliasName, query, aggs) {
  try {
    const resp = await es.getClient().search({
      index: aliasName, size: 0,
      query: query || { match_all: {} }, aggs
    });
    return resp.aggregations || {};
  } catch (err) {
    console.error(`[adviseEngine] ES agg error on ${aliasName}:`, err.message);
    return null;
  }
}

async function esSearch(aliasName, query, size = 100, _source = null) {
  try {
    const body = { index: aliasName, size, query: query || { match_all: {} } };
    if (_source) body._source = _source;
    const resp = await es.getClient().search(body);
    return resp.hits?.hits || [];
  } catch (err) {
    console.error(`[adviseEngine] ES search error on ${aliasName}:`, err.message);
    return [];
  }
}

// ── Rule handlers ──────────────────────────────────────────────────────────────
// Each handler: async (dataset, config) => Finding[]
// dataset: { name, label, es_alias, fields }
// config:  merged config object from template default + instance override

const RULE_HANDLERS = {

  idle_days: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;  // shorthand

    const hits = await esSearch(dataset.es_alias, {
      bool: {
        must_not: [{ term: { _status: 'deleted' } }],
        filter:   [{ term: { [f.field_idle_flag]: true } }]
      }
    }, 500, [f.field_unit_code, f.field_yard, f.field_unit_type,
              f.field_days_idle, f.field_current_status, f.field_book_value,
              'LastAssignmentEndDate']);

    for (const hit of hits) {
      const src      = hit._source;
      const idleDays = typeof src[f.field_days_idle] === 'number' ? src[f.field_days_idle] : null;
      if (idleDays === null || idleDays < f.warn_days) continue;

      findings.push({
        category:     'equipment',
        severity:     idleDays >= f.critical_days ? 'critical' : 'watch',
        metric_key:   'idle_days',
        entity_type:  'equipment',
        entity_id:    hit._id,
        entity_label: src[f.field_unit_code] || hit._id,
        data_json: {
          idle_days:          idleDays,
          yard:               src[f.field_yard] || 'Unknown',
          unit_type:          src[f.field_unit_type] || null,
          current_status:     src[f.field_current_status] || null,
          book_value:         src[f.field_book_value] || null,
          last_assignment:    src.LastAssignmentEndDate || null,
          benchmark_warn:     f.warn_days,
          benchmark_critical: f.critical_days,
        }
      });
    }
    findings.sort((a, b) => b.data_json.idle_days - a.data_json.idle_days);
    return findings.slice(0, 25);
  },

  compliance_expiry: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;

    const hits = await esSearch(dataset.es_alias, {
      bool: {
        must_not: [{ term: { _status: 'deleted' } }],
        should: [
          { range: { [f.field_days_inspection]:   { lte: f.warn_days } } },
          { range: { [f.field_days_insurance]:    { lte: f.warn_days } } },
          { range: { [f.field_days_registration]: { lte: f.warn_days } } },
        ],
        minimum_should_match: 1
      }
    }, 200, [f.field_unit_code, f.field_yard, f.field_unit_type,
              f.field_days_inspection, f.field_days_insurance, f.field_days_registration,
              f.field_date_inspection, f.field_date_insurance, f.field_date_registration,
              f.field_pm_status]);

    for (const hit of hits) {
      const src = hit._source;
      const checks = [
        { key: 'inspection',   days: src[f.field_days_inspection],   expDate: src[f.field_date_inspection] },
        { key: 'insurance',    days: src[f.field_days_insurance],     expDate: src[f.field_date_insurance] },
        { key: 'registration', days: src[f.field_days_registration],  expDate: src[f.field_date_registration] },
      ];
      for (const check of checks) {
        if (typeof check.days !== 'number' || check.days > f.warn_days) continue;
        const expiredLabel = check.days < 0
          ? `EXPIRED ${Math.abs(check.days)}d ago`
          : `expires in ${check.days}d`;
        findings.push({
          category:     'equipment',
          severity:     check.days <= f.critical_days ? 'critical' : 'watch',
          metric_key:   'compliance_expiry',
          entity_type:  'equipment',
          entity_id:    `${hit._id}_${check.key}`,
          entity_label: src[f.field_unit_code] || hit._id,
          data_json: {
            compliance_type:   check.key,
            days_until_expiry: check.days,
            expiry_date:       check.expDate || null,
            expiry_label:      expiredLabel,
            yard:              src[f.field_yard] || 'Unknown',
            unit_type:         src[f.field_unit_type] || null,
            pm_status:         src[f.field_pm_status] || null,
          }
        });
      }
    }
    findings.sort((a, b) => a.data_json.days_until_expiry - b.data_json.days_until_expiry);
    return findings.slice(0, 30);
  },

  yard_revenue: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;

    const aggs = await esAgg(dataset.es_alias,
      { bool: { must_not: [{ term: { _status: 'deleted' } }] } },
      {
        by_yard: {
          terms: { field: f.field_yard, size: 50, missing: 'Unknown' },
          aggs: {
            total_revenue:   { sum:         { field: f.field_revenue } },
            total_billable:  { sum:         { field: f.field_billable_hours } },
            total_available: { sum:         { field: f.field_available_hours } },
          }
        }
      }
    );
    if (!aggs?.by_yard?.buckets?.length) return findings;

    const buckets = aggs.by_yard.buckets.filter(b => b.doc_count >= f.min_doc_count);
    if (buckets.length < 2) return findings;

    const revenues = buckets.map(b => b.total_revenue?.value || 0);
    const avg      = revenues.reduce((a, b) => a + b, 0) / revenues.length;
    if (avg === 0) return findings;

    for (const b of buckets) {
      const rev     = b.total_revenue?.value || 0;
      const pctOfAvg = rev / avg;
      if (pctOfAvg * 100 >= f.warn_pct) continue;

      const billable  = b.total_billable?.value  || 0;
      const available = b.total_available?.value || 0;
      findings.push({
        category:     'yard',
        severity:     pctOfAvg * 100 < f.critical_pct ? 'critical' : 'watch',
        metric_key:   'yard_revenue',
        entity_type:  'yard',
        entity_id:    b.key,
        entity_label: b.key,
        data_json: {
          revenue:          Math.round(rev),
          peer_avg_revenue: Math.round(avg),
          pct_of_avg:       Math.round(pctOfAvg * 100),
          utilization_rate: available > 0 ? Math.round((billable / available) * 100) : null,
          doc_count:        b.doc_count,
        }
      });
    }
    return findings;
  },

  customer_concentration: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;

    const aggs = await esAgg(dataset.es_alias,
      { bool: { must_not: [{ term: { _status: 'deleted' } }] } },
      {
        total:       { sum: { field: f.field_revenue } },
        by_customer: {
          terms: { field: f.field_customer, size: 10, order: { rev: 'desc' } },
          aggs:  { rev: { sum: { field: f.field_revenue } } }
        }
      }
    );
    if (!aggs?.total?.value || !aggs?.by_customer?.buckets?.length) return findings;

    const total = aggs.total.value;
    for (const b of aggs.by_customer.buckets) {
      const rev = b.rev?.value || 0;
      const pct = (rev / total) * 100;
      if (pct < f.warn_pct) break;
      findings.push({
        category:     'customer',
        severity:     pct > f.critical_pct ? 'critical' : 'watch',
        metric_key:   'customer_concentration',
        entity_type:  'customer',
        entity_id:    b.key,
        entity_label: b.key,
        data_json: {
          revenue:        Math.round(rev),
          total_revenue:  Math.round(total),
          pct_of_total:   Math.round(pct),
          benchmark_pct:  f.warn_pct,
          job_count:      b.doc_count,
        }
      });
    }
    return findings;
  },

  customer_churn: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;

    const aggs = await esAgg(dataset.es_alias,
      { bool: { must_not: [{ term: { _status: 'deleted' } }], filter: [{ exists: { field: f.field_closed_date } }] } },
      {
        by_customer: {
          terms: { field: f.field_customer, size: 200 },
          aggs: {
            last_job:  { max:         { field: f.field_closed_date } },
            total_rev: { sum:         { field: f.field_revenue } },
            job_count: { value_count: { field: f.field_job_code } },
          }
        }
      }
    );
    if (!aggs?.by_customer?.buckets?.length) return findings;

    const now = Date.now();
    for (const b of aggs.by_customer.buckets) {
      const lastMs    = b.last_job?.value;
      if (!lastMs) continue;
      const daysSince = Math.floor((now - lastMs) / 86400000);
      if (daysSince < f.warn_days) continue;

      findings.push({
        category:     'customer',
        severity:     daysSince >= f.critical_days ? 'critical' : 'watch',
        metric_key:   'customer_churn',
        entity_type:  'customer',
        entity_id:    b.key,
        entity_label: b.key,
        data_json: {
          days_since_last_job: daysSince,
          last_closed_date:    new Date(lastMs).toISOString().slice(0, 10),
          lifetime_revenue:    Math.round(b.total_rev?.value || 0),
          job_count:           b.job_count?.value || 0,
          churn_threshold:     f.warn_days,
        }
      });
    }
    findings.sort((a, b) => b.data_json.days_since_last_job - a.data_json.days_since_last_job);
    return findings.slice(0, 20);
  },

  past_due_invoice: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;

    const hits = await esSearch(dataset.es_alias, {
      bool: {
        must_not: [
          { term: { _status: 'deleted' } },
          { term: { [f.field_paid_flag]:   true } },
          { term: { [f.field_voided_flag]: true } },
        ],
        filter: [
          { range: { [f.field_days_past_due]: { gte: f.warn_days } } },
          { range: { [f.field_balance_due]:   { gt:  0 } } },
        ]
      }
    }, 200, [f.field_invoice_number, f.field_customer, f.field_yard,
              f.field_invoice_net, f.field_balance_due, f.field_days_past_due,
              f.field_due_date, 'JobCode', 'CustomerTier']);

    for (const hit of hits) {
      const src = hit._source;
      const dpd = typeof src[f.field_days_past_due] === 'number' ? src[f.field_days_past_due] : 0;
      findings.push({
        category:     'financial',
        severity:     dpd >= f.critical_days ? 'critical' : 'watch',
        metric_key:   'past_due_invoice',
        entity_type:  'invoice',
        entity_id:    hit._id,
        entity_label: src[f.field_invoice_number] || hit._id,
        data_json: {
          days_past_due:  dpd,
          balance_due:    src[f.field_balance_due],
          invoice_net:    src[f.field_invoice_net],
          customer:       src[f.field_customer] || 'Unknown',
          customer_tier:  src.CustomerTier || null,
          yard:           src[f.field_yard] || 'Unknown',
          due_date:       src[f.field_due_date] || null,
          job_code:       src.JobCode || null,
        }
      });
    }
    findings.sort((a, b) => b.data_json.days_past_due - a.data_json.days_past_due);
    return findings.slice(0, 25);
  },

  quote_expiring: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;

    const hits = await esSearch(dataset.es_alias, {
      bool: {
        must_not: [
          { term: { _status: 'deleted' } },
          { term: { [f.field_won_flag]:       true } },
          { term: { [f.field_lost_flag]:      true } },
          { term: { [f.field_expired_flag]:   true } },
          { term: { [f.field_cancelled_flag]: true } },
        ],
        filter: [{ range: { [f.field_days_until_exp]: { gte: 0, lte: f.warn_days } } }]
      }
    }, 50, [f.field_quote_number, f.field_customer, f.field_sales_rep,
             f.field_quoted_value, f.field_days_until_exp, f.field_exp_date, 'BillingType']);

    for (const hit of hits) {
      const src = hit._source;
      const daysLeft = src[f.field_days_until_exp] ?? f.warn_days;
      findings.push({
        category:     'financial',
        severity:     daysLeft <= f.critical_days ? 'critical' : 'watch',
        metric_key:   'quote_expiring',
        entity_type:  'quote',
        entity_id:    hit._id,
        entity_label: src[f.field_quote_number] || hit._id,
        data_json: {
          days_until_expiration: daysLeft,
          expiration_date:       src[f.field_exp_date] || null,
          customer:              src[f.field_customer] || 'Unknown',
          sales_rep:             src[f.field_sales_rep] || null,
          quoted_value:          src[f.field_quoted_value] || null,
          billing_type:          src.BillingType || null,
        }
      });
    }
    return findings;
  },

  quote_win_rate: async (dataset, cfg) => {
    const findings = [];
    if (!dataset?.es_alias) return findings;
    const f = cfg;

    const aggs = await esAgg(dataset.es_alias,
      {
        bool: {
          must_not: [{ term: { _status: 'deleted' } }],
          should: [
            { term: { [f.field_won_flag]:     true } },
            { term: { [f.field_lost_flag]:    true } },
            { term: { [f.field_expired_flag]: true } },
          ],
          minimum_should_match: 1
        }
      },
      {
        by_rep: {
          terms: { field: f.field_sales_rep, size: 50 },
          aggs: {
            won:   { filter:      { term: { [f.field_won_flag]: true } } },
            total: { value_count: { field: f.field_quote_id } },
            value: { sum:         { field: f.field_quoted_value } },
          }
        }
      }
    );
    if (!aggs?.by_rep?.buckets?.length) return findings;

    for (const b of aggs.by_rep.buckets) {
      const totalCount = b.total?.value || 0;
      if (totalCount < f.min_quotes) continue;
      const wonCount = b.won?.doc_count || 0;
      const winRate  = wonCount / totalCount;
      if (winRate * 100 >= f.warn_pct) continue;

      findings.push({
        category:     'financial',
        severity:     winRate * 100 < f.critical_pct ? 'critical' : 'watch',
        metric_key:   'quote_win_rate',
        entity_type:  'sales_rep',
        entity_id:    b.key,
        entity_label: b.key,
        data_json: {
          win_rate_pct:   Math.round(winRate * 100),
          won_count:      wonCount,
          total_quotes:   totalCount,
          pipeline_value: Math.round(b.value?.value || 0),
          benchmark_pct:  f.warn_pct,
        }
      });
    }
    return findings;
  },

};

// ── LLM recommendation builder ─────────────────────────────────────────────────

const FINDING_PROMPTS = {
  idle_days: (f) =>
    `Equipment "${f.entity_label}" (${f.data_json.unit_type || 'unknown type'}) at ${f.data_json.yard} has been idle ${f.data_json.idle_days} days. Status: ${f.data_json.current_status || 'unknown'}. Book value: ${f.data_json.book_value ? '$' + Number(f.data_json.book_value).toLocaleString() : 'unknown'}. Thresholds: warn=${f.data_json.benchmark_warn}d, critical=${f.data_json.benchmark_critical}d.`,
  compliance_expiry: (f) =>
    `Equipment "${f.entity_label}" at ${f.data_json.yard}: ${f.data_json.compliance_type} ${f.data_json.expiry_label}${f.data_json.expiry_date ? ' (' + f.data_json.expiry_date + ')' : ''}. PM status: ${f.data_json.pm_status || 'unknown'}.`,
  yard_revenue: (f) =>
    `Yard "${f.entity_label}" is at ${f.data_json.pct_of_avg}% of peer avg revenue ($${f.data_json.revenue.toLocaleString()} vs avg $${f.data_json.peer_avg_revenue.toLocaleString()}).${f.data_json.utilization_rate !== null ? ` Utilization: ${f.data_json.utilization_rate}%.` : ''}`,
  customer_concentration: (f) =>
    `Customer "${f.entity_label}" is ${f.data_json.pct_of_total}% of total revenue ($${f.data_json.revenue.toLocaleString()} of $${f.data_json.total_revenue.toLocaleString()}, ${f.data_json.job_count} jobs). Threshold: ${f.data_json.benchmark_pct}%.`,
  customer_churn: (f) =>
    `Customer "${f.entity_label}" — no closed job in ${f.data_json.days_since_last_job} days (last: ${f.data_json.last_closed_date}). Lifetime revenue: $${f.data_json.lifetime_revenue.toLocaleString()} across ${f.data_json.job_count} jobs.`,
  past_due_invoice: (f) =>
    `Invoice "${f.entity_label}" for ${f.data_json.customer} (${f.data_json.yard}) is ${f.data_json.days_past_due} days past due. Balance: $${Number(f.data_json.balance_due).toLocaleString()} of $${Number(f.data_json.invoice_net).toLocaleString()}. Due: ${f.data_json.due_date || 'unknown'}.`,
  quote_expiring: (f) =>
    `Quote "${f.entity_label}" for ${f.data_json.customer} (rep: ${f.data_json.sales_rep || 'unassigned'}) expires in ${f.data_json.days_until_expiration}d${f.data_json.quoted_value ? '. Value: $' + Number(f.data_json.quoted_value).toLocaleString() : ''}.`,
  quote_win_rate: (f) =>
    `Rep "${f.entity_label}": ${f.data_json.win_rate_pct}% win rate (${f.data_json.won_count}/${f.data_json.total_quotes} closed). Pipeline value: $${f.data_json.pipeline_value.toLocaleString()}. Benchmark: ${f.data_json.benchmark_pct}%.`,
};

async function generateRecommendation(finding, role = 'owner', clientId = null) {
  const promptFn = FINDING_PROMPTS[finding.metric_key];
  if (!promptFn) return null;
  return llm.complete(llm.systemPromptForRole(role), promptFn(finding), clientId);
}

// ── Concurrency pool ────────────────────────────────────────────────────────────

async function runWithConcurrency(tasks, limit) {
  const results = [];
  let i = 0;
  async function runNext() {
    if (i >= tasks.length) return;
    const idx = i++;
    results[idx] = await tasks[idx]();
    await runNext();
  }
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, runNext));
  return results;
}

// ── Main run ───────────────────────────────────────────────────────────────────

async function runForClient(clientId, role = 'owner') {
  const startMs = Date.now();
  console.log(`[adviseEngine] Running for client=${clientId}`);

  const [datasets, rules] = await Promise.all([
    loadClientDatasets(clientId),
    loadRulesForClient(clientId),
  ]);

  if (!datasets.length) {
    console.log(`[adviseEngine] No visible_to_ai datasets for client=${clientId}`);
    return null;
  }

  console.log(`[adviseEngine] ${datasets.map(d=>d.name).join(', ')} | ${rules.length} rules active`);

  const allRawFindings = [];

  for (const rule of rules) {
    const handler = RULE_HANDLERS[rule.rule_type];
    if (!handler) {
      console.warn(`[adviseEngine] No handler for rule_type="${rule.rule_type}" — skipping`);
      continue;
    }
    const dataset = findDataset(datasets, rule.dataset_hint);
    try {
      const found = await handler(dataset, rule.config);
      console.log(`[adviseEngine]   ${rule.rule_type}: ${found.length} findings`);
      allRawFindings.push(...found);
    } catch (err) {
      console.error(`[adviseEngine] Rule "${rule.rule_type}" error:`, err.message);
    }
  }

  console.log(`[adviseEngine] ${allRawFindings.length} raw findings — generating recommendations`);

  // LLM recommendations in parallel
  const tasks = allRawFindings.map(f => async () => {
    try   { f.recommendation = await generateRecommendation(f, role, clientId); }
    catch (err) { console.error(`[adviseEngine] LLM ${f.metric_key}:`, err.message); f.recommendation = null; }
    return f;
  });
  await runWithConcurrency(tasks, LLM_CONCURRENCY);

  const durationMs = Date.now() - startMs;

  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const snap = await db.query(`
      INSERT INTO advise_snapshots (client_id, finding_count, duration_ms, status)
      VALUES ($1,$2,$3,'ok') RETURNING id
    `, [clientId, allRawFindings.length, durationMs]);

    const snapshotId = snap.rows[0].id;
    for (const f of allRawFindings) {
      await db.query(`
        INSERT INTO advise_findings
          (snapshot_id, client_id, category, severity, metric_key,
           entity_type, entity_id, entity_label, data_json, recommendation)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      `, [snapshotId, clientId, f.category, f.severity, f.metric_key,
          f.entity_type||null, f.entity_id||null, f.entity_label||null,
          JSON.stringify(f.data_json), f.recommendation||null]);
    }
    await db.query('COMMIT');
    console.log(`[adviseEngine] client=${clientId} — ${allRawFindings.length} findings in ${durationMs}ms`);
    return { snapshotId, findingCount: allRawFindings.length, durationMs };
  } catch (err) {
    await db.query('ROLLBACK');
    console.error(`[adviseEngine] DB write failed for client=${clientId}:`, err.message);
    throw err;
  } finally {
    db.release();
  }
}

async function runAll() {
  console.log('[adviseEngine] Starting full run across all clients');
  const clients = await loadActiveClients();
  for (const clientId of clients) {
    try   { await runForClient(clientId); }
    catch (err) { console.error(`[adviseEngine] Failed for client=${clientId}:`, err.message); }
  }
  console.log('[adviseEngine] Full run complete');
}

module.exports = { runAll, runForClient, loadActiveClients, seedGlobalTemplates, GLOBAL_TEMPLATES };
