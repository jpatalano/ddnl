#!/usr/bin/env node
/**
 * fcc-seed-q2c.js
 *
 * Registers all 10 Q2C dataset definitions in Postgres for the FCC instance.
 * Sets field metadata (segments vs metrics), PKs, and labels.
 * Safe to re-run — uses ON CONFLICT DO UPDATE / DO NOTHING.
 *
 * Run:
 *   DATABASE_URL="postgresql://postgres:FccDbPass2026!@shortline.proxy.rlwy.net:56142/railway" \
 *   node fcc-seed-q2c.js
 */

'use strict';

const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) { console.error('DATABASE_URL required'); process.exit(1); }

const pool = new Pool({ connectionString: DATABASE_URL, ssl: false });

const CLIENT_ID = 'fcc';

// Helper: CamelCase → "Camel Case"
function label(f) { return f.replace(/([A-Z])/g, ' $1').trim(); }

// ─── Dataset definitions ───────────────────────────────────────────────────
// Each entry: name, label, description, pk, segments[], metrics[]

const DATASETS = [

  // ── quote ──────────────────────────────────────────────────────────────
  {
    name: 'quote',
    label: 'Quotes',
    description: 'One row per quote (all statuses). Quote lifecycle, win rate, pricing analysis.',
    pk: ['PK'],
    segments: [
      'QuoteId','QuoteNumber','RevisionNumber','ParentQuoteId','WonJobCode',
      'InstanceId','TenantId',
      'QuoteStatus','BillingType','JobType','Source','LostReason',
      'SalesRep','ProductMix','ValueBand',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'QuotedValue','QuotedCostEstimate','QuotedMargin','LineCount',
      'DaysRequestedToSent','DaysSentToResponded','DaysSentToClosed','DaysUntilExpiration',
      'QuoteCount','SentFlag','WonFlag','LostFlag','ExpiredFlag','CancelledFlag',
    ],
    dates: [
      'QuoteRequestedDate','QuoteSentDate','QuoteRespondedDate',
      'QuoteExpirationDate','QuoteClosedDate','ModifiedDate',
    ],
  },

  // ── quote_line ─────────────────────────────────────────────────────────
  {
    name: 'quote_line',
    label: 'Quote Lines',
    description: 'One row per line item on a quote. Pricing detail and billing-type mix.',
    pk: ['PK'],
    segments: [
      'QuoteLineId','QuoteId','QuoteNumber',
      'InstanceId','TenantId',
      'QuoteStatus','BillingType','CustomerId','CustomerName','SalesRep','Yard','Region',
      'LineType','UnitClass','TonnageBucket','ItemCode','ItemDescription',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'Quantity','UnitPrice','ExtendedPrice','DiscountAmount','DiscountPct',
      'CostEstimate','MarginEstimate',
    ],
    dates: ['ModifiedDate'],
  },

  // ── job ────────────────────────────────────────────────────────────────
  {
    name: 'job',
    label: 'Jobs',
    description: 'One row per job — current state and lifetime rollups.',
    pk: ['PK'],
    segments: [
      'JobCode','JobName','WinningQuoteId','WinningQuoteNumber',
      'InstanceId','TenantId',
      'JobStatus','JobType','BillingType','ProjectManager','SalesRep',
      'Yard','Region','ValueBand',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'RiskFlags','IsAIAJob',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'QuotedValue','AwardedValue','EstimatedRevenue','EstimatedCost',
      'TicketedRevenue','InvoicedGross','CreditMemoAmount','InvoicedNet',
      'CollectedAmount','RetainageHeld','BadDebtAmount',
      'DirectLaborCost','DirectEquipmentCost','SubrentalCost','OtherDirectCost','TotalDirectCost',
      'GrossMargin','GrossMarginPct',
      'TicketCount','InvoiceCount','PaymentCount',
      'LaborHours','OvertimeHours','BillableHours',
      'DaysOpen','PctComplete','PctBilled','PctCompleteMinusBilled',
    ],
    dates: [
      'WonDate','JobCreatedDate','FirstScheduledDate','JobStartDate',
      'FirstTicketDate','WorkCompleteDate','FirstInvoiceDate','FinalInvoiceDate',
      'FirstPaymentDate','FinalPaymentDate','ClosedDate','ModifiedDate',
    ],
  },

  // ── job_daily ──────────────────────────────────────────────────────────
  {
    name: 'job_daily',
    label: 'Job Daily',
    description: 'One row per job × fiscal date. WIP trends, aging, daily revenue attribution.',
    pk: ['PK'],
    segments: [
      'JobCode','JobName','JobStatus','JobType','BillingType',
      'ProjectManager','SalesRep','Yard','Region','ValueBand','IsAIAJob',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'InstanceId','TenantId',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'DayFlag','OpenFlag','InProgressFlag','BillableDayFlag','DaysOpenAsOf',
      'TicketedRevenueDaily','InvoicedDaily','CollectedDaily',
      'LaborHoursDaily','EquipmentHoursDaily','WIPBalanceAsOf',
    ],
    dates: ['FiscalDate','ModifiedDate'],
  },

  // ── ticket ─────────────────────────────────────────────────────────────
  {
    name: 'ticket',
    label: 'Tickets',
    description: 'One row per e-ticket. Execution detail and cycle time.',
    pk: ['PK'],
    segments: [
      'TicketId','TicketNumber','JobCode','JobName','JobType','JobStatus','BillingType',
      'ProjectManager','SalesRep','Yard','Region',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'TicketType','TicketStatus','OperatorId','OperatorName','UnitCode','UnitClass',
      'InstanceId','TenantId',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'Hours','OvertimeHours','BillableAmount',
      'LaborCost','EquipmentCost','OtherCost','Margin',
      'DaysWorkCompleteToFinalize','DaysFinalizeToInvoice',
      'DaysToFinalize','DaysToInvoice',
      'TicketCount','FinalizedFlag','InvoicedFlag','OnTimeStartFlag',
    ],
    dates: [
      'TicketDate','WorkStartTime','WorkEndTime',
      'FinalizedDate','InvoicedDate','ModifiedDate',
    ],
  },

  // ── invoice ────────────────────────────────────────────────────────────
  {
    name: 'invoice',
    label: 'Invoices',
    description: 'One row per invoice. Billing events and invoice totals.',
    pk: ['PK'],
    segments: [
      'InvoiceId','InvoiceNumber','InvoiceType',
      'JobCode','JobName','JobType','JobStatus','BillingType',
      'ProjectManager','SalesRep','Yard','Region',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'InstanceId','TenantId',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'InvoiceGross','DiscountAmount','TaxAmount','RetainageHeld','InvoiceNet',
      'LineCount','AmountPaid','BalanceDue',
      'InvoiceCount','PaidFlag','VoidedFlag',
      'DaysIssuedToPaid','DaysPastDue',
    ],
    dates: ['InvoiceDate','DueDate','PaidDate','ModifiedDate'],
  },

  // ── invoice_line ───────────────────────────────────────────────────────
  {
    name: 'invoice_line',
    label: 'Invoice Lines',
    description: 'One row per invoice line. Revenue by billing type and line-level margin.',
    pk: ['PK'],
    segments: [
      'InvoiceLineId','InvoiceId','InvoiceNumber','InvoiceType',
      'JobCode','JobName','BillingType','LineType',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'SalesRep','Yard','Region',
      'ItemCode','ItemDescription',
      'InstanceId','TenantId',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'Quantity','UnitPrice','ExtendedAmount','LineCost','LineMargin',
      'RevenueRental','RevenueTM','RevenueAIA','RevenueOther',
    ],
    dates: ['InvoiceDate','ModifiedDate'],
  },

  // ── payment ────────────────────────────────────────────────────────────
  {
    name: 'payment',
    label: 'Payments',
    description: 'One row per payment received. Cash application and collection tracking.',
    pk: ['PK'],
    segments: [
      'PaymentId','InvoiceId','JobCode','JobName',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'PaymentMethod','PaymentType',
      'SalesRep','Yard','Region',
      'InstanceId','TenantId',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
    ],
    metrics: [
      'AppliedAmount','UnappliedAmount','TotalAmount','PaymentCount',
    ],
    dates: ['PaymentDate','ModifiedDate'],
  },

  // ── ar_snapshot ────────────────────────────────────────────────────────
  {
    name: 'ar_snapshot',
    label: 'AR Snapshot',
    description: 'Customer × snapshot date. AR aging over time — daily point-in-time state.',
    pk: ['PK'],
    segments: [
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'InstanceId','TenantId',
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
      'PastDueFlag',
    ],
    metrics: [
      'TotalAR','ARCurrent','AR31_60','AR61_90','ARover90',
      'RetainageHeld','CreditsOnAccount',
      'OldestInvoiceDaysPastDue','DSO','OpenInvoiceCount',
    ],
    dates: ['SnapshotDate','OldestInvoiceDate','ModifiedDate'],
  },

  // ── q2c_funnel ─────────────────────────────────────────────────────────
  {
    name: 'q2c_funnel',
    label: 'Q2C Funnel',
    description: 'One row per won job — wide denormalized timeline. The dashboard workhorse.',
    pk: ['PK'],
    segments: [
      'JobCode','JobName','WinningQuoteId','WinningQuoteNumber',
      'InstanceId','TenantId',
      'CustomerId','CustomerName','CustomerSegment','CustomerRegion','CustomerTier',
      'JobType','BillingType','ProjectManager','SalesRep','Yard','Region',
      'Source','QuoteRevisionCount','ProductMix','ValueBand',
      'IsAIAJob','RiskFlags',
      // Stage flags
      'HasLead','HasQuote','HasQuoteResponse','IsWon',
      'IsScheduled','IsInProgress','IsWorkComplete','IsBilled','IsCollected','IsClosed','IsCancelled',
      'OnTimeStartFlag','OverBudgetFlag','LowMarginFlag',
      // Primary fiscal (JobStartDate)
      'FiscalYear','FiscalQuarter','FiscalMonth','FiscalMonthLabel','FiscalWeek',
      'FiscalDayOfWeek','IsWeekend','IsHoliday',
      // Multi-anchor fiscal
      'QuoteSent_FiscalYear','QuoteSent_FiscalMonthLabel',
      'Won_FiscalYear','Won_FiscalMonthLabel',
      'Closed_FiscalYear','Closed_FiscalMonthLabel',
    ],
    metrics: [
      // Cycle times
      'DaysLeadToQuote','DaysQuoteToRespond','DaysQuoteToWon',
      'DaysWonToStart','DaysStartToComplete','DaysCompleteToInvoice',
      'DaysInvoiceToPaid','DaysTotalCycle','DaysOpenAsOf',
      // Amounts
      'QuotedValue','AwardedValue','DiscountFromQuote',
      'EstimatedRevenue','EstimatedCost','EstimatedMargin',
      'TicketedRevenue','InvoicedGross','CreditMemoAmount','InvoicedNet',
      'CollectedAmount','RetainageHeld','BadDebtAmount',
      'DirectLaborCost','DirectEquipmentCost','SubrentalCost','OtherDirectCost','TotalDirectCost',
      'GrossMargin','UnbilledWIP',
      'LaborHours','OvertimeHours','BillableHours',
      'TicketCount','InvoiceCount','PaymentCount',
      // Ratios (stored for display/filter convenience)
      'GrossMarginPct','OvertimePct','DiscountPct','EstimateAccuracyPct',
      'QuoteAccuracyPct','BillingLagPct',
      // AIA
      'PctComplete','PctBilled','PctCompleteMinusBilled',
    ],
    dates: [
      'LeadCreatedDate','QuoteRequestedDate','QuoteSentDate','QuoteRespondedDate',
      'WonDate','JobCreatedDate','FirstScheduledDate','JobStartDate',
      'FirstTicketDate','WorkCompleteDate','FirstInvoiceDate','FinalInvoiceDate',
      'FirstPaymentDate','FinalPaymentDate','ClosedDate','ModifiedDate',
    ],
  },

];

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const ds of DATASETS) {
      console.log(`\nRegistering: ${ds.name}`);

      // Upsert dataset definition
      const { rows: [def] } = await client.query(
        `INSERT INTO dataset_definitions
           (client_id, name, label, description, is_active, show_on_explorer, dataset_type)
         VALUES ($1, $2, $3, $4, true, true, 'client')
         ON CONFLICT (client_id, name) DO UPDATE
           SET label=EXCLUDED.label, description=EXCLUDED.description, updated_at=NOW()
         RETURNING id`,
        [CLIENT_ID, ds.name, ds.label, ds.description]
      );
      const dsId = def.id;

      // Set primary key fields
      await client.query(
        `UPDATE dataset_definitions SET primary_key_fields=$1 WHERE id=$2`,
        [JSON.stringify(ds.pk), dsId]
      );

      // Segment fields
      for (const f of ds.segments) {
        await client.query(
          `INSERT INTO dataset_field_metadata
             (dataset_id, field_name, label, field_type, format)
           VALUES ($1, $2, $3, 'segment', 'text')
           ON CONFLICT (dataset_id, field_name) DO NOTHING`,
          [dsId, f, label(f)]
        );
      }

      // Date fields — stored as segments (keyword/date), not aggregated as metrics
      for (const f of ds.dates) {
        await client.query(
          `INSERT INTO dataset_field_metadata
             (dataset_id, field_name, label, field_type, format)
           VALUES ($1, $2, $3, 'segment', 'date')
           ON CONFLICT (dataset_id, field_name) DO NOTHING`,
          [dsId, f, label(f)]
        );
      }

      // Metric fields
      for (const f of ds.metrics) {
        const fmt = f.endsWith('Pct') || f.endsWith('Rate') ? 'percent_1dp'
                  : f.includes('Days') || f === 'DaysOpen' || f === 'DaysOpenAsOf' ? 'days'
                  : f.includes('Hours') ? 'decimal_2dp'
                  : f.endsWith('Flag') || f.endsWith('Count') || f === 'RevisionNumber' ? 'integer'
                  : 'currency_usd_0dp';

        await client.query(
          `INSERT INTO dataset_field_metadata
             (dataset_id, field_name, label, field_type, format)
           VALUES ($1, $2, $3, 'metric', $4)
           ON CONFLICT (dataset_id, field_name) DO NOTHING`,
          [dsId, f, label(f), fmt]
        );
      }

      const segCount = ds.segments.length + ds.dates.length;
      console.log(`  id=${dsId}  segments=${segCount}  metrics=${ds.metrics.length}`);
    }

    await client.query('COMMIT');
    console.log('\n✓ Q2C seed complete.');
  } catch(e) {
    await client.query('ROLLBACK');
    console.error('SEED FAILED:', e.message);
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(e => { console.error(e); process.exit(1); });
