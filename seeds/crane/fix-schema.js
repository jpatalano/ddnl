#!/usr/bin/env node
/**
 * fcc-fix-schema.js
 * 1. Fix bad labels in dataset_field_metadata (acronym splitter produced "Y T D", "P M", etc.)
 * 2. Seed dataset_schema_versions so /api/bi/datasets returns correct segment/metric counts
 *    and /api/bi/query can resolve field types
 */

const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) { console.error('DATABASE_URL required'); process.exit(1); }

const pool = new Pool({ connectionString: DATABASE_URL, ssl: false });

// ─── Label overrides — fix anything the naive camelCase splitter got wrong ──
// Format: { FieldName: 'Correct Label' }
const LABEL_OVERRIDES = {
  // equipment
  HoursSinceLastPM:         'Hours Since Last PM',
  PMStatus:                 'PM Status',
  YTDAvailableHours:        'YTD Available Hours',
  YTDBillableHours:         'YTD Billable Hours',
  YTDMaintenanceCost:       'YTD Maintenance Cost',
  YTDRevenue:               'YTD Revenue',
  InstanceId:               'Instance ID',
  TenantId:                 'Tenant ID',
  // equipment_daily
  PMsCompletedLate:         'PMs Completed Late',
  PMsCompletedOnTime:       'PMs Completed On Time',
  PMsScheduled:             'PMs Scheduled',
  RevenueAIA:               'Revenue AIA',
  RevenueTM:                'Revenue TM',
};

// ─── Full field schema for dataset_schema_versions ───────────────────────────
// fieldType: 'segment' | 'metric' | 'identifier' | 'temporal' | 'display'
// aggregationType: SUM | AVG | MIN | MAX | COUNT_DISTINCT
// displayFormat: number | currency | percent | text | date

const EQUIPMENT_FIELDS = [
  // Identifiers
  { name: 'PK',                       fieldType: 'identifier', label: 'PK' },
  { name: 'UnitCode',                 fieldType: 'identifier', label: 'Unit Code' },
  { name: 'InstanceId',               fieldType: 'identifier', label: 'Instance ID' },
  { name: 'TenantId',                 fieldType: 'identifier', label: 'Tenant ID' },
  { name: 'ModifiedDate',             fieldType: 'temporal',   label: 'Modified Date' },

  // Segments — unit attributes
  { name: 'UnitType',                 fieldType: 'segment',    label: 'Unit Type' },
  { name: 'UnitClass',                fieldType: 'segment',    label: 'Unit Class' },
  { name: 'TonnageBucket',            fieldType: 'segment',    label: 'Tonnage Bucket' },
  { name: 'Make',                     fieldType: 'segment',    label: 'Make' },
  { name: 'Model',                    fieldType: 'segment',    label: 'Model' },
  { name: 'SerialNumber',             fieldType: 'display',    label: 'Serial Number' },
  { name: 'Company',                  fieldType: 'segment',    label: 'Company' },
  { name: 'Yard',                     fieldType: 'segment',    label: 'Yard' },
  { name: 'Region',                   fieldType: 'segment',    label: 'Region' },
  { name: 'OwnershipType',            fieldType: 'segment',    label: 'Ownership Type' },
  { name: 'AgeBucket',                fieldType: 'segment',    label: 'Age Bucket' },
  { name: 'Description',              fieldType: 'display',    label: 'Description' },

  // Current state
  { name: 'CurrentStatus',            fieldType: 'segment',    label: 'Current Status' },
  { name: 'CurrentJobCode',           fieldType: 'display',    label: 'Current Job Code' },
  { name: 'CurrentJobCustomer',       fieldType: 'display',    label: 'Current Job Customer' },
  { name: 'CurrentYard',              fieldType: 'segment',    label: 'Current Yard' },
  { name: 'LastAssignmentStartDate',  fieldType: 'temporal',   label: 'Last Assignment Start' },
  { name: 'LastAssignmentEndDate',    fieldType: 'temporal',   label: 'Last Assignment End' },
  { name: 'IdleFlag',                 fieldType: 'segment',    label: 'Idle Flag' },

  // Telemetry
  { name: 'LastTelemetryDate',        fieldType: 'temporal',   label: 'Last Telemetry Date' },
  { name: 'PMStatus',                 fieldType: 'segment',    label: 'PM Status' },

  // Compliance
  { name: 'AnnualInspectionExpDate',  fieldType: 'temporal',   label: 'Inspection Exp Date' },
  { name: 'InsurancePolicy',          fieldType: 'display',    label: 'Insurance Policy' },
  { name: 'InsuranceExpDate',         fieldType: 'temporal',   label: 'Insurance Exp Date' },
  { name: 'RegistrationExpDate',      fieldType: 'temporal',   label: 'Registration Exp Date' },
  { name: 'ComplianceStatus',         fieldType: 'segment',    label: 'Compliance Status' },
  { name: 'PaybackStatus',            fieldType: 'segment',    label: 'Payback Status' },

  // Acquisition / lifecycle
  { name: 'AcquisitionDate',          fieldType: 'temporal',   label: 'Acquisition Date' },
  { name: 'DisposalDate',             fieldType: 'temporal',   label: 'Disposal Date' },

  // Fiscal (current period labels)
  { name: 'CurrentFiscalYear',        fieldType: 'segment',    label: 'Current Fiscal Year' },
  { name: 'CurrentFiscalQuarter',     fieldType: 'segment',    label: 'Current Fiscal Quarter' },
  { name: 'CurrentFiscalMonth',       fieldType: 'segment',    label: 'Current Fiscal Month' },
  { name: 'CurrentFiscalMonthLabel',  fieldType: 'segment',    label: 'Current Fiscal Month Label' },

  // Metrics
  { name: 'Tonnage',                  fieldType: 'metric', label: 'Tonnage',                      aggregationType: 'AVG',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'AcquisitionYear',          fieldType: 'metric', label: 'Acquisition Year',              aggregationType: 'MIN',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'AgeYears',                 fieldType: 'metric', label: 'Age Years',                    aggregationType: 'AVG',          displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'DaysSinceLastUse',         fieldType: 'metric', label: 'Days Since Last Use',           aggregationType: 'AVG',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'EngineHoursLatest',        fieldType: 'metric', label: 'Engine Hours (Latest)',         aggregationType: 'MAX',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'OdometerLatest',           fieldType: 'metric', label: 'Odometer (Latest)',             aggregationType: 'MAX',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'HoursSinceLastPM',         fieldType: 'metric', label: 'Hours Since Last PM',          aggregationType: 'AVG',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'DaysUntilInspectionExp',   fieldType: 'metric', label: 'Days Until Inspection Exp',    aggregationType: 'MIN',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'DaysUntilInsuranceExp',    fieldType: 'metric', label: 'Days Until Insurance Exp',     aggregationType: 'MIN',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'DaysUntilRegistrationExp', fieldType: 'metric', label: 'Days Until Registration Exp',  aggregationType: 'MIN',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'ComplianceExpiringCount',  fieldType: 'metric', label: 'Compliance Expiring Count',    aggregationType: 'SUM',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'AcquisitionCost',          fieldType: 'metric', label: 'Acquisition Cost',             aggregationType: 'SUM',          displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'BookValue',                fieldType: 'metric', label: 'Book Value',                   aggregationType: 'SUM',          displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'DisposalValue',            fieldType: 'metric', label: 'Disposal Value',               aggregationType: 'SUM',          displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'LifetimeRevenue',          fieldType: 'metric', label: 'Lifetime Revenue',             aggregationType: 'SUM',          displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'LifetimeEngineHours',      fieldType: 'metric', label: 'Lifetime Engine Hours',        aggregationType: 'SUM',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'LifetimeBillableHours',    fieldType: 'metric', label: 'Lifetime Billable Hours',      aggregationType: 'SUM',          displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'LifetimeAssignedDays',     fieldType: 'metric', label: 'Lifetime Assigned Days',       aggregationType: 'SUM',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'LifetimeMaintenanceCost',  fieldType: 'metric', label: 'Lifetime Maintenance Cost',    aggregationType: 'SUM',          displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'LifetimeFailures',         fieldType: 'metric', label: 'Lifetime Failures',            aggregationType: 'SUM',          displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'YTDRevenue',               fieldType: 'metric', label: 'YTD Revenue',                  aggregationType: 'SUM',          displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'YTDBillableHours',         fieldType: 'metric', label: 'YTD Billable Hours',           aggregationType: 'SUM',          displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'YTDAvailableHours',        fieldType: 'metric', label: 'YTD Available Hours',          aggregationType: 'SUM',          displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'YTDMaintenanceCost',       fieldType: 'metric', label: 'YTD Maintenance Cost',         aggregationType: 'SUM',          displayFormat: 'currency', decimalPlaces: 0 },
];

const EQUIPMENT_DAILY_FIELDS = [
  // Identifiers
  { name: 'PK',                        fieldType: 'identifier', label: 'PK' },
  { name: 'UnitCode',                  fieldType: 'identifier', label: 'Unit Code' },
  { name: 'FiscalDate',                fieldType: 'temporal',   label: 'Fiscal Date' },
  { name: 'InstanceId',                fieldType: 'identifier', label: 'Instance ID' },
  { name: 'TenantId',                  fieldType: 'identifier', label: 'Tenant ID' },
  { name: 'ModifiedDate',              fieldType: 'temporal',   label: 'Modified Date' },

  // Segments — unit attributes (denormalized)
  { name: 'UnitType',                  fieldType: 'segment', label: 'Unit Type' },
  { name: 'UnitClass',                 fieldType: 'segment', label: 'Unit Class' },
  { name: 'TonnageBucket',             fieldType: 'segment', label: 'Tonnage Bucket' },
  { name: 'Make',                      fieldType: 'segment', label: 'Make' },
  { name: 'Model',                     fieldType: 'segment', label: 'Model' },
  { name: 'Company',                   fieldType: 'segment', label: 'Company' },
  { name: 'Yard',                      fieldType: 'segment', label: 'Yard' },
  { name: 'Region',                    fieldType: 'segment', label: 'Region' },
  { name: 'OwnershipType',             fieldType: 'segment', label: 'Ownership Type' },
  { name: 'AgeBucket',                 fieldType: 'segment', label: 'Age Bucket' },
  { name: 'AcquisitionYear',           fieldType: 'segment', label: 'Acquisition Year' },

  // Segments — fiscal
  { name: 'FiscalYear',                fieldType: 'segment', label: 'Fiscal Year' },
  { name: 'FiscalQuarter',             fieldType: 'segment', label: 'Fiscal Quarter' },
  { name: 'FiscalMonth',               fieldType: 'segment', label: 'Fiscal Month' },
  { name: 'FiscalMonthLabel',          fieldType: 'segment', label: 'Fiscal Month Label' },
  { name: 'FiscalWeek',                fieldType: 'segment', label: 'Fiscal Week' },
  { name: 'FiscalPeriod',              fieldType: 'segment', label: 'Fiscal Period' },
  { name: 'FiscalDayOfWeek',           fieldType: 'segment', label: 'Day of Week' },
  { name: 'FiscalDayOfMonth',          fieldType: 'segment', label: 'Day of Month' },
  { name: 'CalendarYear',              fieldType: 'segment', label: 'Calendar Year' },
  { name: 'CalendarMonth',             fieldType: 'segment', label: 'Calendar Month' },
  { name: 'IsWeekend',                 fieldType: 'segment', label: 'Is Weekend' },
  { name: 'IsHoliday',                 fieldType: 'segment', label: 'Is Holiday' },

  // Segments — daily activity
  { name: 'PrimaryJobCode',            fieldType: 'segment', label: 'Job Code' },
  { name: 'PrimaryJobCustomer',        fieldType: 'segment', label: 'Customer' },
  { name: 'PrimaryStatus',             fieldType: 'segment', label: 'Daily Status' },
  { name: 'BillingType',               fieldType: 'segment', label: 'Billing Type' },

  // Metrics — availability & utilization
  { name: 'DayFlag',                   fieldType: 'metric', label: 'Day Flag',                    aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'AvailableHours',            fieldType: 'metric', label: 'Available Hours',             aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'TargetHours',               fieldType: 'metric', label: 'Target Hours',                aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'BillableHours',             fieldType: 'metric', label: 'Billable Hours',              aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'RevenueHours',              fieldType: 'metric', label: 'Revenue Hours',               aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'AssignedFlag',              fieldType: 'metric', label: 'Assigned Days',               aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'IdleFlag',                  fieldType: 'metric', label: 'Idle Days',                   aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'TransferFlag',              fieldType: 'metric', label: 'Transfer Days',               aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },

  // Metrics — downtime
  { name: 'DowntimeFlag',              fieldType: 'metric', label: 'Downtime Days',               aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'DowntimeHours',             fieldType: 'metric', label: 'Downtime Hours',              aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'ScheduledDowntimeHours',    fieldType: 'metric', label: 'Scheduled Downtime Hrs',      aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'UnscheduledDowntimeHours',  fieldType: 'metric', label: 'Unscheduled Downtime Hrs',    aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },

  // Metrics — revenue
  { name: 'Revenue',                   fieldType: 'metric', label: 'Revenue',                     aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'TargetRevenue',             fieldType: 'metric', label: 'Target Revenue',              aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'RevenueRental',             fieldType: 'metric', label: 'Revenue (Rental)',            aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'RevenueTM',                 fieldType: 'metric', label: 'Revenue (T&M)',               aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'RevenueAIA',                fieldType: 'metric', label: 'Revenue (AIA)',               aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'RevenueOther',              fieldType: 'metric', label: 'Revenue (Other)',             aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },

  // Metrics — maintenance
  { name: 'MaintenanceCost',           fieldType: 'metric', label: 'Maintenance Cost',            aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'PartsCost',                 fieldType: 'metric', label: 'Parts Cost',                  aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'LaborCost',                 fieldType: 'metric', label: 'Labor Cost',                  aggregationType: 'SUM', displayFormat: 'currency', decimalPlaces: 0 },
  { name: 'PMsScheduled',              fieldType: 'metric', label: 'PMs Scheduled',               aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'PMsCompletedOnTime',        fieldType: 'metric', label: 'PMs On Time',                 aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'PMsCompletedLate',          fieldType: 'metric', label: 'PMs Late',                    aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'UnscheduledRepairs',        fieldType: 'metric', label: 'Unscheduled Repairs',         aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'FailureEvents',             fieldType: 'metric', label: 'Failure Events',              aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'WorkOrdersOpened',          fieldType: 'metric', label: 'Work Orders Opened',          aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'WorkOrdersClosed',          fieldType: 'metric', label: 'Work Orders Closed',          aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },

  // Metrics — telemetry
  { name: 'EngineHoursDelta',          fieldType: 'metric', label: 'Engine Hours',                aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },
  { name: 'OdometerMilesDelta',        fieldType: 'metric', label: 'Odometer Miles',              aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'FuelGallons',               fieldType: 'metric', label: 'Fuel (Gallons)',              aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 1 },

  // Metrics — fleet sizing
  { name: 'ActiveFlag',                fieldType: 'metric', label: 'Active Days',                 aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'InServiceFlag',             fieldType: 'metric', label: 'In-Service Days',             aggregationType: 'SUM', displayFormat: 'number',   decimalPlaces: 0 },
  { name: 'AcquisitionYearMetric',     fieldType: 'metric', label: 'Acquisition Year (val)',      aggregationType: 'MIN', displayFormat: 'number',   decimalPlaces: 0 },
];

async function main() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ── Step 1: Fix labels in dataset_field_metadata ─────────────────────────
    console.log('Fixing labels...');
    let fixed = 0;
    for (const [fieldName, correctLabel] of Object.entries(LABEL_OVERRIDES)) {
      const { rowCount } = await client.query(
        `UPDATE dataset_field_metadata SET label=$1 WHERE field_name=$2`,
        [correctLabel, fieldName]
      );
      if (rowCount > 0) {
        console.log(`  ${fieldName} → "${correctLabel}" (${rowCount} rows)`);
        fixed += rowCount;
      }
    }
    console.log(`  Total fixed: ${fixed} label(s)`);

    // ── Step 2: Seed dataset_schema_versions ─────────────────────────────────
    console.log('\nSeeding dataset_schema_versions...');

    const datasets = [
      { name: 'equipment',       fields: EQUIPMENT_FIELDS },
      { name: 'equipment_daily', fields: EQUIPMENT_DAILY_FIELDS },
    ];

    for (const ds of datasets) {
      // Get dataset_definition id + current_version
      const { rows: [def] } = await client.query(
        `SELECT id, current_version FROM dataset_definitions WHERE client_id='fcc' AND name=$1`,
        [ds.name]
      );
      if (!def) { console.error(`  ERROR: dataset ${ds.name} not found`); continue; }

      const version = def.current_version || 1;

      // Ensure current_version is set on dataset_definitions
      await client.query(
        `UPDATE dataset_definitions SET current_version=$1 WHERE id=$2`,
        [version, def.id]
      );

      // Build the fields JSONB — strip the AcquisitionYearMetric placeholder (it's just a segment in real data)
      const fields = ds.fields.filter(f => f.name !== 'AcquisitionYearMetric');

      const segCount  = fields.filter(f => f.fieldType === 'segment').length;
      const metCount  = fields.filter(f => f.fieldType === 'metric').length;

      await client.query(
        `INSERT INTO dataset_schema_versions (dataset_id, version, fields, es_index, published_at, published_by, compat_status)
         VALUES ($1, $2, $3::jsonb, $4, NOW(), 'system', 'compatible')
         ON CONFLICT (dataset_id, version) DO UPDATE
           SET fields=$3::jsonb, published_at=NOW()`,
        [def.id, version, JSON.stringify(fields), `fcc_fcc_${ds.name}`]
      );

      console.log(`  ${ds.name} v${version}: ${segCount} segments, ${metCount} metrics → seeded`);
    }

    await client.query('COMMIT');
    console.log('\nFix complete.');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('FAILED:', e.message);
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(e => { console.error(e); process.exit(1); });
