#!/usr/bin/env node
/**
 * fcc-reindex-schema.js
 * Delete existing v1 auto-mapped indices, recreate with proper keyword mappings,
 * update schema version in Postgres.
 */
process.chdir(__dirname);

const es     = require('./esClient');
const { pool } = require('./db');

const CLIENT_ID = 'fcc';
const DATASETS  = ['equipment', 'equipment_daily'];

async function main() {
  // Load fields from DB
  for (const dsName of DATASETS) {
    console.log(`\n── ${dsName} ──`);

    const { rows: [def] } = await pool.query(
      `SELECT id, current_version FROM dataset_definitions WHERE client_id=$1 AND name=$2`,
      [CLIENT_ID, dsName]
    );
    if (!def) { console.error('  Not found'); continue; }

    // Get fields from schema version we seeded earlier (may be version 0 or 1)
    const { rows } = await pool.query(
      `SELECT fields, version FROM dataset_schema_versions WHERE dataset_id=$1 ORDER BY version DESC LIMIT 1`,
      [def.id]
    );
    if (!rows.length) { console.error('  No schema version found'); continue; }

    const { fields, version } = rows[0];
    console.log(`  Schema: v${version}, ${fields.length} fields`);

    // Delete old auto-mapped index if exists
    const oldIdxName = `fcc__${dsName}__v${version}`;
    try {
      const esClient = es.getClient();
      const exists = await esClient.indices.exists({ index: oldIdxName });
      if (exists) {
        console.log(`  Deleting ${oldIdxName}...`);
        await esClient.indices.delete({ index: oldIdxName });
        console.log(`  Deleted`);
      } else {
        console.log(`  Index ${oldIdxName} not found, skipping delete`);
      }
    } catch (e) {
      console.log(`  Delete attempt: ${e.message}`);
    }

    // Create new index with proper mappings
    console.log(`  Creating ${oldIdxName} with schema mappings...`);
    const newIdx = await es.createIndex(CLIENT_ID, dsName, version, fields);
    console.log(`  Created: ${newIdx}`);

    // Swap alias to new index (no old version to remove)
    await es.swapAlias(CLIENT_ID, dsName, version, null);
    console.log(`  Alias fcc__${dsName} → ${newIdx}`);

    // Ensure DB version is correct
    await pool.query(
      `UPDATE dataset_definitions SET current_version=$1 WHERE client_id=$2 AND name=$3`,
      [version, CLIENT_ID, dsName]
    );
    await pool.query(
      `UPDATE dataset_schema_versions SET es_index=$1, published_at=NOW() WHERE dataset_id=$2 AND version=$3`,
      [newIdx, def.id, version]
    );
    console.log(`  DB updated: current_version=${version}`);
  }

  await pool.end();
  console.log('\nReindex complete. Run simulator to re-ingest data.');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
