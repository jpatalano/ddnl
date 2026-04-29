#!/usr/bin/env node
'use strict';
const https = require('https');
const http  = require('http');
const url   = require('url');
const { Pool } = require('pg');

const API_BASE    = 'https://fcc-app-production.up.railway.app';
const API_KEY     = 'ik_fcc_2a98cd7b986ff20ed9bff0fa1aed9644b2621e0b124a1646bbd9c41683a44944';
const INSTANCE_ID = 'fcc';
const DB_URL      = 'postgresql://postgres:FccDbPass2026!@shortline.proxy.rlwy.net:56142/railway';

const DATASETS = ['quote','quote_line','job','job_daily','ticket','invoice',
                  'invoice_line','payment','ar_snapshot','q2c_funnel'];

const pool = new Pool({ connectionString: DB_URL, ssl: false });

function postJson(path, body) {
  return new Promise((resolve, reject) => {
    const parsed = new url.URL(API_BASE + path);
    const lib    = parsed.protocol === 'https:' ? https : http;
    const data   = JSON.stringify(body);
    const opts   = {
      hostname: parsed.hostname,
      port:     parsed.port || 443,
      path:     parsed.pathname,
      method:   'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(data),
        'X-Api-Key':      API_KEY,
        'X-Instance-Id':  INSTANCE_ID,
      },
    };
    const req = lib.request(opts, res => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch(e) { reject(new Error('Parse: ' + raw.slice(0,200))); }
      });
    });
    req.on('error', reject);
    req.write(data); req.end();
  });
}

async function main() {
  const client = await pool.connect();
  try {
    for (const dsName of DATASETS) {
      // Load field metadata
      const { rows: [def] } = await client.query(
        `SELECT id FROM dataset_definitions WHERE client_id='fcc' AND name=$1`, [dsName]
      );
      if (!def) { console.log(`  ${dsName}: NOT FOUND in DB`); continue; }

      const { rows: meta } = await client.query(
        `SELECT field_name, label, field_type, format FROM dataset_field_metadata
         WHERE dataset_id=$1 ORDER BY id`, [def.id]
      );

      // Map to field schema expected by publish route
      const fields = meta.map(m => ({
        name:      m.field_name,
        label:     m.label || m.field_name,
        fieldType: m.field_type === 'metric' ? 'metric' : 'segment',
        type:      m.field_type === 'metric' ? 'number'
                 : m.format === 'date'       ? 'date'
                 : 'keyword',
        format:    m.format || (m.field_type === 'metric' ? 'number' : 'text'),
        isActive:  true,
        aggregationType: m.field_type === 'metric' ? 'SUM' : null,
      }));

      process.stdout.write(`  Publishing ${dsName} (${fields.length} fields)... `);
      try {
        const r = await postJson(`/api/ingest/admin/datasets/${dsName}/publish`, { fields, force: true });
        if (r.success) console.log(`OK v${r.newVersion} → ${r.esIndex}`);
        else console.log(`ERR: ${r.error || r.message}`);
      } catch(e) {
        console.log(`ERR: ${e.message}`);
      }
    }
  } finally {
    client.release();
    await pool.end();
  }
}
main().catch(e => { console.error(e); process.exit(1); });
