/**
 * llmProvider.js — Clean LLM provider interface for Advise module
 *
 * Providers:
 *   openai    — OpenAI Chat Completions (default)
 *   anthropic — Anthropic Messages API
 *   mock      — Returns deterministic mock text (no API calls)
 *
 * Config via env vars:
 *   ADVISE_LLM_PROVIDER   = openai | anthropic | mock  (default: mock if no key present)
 *   OPENAI_API_KEY        — required for openai
 *   OPENAI_MODEL          — default: gpt-4o-mini
 *   ANTHROPIC_API_KEY     — required for anthropic
 *   ANTHROPIC_MODEL       — default: claude-3-haiku-20240307
 *
 * Usage:
 *   const llm = require('./llmProvider');
 *   const text = await llm.complete(systemPrompt, userPrompt);
 */

'use strict';

const https    = require('https');
const { pool } = require('./db');

// ── Per-instance settings (DB wins over env var) ──────────────────────────────
//
// Settings are stored in instance_settings(client_id, key, value).
// For sensitive values (api_key) the DB value is used directly — no extra
// encryption layer at this stage; the DB itself should be the trust boundary.
//
// getInstanceSetting(clientId, key, envFallback)
//   → DB value if set, else process.env[envFallback] if set, else null

async function getInstanceSetting(clientId, key, envFallback = null) {
  try {
    const { rows } = await pool.query(
      `SELECT value FROM instance_settings WHERE client_id=$1 AND key=$2 LIMIT 1`,
      [clientId, key]
    );
    if (rows.length && rows[0].value !== null && rows[0].value !== '') {
      return rows[0].value;
    }
  } catch (e) {
    // Non-fatal — fall through to env var
    console.warn(`[llmProvider] getInstanceSetting(${key}) DB error:`, e.message);
  }
  return envFallback ? (process.env[envFallback] || null) : null;
}

async function setInstanceSetting(clientId, key, value) {
  await pool.query(
    `INSERT INTO instance_settings (client_id, key, value, updated_at)
     VALUES ($1, $2, $3, NOW())
     ON CONFLICT (client_id, key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()`,
    [clientId, key, value]
  );
}

// ── Provider resolution ───────────────────────────────────────────────────────
// Synchronous fallback used when clientId is not available.
// Prefer resolveProviderForClient(clientId) in async contexts.

function resolveProvider() {
  const explicit = (process.env.ADVISE_LLM_PROVIDER || '').toLowerCase();
  if (explicit === 'openai')    return 'openai';
  if (explicit === 'anthropic') return 'anthropic';
  if (explicit === 'mock')      return 'mock';
  // Auto-detect from available keys
  if (process.env.OPENAI_API_KEY)    return 'openai';
  if (process.env.ANTHROPIC_API_KEY) return 'anthropic';
  return 'mock';
}

async function resolveProviderForClient(clientId) {
  const stored = await getInstanceSetting(clientId, 'ai.provider', 'ADVISE_LLM_PROVIDER');
  if (!stored) {
    if (process.env.OPENAI_API_KEY)    return 'openai';
    if (process.env.ANTHROPIC_API_KEY) return 'anthropic';
    return 'mock';
  }
  return stored;
}

// ── HTTP helper — native https only (no axios/fetch) ─────────────────────────

function httpsPost(hostname, path, headers, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const options = {
      hostname,
      path,
      method: 'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(payload),
        ...headers
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch (e) {
          reject(new Error(`LLM: invalid JSON response — ${data.slice(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ── OpenAI ────────────────────────────────────────────────────────────────────

async function completeOpenAI(systemPrompt, userPrompt, apiKey, model) {
  const useKey   = apiKey || process.env.OPENAI_API_KEY;
  const useModel = model  || process.env.OPENAI_MODEL || 'gpt-4o-mini';
  const result = await httpsPost(
    'api.openai.com',
    '/v1/chat/completions',
    { Authorization: `Bearer ${useKey}` },
    {
      model: useModel,
      max_tokens: 512,
      temperature: 0.4,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user',   content: userPrompt }
      ]
    }
  );
  if (result.status !== 200) {
    throw new Error(`OpenAI error ${result.status}: ${JSON.stringify(result.body?.error)}`);
  }
  return result.body?.choices?.[0]?.message?.content?.trim() || '';
}

// ── Anthropic ─────────────────────────────────────────────────────────────────

async function completeAnthropic(systemPrompt, userPrompt, apiKey, model) {
  const useKey   = apiKey || process.env.ANTHROPIC_API_KEY;
  const useModel = model  || process.env.ANTHROPIC_MODEL || 'claude-3-haiku-20240307';
  const result = await httpsPost(
    'api.anthropic.com',
    '/v1/messages',
    {
      'x-api-key':         useKey,
      'anthropic-version': '2023-06-01'
    },
    {
      model: useModel,
      max_tokens: 512,
      system: systemPrompt,
      messages: [{ role: 'user', content: userPrompt }]
    }
  );
  if (result.status !== 200) {
    throw new Error(`Anthropic error ${result.status}: ${JSON.stringify(result.body?.error)}`);
  }
  return result.body?.content?.[0]?.text?.trim() || '';
}

// ── Mock ──────────────────────────────────────────────────────────────────────

function completeMock(systemPrompt, userPrompt) {
  // Deterministic mock — extracts the finding description from the prompt
  const match = userPrompt.match(/Finding:\s*(.+?)(?:\n|$)/i);
  const finding = match ? match[1].trim() : 'this metric is outside expected range';
  return Promise.resolve(
    `Based on current performance data, ${finding}. ` +
    `Review operational patterns and consider reallocation or process adjustment ` +
    `to bring this metric back into target range. [mock — configure ADVISE_LLM_PROVIDER]`
  );
}

// ── Public interface ──────────────────────────────────────────────────────────

/**
 * Generate a recommendation narrative for a single Advise finding.
 *
 * @param {string} systemPrompt  — role/context framing
 * @param {string} userPrompt    — finding description + data
 * @returns {Promise<string>}    — recommendation text
 */
async function complete(systemPrompt, userPrompt, clientId = null) {
  const provider = clientId ? await resolveProviderForClient(clientId) : resolveProvider();
  const apiKey   = clientId ? await getInstanceSetting(clientId, 'ai.api_key',
                                provider === 'openai' ? 'OPENAI_API_KEY' : 'ANTHROPIC_API_KEY') : null;
  const model    = clientId ? await getInstanceSetting(clientId, 'ai.model', null) : null;
  try {
    switch (provider) {
      case 'openai':    return await completeOpenAI(systemPrompt, userPrompt, apiKey, model);
      case 'anthropic': return await completeAnthropic(systemPrompt, userPrompt, apiKey, model);
      default:          return await completeMock(systemPrompt, userPrompt);
    }
  } catch (err) {
    console.error(`[llmProvider:${provider}] Error:`, err.message);
    // Fallback: return mock so Advise always produces something
    return completeMock(systemPrompt, userPrompt);
  }
}

/**
 * Build the system prompt for a given Advise role lens.
 * Caller passes the role string from the UI picker.
 */
function systemPromptForRole(role) {
  const base = `You are an expert business advisor for a crane service and equipment rental company. 
You analyze operational KPIs and provide concise, actionable recommendations (2-3 sentences max).
Be direct, specific, and practical. Avoid generic filler language.`;

  const lensMap = {
    'owner':     `${base} The reader is the Owner or Executive — focus on financial impact, strategic risk, and high-level decisions.`,
    'ops':       `${base} The reader is an Operations Manager — focus on equipment placement, utilization efficiency, and yard-level actions.`,
    'finance':   `${base} The reader is in Finance or Billing — focus on revenue impact, margin, and billing/collection issues.`,
    'sales':     `${base} The reader is in Sales or Business Development — focus on customer opportunities, upsell, and revenue growth.`,
    'yard':      `${base} The reader is a Yard Manager — focus on day-to-day equipment, job scheduling, and local yard performance.`,
  };
  return lensMap[role] || lensMap['owner'];
}

module.exports = { complete, systemPromptForRole, resolveProvider, resolveProviderForClient, getInstanceSetting, setInstanceSetting };
