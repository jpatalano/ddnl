/**
 * aiChatRouter.js — AI Chat conversation API
 *
 * Mounted at /api/ai in server.js (behind global auth middleware).
 *
 * Endpoints:
 *   GET    /api/ai/conversations              — list conversations
 *   POST   /api/ai/conversations              — create conversation
 *   GET    /api/ai/conversations/:id          — load conversation + messages
 *   DELETE /api/ai/conversations/:id          — delete conversation
 *   POST   /api/ai/conversations/:id/messages — send message, get AI reply
 *   POST   /api/ai/conversations/:id/confirm  — execute a confirmed tag action
 *   PATCH  /api/ai/conversations/:id/title    — rename conversation
 *
 * Message flow:
 *   1. User message saved to ai_messages
 *   2. Full conversation history loaded
 *   3. LLM called with schema context + tool definitions
 *   4. If LLM emits a tool call → execute → feed result back → get final answer
 *   5. Assistant message (+ optional action_payload) saved
 *   6. Response returned to client
 *
 * First message auto-generates a conversation title via LLM.
 */

'use strict';

const express = require('express');
const router  = express.Router();
const { pool }                    = require('./db');
const llm                         = require('./llmProvider');
const { buildSchemaContext, getToolDefinitions, executeTool, executeConfirmedTagAction } = require('./aiQueryEngine');

// ── Helpers ───────────────────────────────────────────────────────────────────

function resolveClientId(req) {
  if (req.resolvedClientId) return req.resolvedClientId;
  const inst = req.app.locals.INSTANCE;
  return inst?.clientId || inst?.id || 'demo';
}

function resolveUsername(req) {
  // From TC session or basic auth header
  if (req.tcSession?.username) return req.tcSession.username;
  const auth = req.headers['authorization'];
  if (auth?.startsWith('Basic ')) {
    const [user] = Buffer.from(auth.slice(6), 'base64').toString().split(':');
    return user;
  }
  return 'user';
}

// ── System prompt builder ─────────────────────────────────────────────────────

function buildSystemPrompt(schemaSummary, role) {
  const roleDescriptions = {
    owner:   'Owner / Executive — focus on financial impact, strategic decisions, risk.',
    ops:     'Operations Manager — focus on equipment, utilization, yard efficiency.',
    finance: 'Finance / Billing — focus on revenue, margin, invoicing, collections.',
    sales:   'Sales / Business Development — focus on customer opportunities, growth, upsell.',
    yard:    'Yard Manager — focus on day-to-day equipment, jobs, local performance.'
  };
  const roleLine = roleDescriptions[role] || roleDescriptions.owner;

  return `You are an expert AI analyst for a crane service and equipment rental company.
You have direct access to the company's operational data and can query it to answer questions.

User role: ${roleLine}

${schemaSummary}

Guidelines:
- Always query the data before answering factual questions — never guess numbers.
- When you query, be specific: filter by relevant fields, sort by the most useful metric.
- Present results clearly: use bullet points or tables for lists, bold key numbers.
- When you notice something interesting in the data, call it out proactively.
- For "shaking trees" / outreach requests: query for customers who haven't had a job recently,
  sort by lifetime spend descending, and give the user a prioritized call list with context.
- For tag actions (apply/remove): explain what you're about to tag and why, then call the tool.
  The user will see a confirmation card and must approve before anything changes.
- Keep responses concise and actionable — the user is a busy operator, not a data analyst.
- If a query returns no results, say so clearly and suggest an alternative.`;
}

// ── LLM multi-turn with tools ─────────────────────────────────────────────────

/**
 * Call LLM with full conversation history + tools.
 * Handles one round of tool use (query → result → final answer).
 * Returns { content, tool_calls, tool_result, action_payload }
 */
async function callLlmWithTools(systemPrompt, messages, clientId) {
  // Resolve provider + credentials from DB (falls back to env vars)
  const provider = await llm.resolveProviderForClient(clientId);
  const apiKey   = await llm.getInstanceSetting(clientId, 'ai.api_key',
                     provider === 'openai' ? 'OPENAI_API_KEY' : 'ANTHROPIC_API_KEY');
  const model    = await llm.getInstanceSetting(clientId, 'ai.model', null);
  const tools    = getToolDefinitions();

  if (provider === 'mock') {
    return callLlmMock(messages, clientId);
  }

  if (provider === 'openai') {
    return callOpenAIWithTools(systemPrompt, messages, tools, clientId, apiKey, model);
  }

  if (provider === 'anthropic') {
    return callAnthropicWithTools(systemPrompt, messages, tools, clientId, apiKey, model);
  }

  return callLlmMock(messages, clientId);
}

// ── OpenAI tool loop ──────────────────────────────────────────────────────────

async function callOpenAIWithTools(systemPrompt, messages, tools, clientId, apiKey, modelOverride) {
  const https   = require('https');
  const model   = modelOverride || process.env.OPENAI_MODEL || 'gpt-4o-mini';
  const useKey  = apiKey || process.env.OPENAI_API_KEY;

  // Convert messages to OpenAI format
  const oaiMessages = [{ role: 'system', content: systemPrompt }];
  for (const m of messages) {
    if (m.role === 'user' || m.role === 'assistant') {
      oaiMessages.push({ role: m.role, content: m.content || '' });
    }
    if (m.role === 'tool' && m.tool_result) {
      // Tool results go as tool messages in OpenAI format
      oaiMessages.push({
        role: 'tool',
        tool_call_id: m.tool_call_id || 'call_0',
        content: JSON.stringify(m.tool_result)
      });
    }
  }

  // Convert tools to OpenAI format
  const oaiTools = tools.map(t => ({
    type: 'function',
    function: { name: t.name, description: t.description, parameters: t.input_schema }
  }));

  async function oaiCall(msgs) {
    return new Promise((resolve, reject) => {
      const body = JSON.stringify({
        model,
        messages: msgs,
        tools:    oaiTools,
        tool_choice: 'auto',
        max_tokens: 1024,
        temperature: 0.3
      });
      const options = {
        hostname: 'api.openai.com',
        path:     '/v1/chat/completions',
        method:   'POST',
        headers: {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(body),
          'Authorization':  `Bearer ${useKey}`
        }
      };
      const req = https.request(options, res => {
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => {
          try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
          catch(e) { reject(new Error('Invalid JSON: ' + data.slice(0,200))); }
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
  }

  // First call
  const r1 = await oaiCall(oaiMessages);
  if (r1.status !== 200) throw new Error(`OpenAI ${r1.status}: ${JSON.stringify(r1.body?.error)}`);

  const msg = r1.body.choices?.[0]?.message;
  if (!msg) throw new Error('No message in OpenAI response');

  // No tool call — done
  if (!msg.tool_calls?.length) {
    return { content: msg.content || '', tool_calls: null, tool_result: null, action_payload: null };
  }

  // Execute tool calls (take first for now)
  const tc       = msg.tool_calls[0];
  const toolName = tc.function.name;
  let toolInput;
  try { toolInput = JSON.parse(tc.function.arguments); } catch(e) { toolInput = {}; }

  const { tool_result, action_payload } = await executeTool(clientId, toolName, toolInput);

  // If action staged, return without second LLM call — UI shows confirmation card
  if (action_payload) {
    const summary = `I've prepared a tag action for your review. ${tool_result.match_count > 0 ? `This will affect ${tool_result.match_count} record(s).` : ''} Please confirm or dismiss below.`;
    return { content: summary, tool_calls: [{ name: toolName, input: toolInput }], tool_result, action_payload };
  }

  // Feed result back for final answer
  const msgs2 = [...oaiMessages, msg, {
    role: 'tool',
    tool_call_id: tc.id,
    content: JSON.stringify(tool_result)
  }];

  const r2 = await oaiCall(msgs2);
  if (r2.status !== 200) throw new Error(`OpenAI r2 ${r2.status}: ${JSON.stringify(r2.body?.error)}`);

  const finalContent = r2.body.choices?.[0]?.message?.content || '';
  return { content: finalContent, tool_calls: [{ name: toolName, input: toolInput }], tool_result, action_payload: null };
}

// ── Anthropic tool loop ───────────────────────────────────────────────────────

async function callAnthropicWithTools(systemPrompt, messages, tools, clientId, apiKey, modelOverride) {
  const https  = require('https');
  const model  = modelOverride || process.env.ANTHROPIC_MODEL || 'claude-3-haiku-20240307';
  const useKey = apiKey || process.env.ANTHROPIC_API_KEY;

  // Convert to Anthropic format
  const anthMessages = [];
  for (const m of messages) {
    if (m.role === 'user') {
      anthMessages.push({ role: 'user', content: m.content || '' });
    } else if (m.role === 'assistant') {
      anthMessages.push({ role: 'assistant', content: m.content || '' });
    }
  }

  async function anthCall(msgs, includeTools = true) {
    const bodyObj = {
      model,
      max_tokens: 1024,
      system:     systemPrompt,
      messages:   msgs
    };
    if (includeTools) bodyObj.tools = tools;

    return new Promise((resolve, reject) => {
      const body = JSON.stringify(bodyObj);
      const options = {
        hostname: 'api.anthropic.com',
        path:     '/v1/messages',
        method:   'POST',
        headers: {
          'Content-Type':   'application/json',
          'Content-Length': Buffer.byteLength(body),
          'x-api-key':      useKey,
          'anthropic-version': '2023-06-01'
        }
      };
      const req = https.request(options, res => {
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => {
          try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
          catch(e) { reject(new Error('Invalid JSON: ' + data.slice(0,200))); }
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
  }

  const r1 = await anthCall(anthMessages);
  if (r1.status !== 200) throw new Error(`Anthropic ${r1.status}: ${JSON.stringify(r1.body?.error)}`);

  const content = r1.body.content || [];
  const textBlock = content.find(b => b.type === 'text');
  const toolBlock = content.find(b => b.type === 'tool_use');

  if (!toolBlock) {
    return { content: textBlock?.text || '', tool_calls: null, tool_result: null, action_payload: null };
  }

  const toolName  = toolBlock.name;
  const toolInput = toolBlock.input || {};
  const { tool_result, action_payload } = await executeTool(clientId, toolName, toolInput);

  if (action_payload) {
    const summary = `I've prepared a tag action for your review. ${tool_result.match_count > 0 ? `This will affect ${tool_result.match_count} record(s).` : ''} Please confirm or dismiss below.`;
    return { content: summary, tool_calls: [{ name: toolName, input: toolInput }], tool_result, action_payload };
  }

  // Feed result back
  const msgs2 = [
    ...anthMessages,
    { role: 'assistant', content },
    {
      role: 'user',
      content: [{
        type:        'tool_result',
        tool_use_id: toolBlock.id,
        content:     JSON.stringify(tool_result)
      }]
    }
  ];

  const r2 = await anthCall(msgs2, false);
  if (r2.status !== 200) throw new Error(`Anthropic r2 ${r2.status}: ${JSON.stringify(r2.body?.error)}`);

  const finalText = (r2.body.content || []).find(b => b.type === 'text')?.text || '';
  return { content: finalText, tool_calls: [{ name: toolName, input: toolInput }], tool_result, action_payload: null };
}

// ── Mock handler ──────────────────────────────────────────────────────────────

async function callLlmMock(messages, clientId) {
  const lastUser = [...messages].reverse().find(m => m.role === 'user');
  const q = lastUser?.content?.toLowerCase() || '';

  let content = 'I\'m running in mock mode (no LLM key configured). Configure `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` to enable full AI responses.';

  if (q.includes('idle') || q.includes('equipment')) {
    content = '**Mock response:** To find idle equipment I would query the equipment dataset filtering for LastJobDate older than 14 days, sorted by idle time descending. Configure an LLM key to get real answers.';
  } else if (q.includes('customer') || q.includes('call') || q.includes('tree')) {
    content = '**Mock response:** For a call list I would query customers sorted by TotalSpend descending, filtered for LastJobDate > 90 days ago. Configure an LLM key to get a real prioritized list.';
  } else if (q.includes('revenue') || q.includes('yard')) {
    content = '**Mock response:** For yard revenue comparison I would aggregate revenue by YardName and compare against peer averages. Configure an LLM key for full analysis.';
  }

  return { content, tool_calls: null, tool_result: null, action_payload: null };
}

// ── Auto-title generator ──────────────────────────────────────────────────────

async function generateTitle(firstUserMessage) {
  try {
    const title = await llm.complete(
      'Generate a very short (4-6 word) conversation title based on the user\'s question. Return only the title, no quotes.',
      `User message: ${firstUserMessage.slice(0, 200)}`
    );
    return title.replace(/['"]/g, '').trim().slice(0, 80) || 'New conversation';
  } catch (e) {
    return 'New conversation';
  }
}

// ── Routes ────────────────────────────────────────────────────────────────────

// GET /api/ai/conversations
router.get('/conversations', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const { rows } = await pool.query(`
      SELECT id, title, created_by, created_at, updated_at,
             (SELECT COUNT(*) FROM ai_messages WHERE conversation_id = ac.id) AS message_count
      FROM ai_conversations ac
      WHERE client_id = $1
      ORDER BY updated_at DESC
      LIMIT 100
    `, [clientId]);
    res.json({ success: true, conversations: rows });
  } catch (err) {
    console.error('[aiChatRouter] GET /conversations:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/ai/conversations
router.post('/conversations', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const username = resolveUsername(req);
    const { title } = req.body || {};
    const { rows } = await pool.query(`
      INSERT INTO ai_conversations (client_id, created_by, title)
      VALUES ($1, $2, $3)
      RETURNING id, title, created_by, created_at, updated_at
    `, [clientId, username, title || 'New conversation']);
    res.json({ success: true, conversation: rows[0] });
  } catch (err) {
    console.error('[aiChatRouter] POST /conversations:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET /api/ai/conversations/:id
router.get('/conversations/:id', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) return res.status(400).json({ success: false, error: 'Invalid id' });

    const convRes = await pool.query(`
      SELECT id, title, created_by, created_at, updated_at
      FROM ai_conversations WHERE id = $1 AND client_id = $2
    `, [id, clientId]);
    if (!convRes.rows.length) return res.status(404).json({ success: false, error: 'Not found' });

    const msgRes = await pool.query(`
      SELECT id, role, content, tool_calls, tool_result, action_payload, created_at
      FROM ai_messages WHERE conversation_id = $1
      ORDER BY created_at ASC
    `, [id]);

    res.json({ success: true, conversation: convRes.rows[0], messages: msgRes.rows });
  } catch (err) {
    console.error('[aiChatRouter] GET /conversations/:id:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// DELETE /api/ai/conversations/:id
router.delete('/conversations/:id', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) return res.status(400).json({ success: false, error: 'Invalid id' });
    await pool.query(`DELETE FROM ai_conversations WHERE id = $1 AND client_id = $2`, [id, clientId]);
    res.json({ success: true });
  } catch (err) {
    console.error('[aiChatRouter] DELETE /conversations/:id:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// PATCH /api/ai/conversations/:id/title
router.patch('/conversations/:id/title', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const id    = parseInt(req.params.id, 10);
    const title = (req.body?.title || '').trim().slice(0, 255);
    if (!title) return res.status(400).json({ success: false, error: 'title required' });
    const { rows } = await pool.query(`
      UPDATE ai_conversations SET title = $1, updated_at = NOW()
      WHERE id = $2 AND client_id = $3
      RETURNING id, title
    `, [title, id, clientId]);
    if (!rows.length) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, conversation: rows[0] });
  } catch (err) {
    console.error('[aiChatRouter] PATCH /title:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/ai/conversations/:id/messages
// Main message endpoint — saves user message, calls LLM, saves response.
router.post('/conversations/:id/messages', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const convId   = parseInt(req.params.id, 10);
    if (isNaN(convId)) return res.status(400).json({ success: false, error: 'Invalid id' });

    const { content, role: requestedRole } = req.body || {};
    if (!content?.trim()) return res.status(400).json({ success: false, error: 'content required' });

    const role = requestedRole || 'owner';

    // Verify conversation belongs to client
    const convRes = await pool.query(`
      SELECT id, title FROM ai_conversations WHERE id = $1 AND client_id = $2
    `, [convId, clientId]);
    if (!convRes.rows.length) return res.status(404).json({ success: false, error: 'Conversation not found' });

    // Save user message
    const userMsgRes = await pool.query(`
      INSERT INTO ai_messages (conversation_id, role, content)
      VALUES ($1, 'user', $2) RETURNING id, created_at
    `, [convId, content.trim()]);
    const userMsgId = userMsgRes.rows[0].id;

    // Load full conversation history (for context window)
    const histRes = await pool.query(`
      SELECT role, content, tool_calls, tool_result, action_payload
      FROM ai_messages WHERE conversation_id = $1
      ORDER BY created_at ASC
    `, [convId]);
    const history = histRes.rows;

    // Build schema context for this client
    const { schemaSummary } = await buildSchemaContext(clientId);
    const systemPrompt = buildSystemPrompt(schemaSummary, role);

    // Call LLM with tools
    let llmResult;
    try {
      llmResult = await callLlmWithTools(systemPrompt, history, clientId);
    } catch (err) {
      console.error('[aiChatRouter] LLM call failed:', err.message);
      llmResult = {
        content: `I encountered an error querying the data: ${err.message}. Please try again.`,
        tool_calls: null, tool_result: null, action_payload: null
      };
    }

    // Save assistant message
    const asstMsgRes = await pool.query(`
      INSERT INTO ai_messages
        (conversation_id, role, content, tool_calls, tool_result, action_payload)
      VALUES ($1, 'assistant', $2, $3, $4, $5)
      RETURNING id, created_at
    `, [
      convId,
      llmResult.content,
      llmResult.tool_calls ? JSON.stringify(llmResult.tool_calls) : null,
      llmResult.tool_result ? JSON.stringify(llmResult.tool_result) : null,
      llmResult.action_payload ? JSON.stringify(llmResult.action_payload) : null
    ]);

    // Update conversation updated_at
    await pool.query(`UPDATE ai_conversations SET updated_at = NOW() WHERE id = $1`, [convId]);

    // Auto-title on first exchange (only if title is still default)
    const conv = convRes.rows[0];
    if (conv.title === 'New conversation' && history.length <= 1) {
      generateTitle(content).then(title => {
        pool.query(`UPDATE ai_conversations SET title = $1 WHERE id = $2`, [title, convId])
          .catch(() => {});
      });
    }

    res.json({
      success: true,
      user_message: {
        id:         userMsgId,
        role:       'user',
        content:    content.trim(),
        created_at: userMsgRes.rows[0].created_at
      },
      assistant_message: {
        id:             asstMsgRes.rows[0].id,
        role:           'assistant',
        content:        llmResult.content,
        tool_calls:     llmResult.tool_calls,
        tool_result:    llmResult.tool_result,
        action_payload: llmResult.action_payload,
        created_at:     asstMsgRes.rows[0].created_at
      }
    });
  } catch (err) {
    console.error('[aiChatRouter] POST /messages:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/ai/conversations/:id/confirm
// Execute a confirmed tag action.
router.post('/conversations/:id/confirm', async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const convId   = parseInt(req.params.id, 10);
    const { message_id, confirmed } = req.body || {};

    if (!message_id) return res.status(400).json({ success: false, error: 'message_id required' });

    // Load the message with the action_payload
    const msgRes = await pool.query(`
      SELECT am.id, am.action_payload
      FROM ai_messages am
      JOIN ai_conversations ac ON ac.id = am.conversation_id
      WHERE am.id = $1 AND ac.client_id = $2
    `, [message_id, clientId]);

    if (!msgRes.rows.length) return res.status(404).json({ success: false, error: 'Message not found' });
    const msgRow = msgRes.rows[0];
    if (!msgRow.action_payload) return res.status(400).json({ success: false, error: 'No action pending on this message' });

    const payload = msgRow.action_payload;

    if (!confirmed) {
      // User dismissed — mark as dismissed
      await pool.query(`
        UPDATE ai_messages SET action_payload = action_payload || '{"confirmed":false,"dismissed":true}'
        WHERE id = $1
      `, [message_id]);

      // Save a follow-up message
      await pool.query(`
        INSERT INTO ai_messages (conversation_id, role, content)
        VALUES ($1, 'assistant', $2)
      `, [convId, `Got it — I've cancelled the tag action.`]);

      return res.json({ success: true, dismissed: true });
    }

    // Execute
    const result = await executeConfirmedTagAction(clientId, payload);

    // Mark message as confirmed
    await pool.query(`
      UPDATE ai_messages
      SET action_payload = action_payload || '{"confirmed":true}'
      WHERE id = $1
    `, [message_id]);

    // Save confirmation result as assistant message
    const action    = payload.action;
    const tagLabel  = payload.tag_label;
    const summary   = action === 'apply'
      ? `Done — applied tag **${tagLabel}** to ${result.applied} record(s).`
      : `Done — removed tag **${tagLabel}** from ${result.removed} record(s).`;

    await pool.query(`
      INSERT INTO ai_messages (conversation_id, role, content)
      VALUES ($1, 'assistant', $2)
    `, [convId, summary]);
    await pool.query(`UPDATE ai_conversations SET updated_at = NOW() WHERE id = $1`, [convId]);

    res.json({ success: true, result, summary });
  } catch (err) {
    console.error('[aiChatRouter] POST /confirm:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

module.exports = router;
