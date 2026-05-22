/**
 * Admin endpoints for Pipeline management.
 *
 * GET  /mgmt/pipeline/stages                  — list all stages (all macros)
 * PATCH /mgmt/pipeline/stages/{macroKey}/{stageKey} — toggle enabled or update config
 * GET  /mgmt/pipeline/postback-log            — postback execution log with stage results
 * GET  /mgmt/pipeline/postback-log/{id}       — single postback log detail
 * GET  /mgmt/dnc                              — list DNC entries
 * POST /mgmt/dnc                              — add phone to DNC
 * DELETE /mgmt/dnc/{phone}                   — remove phone from DNC
 */
const { app }  = require('@azure/functions');
const { getPool, sql } = require('../../lib/db');
const { requireAdminKey, corsHeaders } = require('./middleware');
const { invalidatePipelineCache } = require('../../lib/pipeline');

// ── OPTIONS preflight ─────────────────────────────────────────────────────────
app.http('pipelineOptions', {
  methods: ['OPTIONS'],
  route: 'mgmt/pipeline/{*rest}',
  authLevel: 'anonymous',
  handler: async () => ({ status: 204, headers: corsHeaders(), body: '' }),
});
app.http('dncOptions', {
  methods: ['OPTIONS'],
  route: 'mgmt/dnc/{*rest}',
  authLevel: 'anonymous',
  handler: async () => ({ status: 204, headers: corsHeaders(), body: '' }),
});

// ── GET /mgmt/pipeline/stages ─────────────────────────────────────────────────
app.http('pipelineStagesList', {
  methods: ['GET'],
  route: 'mgmt/pipeline/stages',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;
    const pool = await getPool();
    const r    = await pool.request().query(`
      SELECT macro_key, stage_key, stage_name, description, step_order, enabled, config, updated_at
      FROM pipeline_stages ORDER BY macro_key, step_order
    `);
    const rows = r.recordset.map(s => ({ ...s, config: s.config ? JSON.parse(s.config) : null }));
    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(rows) };
  },
});

// ── PATCH /mgmt/pipeline/stages/{macroKey}/{stageKey} ─────────────────────────
app.http('pipelineStageUpdate', {
  methods: ['PATCH'],
  route: 'mgmt/pipeline/stages/{macroKey}/{stageKey}',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;
    let body;
    try { body = await req.json(); } catch { return { status: 400, body: JSON.stringify({ error: 'invalid json' }) }; }

    const { macroKey, stageKey } = req.params;
    const pool = await getPool();
    const request = pool.request()
      .input('macro', sql.NVarChar(100), macroKey)
      .input('stage', sql.NVarChar(100), stageKey);

    const sets = ['updated_at = SYSDATETIMEOFFSET()'];
    if (body.enabled !== undefined) { sets.push('enabled = @enabled'); request.input('enabled', sql.Bit, body.enabled ? 1 : 0); }
    if (body.config  !== undefined) { sets.push('config = @config');   request.input('config',  sql.NVarChar(sql.MAX), JSON.stringify(body.config)); }

    await request.query(`
      UPDATE pipeline_stages SET ${sets.join(', ')}
      WHERE macro_key = @macro AND stage_key = @stage
    `);

    invalidatePipelineCache();
    return { status: 200, body: JSON.stringify({ ok: true }) };
  },
});

// ── GET /mgmt/pipeline/postback-log ──────────────────────────────────────────
app.http('pipelinePostbackLog', {
  methods: ['GET'],
  route: 'mgmt/pipeline/postback-log',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const page     = Math.max(1, parseInt(req.query.get('page')  ?? '1',  10));
    const pageSize = Math.min(200, parseInt(req.query.get('size') ?? '50', 10));
    const action   = req.query.get('action')   ?? null;
    const phone    = req.query.get('phone')    ?? null;
    const campaign = req.query.get('campaign') ?? null;
    const offset   = (page - 1) * pageSize;

    const pool    = await getPool();
    const request = pool.request()
      .input('offset',   sql.Int, offset)
      .input('pageSize', sql.Int, pageSize);

    const conditions = [];
    if (action)   { conditions.push('final_action = @action');       request.input('action',   sql.NVarChar(50),  action); }
    if (phone)    { conditions.push('phone = @phone');               request.input('phone',    sql.NVarChar(20),  phone); }
    if (campaign) { conditions.push('campaign = @campaign');         request.input('campaign', sql.NVarChar(255), campaign); }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const r = await request.query(`
      SELECT id, phone, call_id, ping_id, mts_id, campaign, vertical,
             publisher_id, buyer_id, call_duration_sec, bid_amount,
             macro_key, final_action, received_at
      FROM postback_log
      ${where}
      ORDER BY received_at DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY
    `);
    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(r.recordset) };
  },
});

// ── GET /mgmt/pipeline/postback-log/{id} ─────────────────────────────────────
app.http('pipelinePostbackLogDetail', {
  methods: ['GET'],
  route: 'mgmt/pipeline/postback-log/{id}',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;
    const pool = await getPool();
    const r    = await pool.request()
      .input('id', sql.UniqueIdentifier, req.params.id)
      .query('SELECT * FROM postback_log WHERE id = @id');
    if (!r.recordset.length) return { status: 404, body: JSON.stringify({ error: 'not found' }) };
    const row = r.recordset[0];
    return {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders() },
      body: JSON.stringify({ ...row, stage_results: row.stage_results ? JSON.parse(row.stage_results) : [] }),
    };
  },
});

// ── GET /mgmt/dnc ─────────────────────────────────────────────────────────────
app.http('dncList', {
  methods: ['GET'],
  route: 'mgmt/dnc',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;
    const page     = Math.max(1, parseInt(req.query.get('page')  ?? '1',  10));
    const pageSize = Math.min(500, parseInt(req.query.get('size') ?? '100', 10));
    const offset   = (page - 1) * pageSize;
    const pool     = await getPool();
    const r        = await pool.request()
      .input('offset',   sql.Int, offset)
      .input('pageSize', sql.Int, pageSize)
      .query(`
        SELECT id, phone, source, reason, added_at, expires_at, added_by
        FROM dnc_list
        ORDER BY added_at DESC
        OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY
      `);
    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(r.recordset) };
  },
});

// ── POST /mgmt/dnc ────────────────────────────────────────────────────────────
app.http('dncAdd', {
  methods: ['POST'],
  route: 'mgmt/dnc',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;
    let body;
    try { body = await req.json(); } catch { return { status: 400, body: JSON.stringify({ error: 'invalid json' }) }; }

    const { normalizePhone } = require('../../lib/normalize');
    const { v4: uuidv4 }     = require('uuid');
    const phones = Array.isArray(body.phones) ? body.phones : [body.phone];
    const source     = body.source     ?? 'manual';
    const reason     = body.reason     ?? null;
    const expires_at = body.expires_at ? new Date(body.expires_at) : null;
    const added_by   = body.added_by   ?? null;

    const pool    = await getPool();
    let inserted  = 0;
    let skipped   = 0;
    for (const raw of phones) {
      const phone = normalizePhone(raw);
      if (!phone) { skipped++; continue; }
      try {
        await pool.request()
          .input('id',         sql.UniqueIdentifier, uuidv4())
          .input('phone',      sql.NVarChar(20),     phone)
          .input('source',     sql.NVarChar(50),     source)
          .input('reason',     sql.NVarChar(255),    reason)
          .input('expires_at', sql.DateTimeOffset,   expires_at)
          .input('added_by',   sql.NVarChar(255),    added_by)
          .query(`
            MERGE dnc_list AS t USING (SELECT @phone AS phone) AS s ON t.phone = s.phone
            WHEN NOT MATCHED THEN
              INSERT (id, phone, source, reason, expires_at, added_by)
              VALUES (@id, @phone, @source, @reason, @expires_at, @added_by);
          `);
        inserted++;
      } catch { skipped++; }
    }
    return { status: 200, body: JSON.stringify({ ok: true, inserted, skipped }) };
  },
});

// ── DELETE /mgmt/dnc/{phone} ──────────────────────────────────────────────────
app.http('dncRemove', {
  methods: ['DELETE'],
  route: 'mgmt/dnc/{phone}',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;
    const pool = await getPool();
    await pool.request()
      .input('phone', sql.NVarChar(20), req.params.phone)
      .query('DELETE FROM dnc_list WHERE phone = @phone');
    return { status: 200, body: JSON.stringify({ ok: true }) };
  },
});
