/**
 * Admin endpoints for Mid-Term Storage (MTS).
 *
 * GET  /mgmt/mts                    — paginated list (all verticals via master view)
 * GET  /mgmt/mts/{id}               — single MTS record with full audit trail
 * GET  /mgmt/mts/{id}/evaluate      — dry-run full decision tree for one record
 * POST /mgmt/mts/{id}/trigger       — manually run sequencer for one record
 * GET  /mgmt/mts/contact-history    — SMS contact history (suppression log)
 * GET  /mgmt/mts/categories         — category_mappings table
 * POST /mgmt/mts/categories         — upsert a category mapping
 * GET  /mgmt/mts/phone-categories   — phone_categories (phone → category membership)
 * POST /mgmt/mts/phone-categories   — upsert a phone category
 * POST /mgmt/mts/prune              — delete expired MTS rows (expires_at < NOW)
 * GET  /mgmt/mts/sequence-triggers  — sequence_triggers audit log
 */
const { app } = require('@azure/functions');
const { getPool, sql } = require('../../lib/db');
const { getConfig } = require('../../lib/config-cache');
const { requireAdminKey, corsHeaders } = require('./middleware');
const { runSequencer } = require('../../lib/sequencer');

// ── OPTIONS preflight ─────────────────────────────────────────────────────────
app.http('mtsOptions', {
  methods: ['OPTIONS'],
  route: 'mgmt/mts/{*rest}',
  authLevel: 'anonymous',
  handler: async () => ({ status: 204, headers: corsHeaders(), body: '' }),
});

// ── GET /mgmt/mts ─────────────────────────────────────────────────────────────
app.http('mtsListAll', {
  methods: ['GET'],
  route: 'mgmt/mts',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const page     = Math.max(1, parseInt(req.query.get('page')  ?? '1',  10));
    const pageSize = Math.min(200, parseInt(req.query.get('size') ?? '50', 10));
    const vertical = req.query.get('vertical') ?? null;
    const seqState = req.query.get('seq_state') ?? null;
    const offset   = (page - 1) * pageSize;

    const pool = await getPool();

    // Build filter conditions
    const conditions = [];
    const request = pool.request()
      .input('offset',   sql.Int, offset)
      .input('pageSize', sql.Int, pageSize);

    if (vertical) { conditions.push('vertical = @vertical'); request.input('vertical', sql.NVarChar(50), vertical); }
    if (seqState) { conditions.push('seq_state = @seqState'); request.input('seqState', sql.NVarChar(30), seqState); }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await request.query(`
      SELECT
        mts_id, ping_id, phone, zip, publisher_id, subid, campaign, vertical,
        seq_state, ringba_status, ringba_bid_amount, ringba_won, ringba_response_ms,
        requires_enrichment, enriched_at, seq_rtb_action, seq_sms_action,
        category_matched, sms_ext_status_code, mts_stored_at, mts_expires_at
      FROM vw_ping_master
      ${where}
      ORDER BY mts_stored_at DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY
    `);

    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(result.recordset) };
  },
});

// ── GET /mgmt/mts/{id} ────────────────────────────────────────────────────────
app.http('mtsGetOne', {
  methods: ['GET'],
  route: 'mgmt/mts/{id}',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const pool = await getPool();
    const result = await pool.request()
      .input('id', sql.UniqueIdentifier, req.params.id)
      .query('SELECT * FROM vw_ping_master WHERE mts_id = @id');

    if (!result.recordset.length) return { status: 404, body: JSON.stringify({ error: 'not found' }) };
    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(result.recordset[0]) };
  },
});

// ── GET /mgmt/mts/contact-history ────────────────────────────────────────────
app.http('mtsContactHistory', {
  methods: ['GET'],
  route: 'mgmt/mts/contact-history',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const page     = Math.max(1, parseInt(req.query.get('page')  ?? '1',  10));
    const pageSize = Math.min(200, parseInt(req.query.get('size') ?? '50', 10));
    const phone    = req.query.get('phone') ?? null;
    const offset   = (page - 1) * pageSize;

    const pool = await getPool();
    const request = pool.request()
      .input('offset',   sql.Int, offset)
      .input('pageSize', sql.Int, pageSize);

    const where = phone
      ? (request.input('phone', sql.NVarChar(20), phone), 'WHERE phone = @phone')
      : '';

    const result = await request.query(`
      SELECT id, phone, contact_type, channel, ping_id, mts_id, campaign, vertical, contacted_at
      FROM contact_history
      ${where}
      ORDER BY contacted_at DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY
    `);

    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(result.recordset) };
  },
});

// ── GET /mgmt/mts/categories ─────────────────────────────────────────────────
app.http('mtsCategoriesList', {
  methods: ['GET'],
  route: 'mgmt/mts/categories',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const pool = await getPool();
    const result = await pool.request().query(
      'SELECT * FROM category_mappings ORDER BY campaign, category_key',
    );
    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(result.recordset) };
  },
});

// ── POST /mgmt/mts/categories ─────────────────────────────────────────────────
app.http('mtsCategoriesUpsert', {
  methods: ['POST'],
  route: 'mgmt/mts/categories',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    let body;
    try { body = await req.json(); } catch { return { status: 400, body: JSON.stringify({ error: 'invalid json' }) }; }

    const { campaign, vertical, category_name, category_key, campaignkit_id, enabled = true } = body;
    if (!campaign || !vertical || !category_name || !category_key) {
      return { status: 400, body: JSON.stringify({ error: 'campaign, vertical, category_name, category_key required' }) };
    }

    const { v4: uuidv4 } = require('uuid');
    const pool = await getPool();
    await pool.request()
      .input('id',             sql.UniqueIdentifier, uuidv4())
      .input('campaign',       sql.NVarChar(255),    campaign)
      .input('vertical',       sql.NVarChar(50),     vertical)
      .input('category_name',  sql.NVarChar(255),    category_name)
      .input('category_key',   sql.NVarChar(255),    category_key)
      .input('campaignkit_id', sql.NVarChar(255),    campaignkit_id ?? null)
      .input('enabled',        sql.Bit,              enabled ? 1 : 0)
      .query(`
        MERGE category_mappings AS target
        USING (SELECT @campaign AS campaign, @category_key AS category_key) AS src
          ON target.campaign = src.campaign AND target.category_key = src.category_key
        WHEN MATCHED THEN
          UPDATE SET category_name = @category_name, vertical = @vertical,
                     campaignkit_id = @campaignkit_id, enabled = @enabled,
                     updated_at = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED THEN
          INSERT (id, campaign, vertical, category_name, category_key, campaignkit_id, enabled)
          VALUES (@id, @campaign, @vertical, @category_name, @category_key, @campaignkit_id, @enabled);
      `);

    return { status: 200, body: JSON.stringify({ ok: true }) };
  },
});

// ── GET /mgmt/mts/phone-categories ───────────────────────────────────────────
app.http('mtsPhoneCategoriesList', {
  methods: ['GET'],
  route: 'mgmt/mts/phone-categories',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const phone = req.query.get('phone') ?? null;
    const pool  = await getPool();
    const request = pool.request();
    const where = phone
      ? (request.input('phone', sql.NVarChar(20), phone), 'WHERE phone = @phone')
      : '';

    const result = await request.query(
      `SELECT * FROM phone_categories ${where} ORDER BY created_at DESC`,
    );
    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(result.recordset) };
  },
});

// ── POST /mgmt/mts/phone-categories ──────────────────────────────────────────
app.http('mtsPhoneCategoriesUpsert', {
  methods: ['POST'],
  route: 'mgmt/mts/phone-categories',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    let body;
    try { body = await req.json(); } catch { return { status: 400, body: JSON.stringify({ error: 'invalid json' }) }; }

    const { phone, category_key, source, valid_until } = body;
    if (!phone || !category_key) {
      return { status: 400, body: JSON.stringify({ error: 'phone and category_key required' }) };
    }

    const { v4: uuidv4 } = require('uuid');
    const pool = await getPool();
    await pool.request()
      .input('id',           sql.UniqueIdentifier, uuidv4())
      .input('phone',        sql.NVarChar(20),     phone)
      .input('category_key', sql.NVarChar(255),    category_key)
      .input('source',       sql.NVarChar(50),     source ?? null)
      .input('valid_until',  sql.DateTimeOffset,   valid_until ? new Date(valid_until) : null)
      .query(`
        MERGE phone_categories AS target
        USING (SELECT @phone AS phone, @category_key AS category_key) AS src
          ON target.phone = src.phone AND target.category_key = src.category_key
        WHEN MATCHED THEN
          UPDATE SET source = @source, valid_until = @valid_until
        WHEN NOT MATCHED THEN
          INSERT (id, phone, category_key, source, valid_until)
          VALUES (@id, @phone, @category_key, @source, @valid_until);
      `);

    return { status: 200, body: JSON.stringify({ ok: true }) };
  },
});

// ── POST /mgmt/mts/prune ─────────────────────────────────────────────────────
app.http('mtsPrune', {
  methods: ['POST'],
  route: 'mgmt/mts/prune',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const pool = await getPool();
    const results = {};
    for (const v of ['auto', 'health', 'medicare', 'home']) {
      const r = await pool.request().query(
        `DELETE FROM [mts_${v}] WHERE expires_at < SYSDATETIMEOFFSET()`,
      );
      results[v] = r.rowsAffected[0];
    }

    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify({ pruned: results }) };
  },
});

// ── GET /mgmt/mts/sequence-triggers ──────────────────────────────────────────
app.http('mtsSequenceTriggers', {
  methods: ['GET'],
  route: 'mgmt/mts/sequence-triggers',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const page     = Math.max(1, parseInt(req.query.get('page')  ?? '1',  10));
    const pageSize = Math.min(200, parseInt(req.query.get('size') ?? '50', 10));
    const pingId   = req.query.get('ping_id') ?? null;
    const flow     = req.query.get('flow') ?? null;
    const offset   = (page - 1) * pageSize;

    const pool = await getPool();
    const request = pool.request()
      .input('offset',   sql.Int, offset)
      .input('pageSize', sql.Int, pageSize);

    const conditions = [];
    if (pingId) { conditions.push('ping_id = @pingId'); request.input('pingId', sql.UniqueIdentifier, pingId); }
    if (flow)   { conditions.push('flow = @flow');     request.input('flow',   sql.NVarChar(10), flow); }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await request.query(`
      SELECT id, mts_id, ping_id, phone, vertical, campaign, flow, action,
             was_enrichment_required, was_contacted_30d, was_in_category,
             category_matched, external_status_code, external_latency_ms, triggered_at
      FROM sequence_triggers
      ${where}
      ORDER BY triggered_at DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY
    `);

    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(result.recordset) };
  },
});

// ── GET /mgmt/mts/{id}/evaluate ───────────────────────────────────────────────
// Dry-run the full decision tree for one MTS record without firing anything.
// Shows every evaluation point and what decision would be made.
app.http('mtsEvaluate', {
  methods: ['GET'],
  route: 'mgmt/mts/{id}/evaluate',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    const pool = await getPool();
    const rec  = await pool.request()
      .input('id', sql.UniqueIdentifier, req.params.id)
      .query('SELECT * FROM vw_ping_master WHERE mts_id = @id');

    if (!rec.recordset.length) return { status: 404, body: JSON.stringify({ error: 'not found' }) };
    const r = rec.recordset[0];

    // ── RTB evaluation ────────────────────────────────────────────────────────
    const enrichmentUrl        = await getConfig('enrichment_provider_url');
    const enrichmentConfigured = !!(enrichmentUrl);

    // A phone is enriched if ANY ping for it already has a zip
    const enrichCheck = await pool.request()
      .input('phone', sql.NVarChar(20), r.phone)
      .query('SELECT TOP 1 zip FROM inbound_pings WHERE phone = @phone AND zip IS NOT NULL');
    const hasKnownZip    = enrichCheck.recordset.length > 0;
    const knownZip       = hasKnownZip ? enrichCheck.recordset[0].zip : null;
    const requiresEnrich = enrichmentConfigured && !hasKnownZip;
    const rtbAction      = requiresEnrich ? 'top_of_funnel' : 'ringba_direct';

    // ── SMS evaluation ────────────────────────────────────────────────────────
    const suppressionDays = parseInt(await getConfig('sms_suppression_window_days') ?? '30', 10);
    const cutoff          = new Date(Date.now() - suppressionDays * 86_400_000);

    const contactCheck = await pool.request()
      .input('phone',  sql.NVarChar(20),   r.phone)
      .input('cutoff', sql.DateTimeOffset, cutoff)
      .query(`
        SELECT TOP 1 contacted_at, channel, campaign
        FROM contact_history
        WHERE phone = @phone AND contact_type = 'sms' AND contacted_at >= @cutoff
        ORDER BY contacted_at DESC
      `);
    const contactedRecently = contactCheck.recordset.length > 0;
    const lastContact       = contactCheck.recordset[0] ?? null;

    let smsAction      = 'sms_suppressed';
    let categoryResult = null;

    if (!contactedRecently) {
      const catCheck = await pool.request()
        .input('campaign', sql.NVarChar(255), r.campaign)
        .query(`
          SELECT TOP 1 category_key, campaignkit_id, category_name
          FROM category_mappings
          WHERE campaign = @campaign AND enabled = 1
        `);

      if (catCheck.recordset.length) {
        const cat    = catCheck.recordset[0];
        smsAction    = 'campaignkit_trigger';
        categoryResult = {
          matched:        true,
          category_key:   cat.category_key,
          category_name:  cat.category_name,
          campaignkit_id: cat.campaignkit_id,
          source_url:     cat.category_key,
        };
      } else {
        smsAction      = 'no_category';
        categoryResult = { matched: false };
      }
    }

    const sequencerEnabled = await getConfig('sequencer_enabled');
    const campaignkitUrl   = await getConfig('campaignkit_trigger_url');

    const evaluation = {
      mts_id:   r.mts_id,
      ping_id:  r.ping_id,
      phone:    r.phone,
      campaign: r.campaign,
      vertical: r.vertical,
      sequencer_enabled: sequencerEnabled === '1',
      rtb_flow: {
        decision:                'requires_enrichment',
        enrichment_configured:   enrichmentConfigured,
        enrichment_provider_url: enrichmentUrl || null,
        has_known_zip:           hasKnownZip,
        known_zip:               knownZip,
        current_zip:             r.zip ?? null,
        requires_enrichment:     requiresEnrich,
        reason: requiresEnrich
          ? 'No prior ping for this phone has a zip — enrichment needed'
          : hasKnownZip
            ? `Phone has known zip (${knownZip}) from a prior ping`
            : 'Enrichment provider not configured — skipping enrichment',
        action: rtbAction,
      },
      sms_flow: {
        decision_1: {
          label:              'contacted_last_30d',
          suppression_days:   suppressionDays,
          suppression_cutoff: cutoff.toISOString(),
          contacted_recently: contactedRecently,
          last_contact:       lastContact,
          result:             contactedRecently ? 'SUPPRESSED' : 'PASS',
        },
        decision_2: contactedRecently ? null : {
          label:    'in_category',
          campaign: r.campaign,
          ...categoryResult,
          result:   categoryResult?.matched ? 'TRIGGER' : 'NO_CATEGORY',
        },
        campaignkit_configured: !!(campaignkitUrl),
        action: smsAction,
      },
    };

    return { status: 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() }, body: JSON.stringify(evaluation) };
  },
});

// ── POST /mgmt/mts/{id}/trigger ───────────────────────────────────────────────
// Manually run the sequencer for one MTS record.
// Pass { "force": true } in the body to run even when sequencer_enabled = '0'.
app.http('mtsTrigger', {
  methods: ['POST'],
  route: 'mgmt/mts/{id}/trigger',
  authLevel: 'anonymous',
  handler: async (req) => {
    const authError = requireAdminKey(req); if (authError) return authError;

    let body = {};
    try { body = await req.json(); } catch { /* force defaults to false */ }
    const force = body.force === true;

    const pool = await getPool();
    const rec  = await pool.request()
      .input('id', sql.UniqueIdentifier, req.params.id)
      .query('SELECT mts_id, ping_id, phone, vertical, campaign, ringba_status, ringba_bid_amount FROM vw_ping_master WHERE mts_id = @id');

    if (!rec.recordset.length) return { status: 404, body: JSON.stringify({ error: 'not found' }) };
    const r = rec.recordset[0];

    if (force) {
      const { invalidateConfigCache } = require('../../lib/config-cache');
      await pool.request().query(`UPDATE system_config SET value = '1' WHERE [key] = 'sequencer_enabled'`);
      invalidateConfigCache();
    }

    let triggered = false;
    let error     = null;
    try {
      await runSequencer({
        mtsId:     r.mts_id,
        pingId:    r.ping_id,
        phone:     r.phone,
        vertical:  r.vertical,
        campaign:  r.campaign,
        rtbStatus: r.ringba_status,
        bidAmount: r.ringba_bid_amount,
      });
      triggered = true;
    } catch (err) {
      error = err.message;
    } finally {
      if (force) {
        const { invalidateConfigCache } = require('../../lib/config-cache');
        await pool.request().query(`UPDATE system_config SET value = '0' WHERE [key] = 'sequencer_enabled'`);
        invalidateConfigCache();
      }
    }

    return {
      status: triggered ? 200 : 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders() },
      body: JSON.stringify({ ok: triggered, mts_id: r.mts_id, force, error }),
    };
  },
});
