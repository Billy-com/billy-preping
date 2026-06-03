/**
 * Sequencing engine — runs after every RTB ping lands in mid-term storage.
 * All actions are internal — nothing posts back to the partner endpoint.
 *
 * RTB flow:
 *   requiresEnrichment? → YES → seq_state = 'enrichment_needed' (top of funnel)
 *                       → NO  → seq_state = 'ringba_direct'
 *
 * SMS flow (internal — fires CampaignKit directly, logged to sequence_triggers):
 *   contactedLast30d?   → YES → sms_suppressed (logged, do nothing)
 *                       → NO  → campaign in category_mappings?
 *                                 → YES → CampaignKit trigger (list 92, source_url = category_key)
 *                                 → NO  → no_category (logged, do nothing)
 */
const { getPool, sql } = require('./db');
const { getConfig } = require('./config-cache');
const { lookupPhone } = require('./ffg');
const { v4: uuidv4 } = require('uuid');

async function runSequencer({ mtsId, pingId, phone, vertical, campaign, rtbStatus, bidAmount, ffgSpineId, ffgLineType }) {
  const enabled = await getConfig('sequencer_enabled');
  if (enabled !== '1') return;

  const pool = await getPool();

  // ── RTB FLOW / TOP OF FUNNEL ──────────────────────────────────────────────
  // Step 3: FFG identity enrichment (if not already done in ping.js).
  // In the hot path, FFG runs in ping.js before Ringba. Here in the sequencer
  // (fire-and-forget), we call FFG if we don't already have spine data so that
  // demographic signals are available for SMS routing decisions.
  let demographic = null;
  let spineId = ffgSpineId ?? null;
  const ffgEnabled = await getConfig('ffg_enabled');
  if (ffgEnabled === '1' && !spineId) {
    const customerId = await getConfig('ffg_customer_id');
    const apiKey     = await getConfig('ffg_api_key');
    const sandbox    = (await getConfig('ffg_sandbox')) === '1';
    if (customerId && apiKey) {
      const ffgResult = await lookupPhone({ phone, customerId, apiKey, sandbox }).catch(() => null);
      if (ffgResult) {
        spineId     = ffgResult.spineId;
        demographic = ffgResult.demographic;
      }
    }
  }

  const requiresEnrichment = await checkRequiresEnrichment(pool, phone);
  const rtbAction   = requiresEnrichment ? 'top_of_funnel' : 'ringba_direct';
  const nextState   = requiresEnrichment ? 'enrichment_needed' : 'ringba_direct';

  await updateMtsState(pool, mtsId, vertical, nextState);
  await logTrigger(pool, {
    mtsId, pingId, phone, vertical, campaign,
    flow: 'rtb', action: rtbAction,
    wasEnrichmentRequired: requiresEnrichment ? 1 : 0,
  });

  // ── SMS FLOW ──────────────────────────────────────────────────────────────
  const suppressionDays = parseInt(await getConfig('sms_suppression_window_days') ?? '30', 10);
  const contactedRecently = await checkContactedRecently(pool, phone, suppressionDays);

  if (contactedRecently) {
    await logTrigger(pool, {
      mtsId, pingId, phone, vertical, campaign,
      flow: 'sms', action: 'sms_suppressed',
      wasContactedThirtyDays: 1,
    });
    return;
  }

  const { inCategory, categoryKey, campaignkitId } = await checkInCategories(pool, phone, campaign);

  if (!inCategory) {
    await logTrigger(pool, {
      mtsId, pingId, phone, vertical, campaign,
      flow: 'sms', action: 'no_category',
      wasContactedThirtyDays: 0, wasInCategory: 0,
    });
    return;
  }

  // Fire CampaignKit trigger
  const { statusCode, latencyMs, response } = await fireCampaignKit({ phone, campaign, vertical, categoryKey, campaignkitId });

  // Record contact so future pings are suppressed for 30 days
  await writeContactHistory(pool, { phone, pingId, mtsId, campaign, vertical });

  await logTrigger(pool, {
    mtsId, pingId, phone, vertical, campaign,
    flow: 'sms', action: 'campaignkit_trigger',
    wasContactedThirtyDays: 0, wasInCategory: 1,
    categoryMatched: categoryKey,
    externalStatusCode: statusCode,
    externalResponse: JSON.stringify(response),
    externalLatencyMs: latencyMs,
  });
}

async function checkRequiresEnrichment(pool, phone) {
  const url = await getConfig('enrichment_provider_url');
  if (!url) return false;
  const r = await pool.request()
    .input('phone', sql.NVarChar(20), phone)
    .query('SELECT TOP 1 1 FROM inbound_pings WHERE phone = @phone AND zip IS NOT NULL');
  return r.recordset.length === 0;
}

async function checkContactedRecently(pool, phone, days) {
  const r = await pool.request()
    .input('phone',  sql.NVarChar(20),    phone)
    .input('cutoff', sql.DateTimeOffset,  new Date(Date.now() - days * 86_400_000))
    .query(`
      SELECT TOP 1 1 FROM contact_history
      WHERE phone = @phone AND contact_type = 'sms' AND contacted_at >= @cutoff
    `);
  return r.recordset.length > 0;
}

async function checkInCategories(pool, phone, campaign) {
  // Lookup is campaign-based only — category_mappings is synced from Lovable/Supabase
  // routing table. campaign column = inbound campaign value, category_key = source_url
  // sent to CampaignKit.
  const r = await pool.request()
    .input('campaign', sql.NVarChar(255), campaign)
    .query(`
      SELECT TOP 1 category_key, campaignkit_id
      FROM category_mappings
      WHERE campaign = @campaign AND enabled = 1
    `);
  if (!r.recordset.length) return { inCategory: false };
  return { inCategory: true, categoryKey: r.recordset[0].category_key, campaignkitId: r.recordset[0].campaignkit_id };
}

async function fireCampaignKit({ phone, campaign, vertical, categoryKey, campaignkitId }) {
  const triggerUrl = await getConfig('campaignkit_trigger_url');
  const apiKey     = await getConfig('campaignkit_api_key');
  const listId     = parseInt(await getConfig('campaignkit_list_id') ?? campaignkitId ?? '92', 10);
  if (!triggerUrl) return { statusCode: null, latencyMs: 0, response: null };

  // Ensure phone has country code (CampaignKit expects e.g. 12345678910)
  const formattedPhone = phone.startsWith('1') ? phone : `1${phone}`;

  const start = Date.now();
  try {
    const res = await fetch(triggerUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({ list_id: listId, phone: formattedPhone, source_url: categoryKey }),
    });
    const latencyMs = Date.now() - start;
    let response = null;
    try { response = await res.json(); } catch { /* ok */ }
    return { statusCode: res.status, latencyMs, response };
  } catch (err) {
    console.error('[sequencer] CampaignKit error:', err.message);
    return { statusCode: null, latencyMs: Date.now() - start, response: null };
  }
}

async function updateMtsState(pool, mtsId, vertical, nextState) {
  await pool.request()
    .input('id',    sql.UniqueIdentifier, mtsId)
    .input('state', sql.NVarChar(30),     nextState)
    .query(`UPDATE [mts_${vertical}] SET seq_state = @state WHERE id = @id`);
}

async function writeContactHistory(pool, { phone, pingId, mtsId, campaign, vertical }) {
  await pool.request()
    .input('id',      sql.UniqueIdentifier, uuidv4())
    .input('phone',   sql.NVarChar(20),     phone)
    .input('type',    sql.NVarChar(20),     'sms')
    .input('channel', sql.NVarChar(50),     'campaignkit')
    .input('ping_id', sql.UniqueIdentifier, pingId)
    .input('mts_id',  sql.UniqueIdentifier, mtsId)
    .input('campaign',sql.NVarChar(255),    campaign)
    .input('vertical',sql.NVarChar(50),     vertical)
    .query(`
      INSERT INTO contact_history (id, phone, contact_type, channel, ping_id, mts_id, campaign, vertical)
      VALUES (@id, @phone, @type, @channel, @ping_id, @mts_id, @campaign, @vertical)
    `);
}

async function logTrigger(pool, {
  mtsId, pingId, phone, vertical, campaign, flow, action,
  wasEnrichmentRequired = null, wasContactedThirtyDays = null, wasInCategory = null,
  categoryMatched = null, externalStatusCode = null, externalResponse = null, externalLatencyMs = null,
}) {
  await pool.request()
    .input('id',       sql.UniqueIdentifier,  uuidv4())
    .input('mts_id',   sql.UniqueIdentifier,  mtsId)
    .input('ping_id',  sql.UniqueIdentifier,  pingId)
    .input('phone',    sql.NVarChar(20),       phone)
    .input('vertical', sql.NVarChar(50),       vertical)
    .input('campaign', sql.NVarChar(255),      campaign)
    .input('flow',     sql.NVarChar(10),       flow)
    .input('action',   sql.NVarChar(50),       action)
    .input('enrich',   sql.Bit,                wasEnrichmentRequired)
    .input('contacted',sql.Bit,                wasContactedThirtyDays)
    .input('incat',    sql.Bit,                wasInCategory)
    .input('cat',      sql.NVarChar(255),      categoryMatched)
    .input('ext_code', sql.Int,                externalStatusCode)
    .input('ext_resp', sql.NVarChar(sql.MAX),  externalResponse)
    .input('ext_ms',   sql.Int,                externalLatencyMs)
    .query(`
      INSERT INTO sequence_triggers
        (id, mts_id, ping_id, phone, vertical, campaign, flow, action,
         was_enrichment_required, was_contacted_30d, was_in_category,
         category_matched, external_status_code, external_response, external_latency_ms)
      VALUES
        (@id, @mts_id, @ping_id, @phone, @vertical, @campaign, @flow, @action,
         @enrich, @contacted, @incat, @cat, @ext_code, @ext_resp, @ext_ms)
    `);
}

module.exports = { runSequencer };
