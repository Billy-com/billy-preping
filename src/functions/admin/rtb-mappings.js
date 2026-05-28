const { app } = require('@azure/functions');
const { getPool, sql } = require('../../lib/db');
const { requireAdminKey, corsHeaders } = require('./middleware');
const { invalidatePublisherMappingsCache } = require('../../lib/config-cache');

const BILLY_BASE = 'https://func-billy-prepng.azurewebsites.net/api';
const RINGBA_BASE = 'https://rtb.ringba.com/v2';

function buildUrls(rtbId) {
  return {
    billy_url:  `${BILLY_BASE}/ping/${rtbId}`,
    ringba_url: `https://rtb.ringba.com/v1/production/${rtbId}.json`,
  };
}

// GET /mgmt/rtb-mappings
app.http('adminRtbMappingsList', {
  methods: ['GET', 'OPTIONS'],
  route: 'mgmt/rtb-mappings',
  authLevel: 'anonymous',
  handler: async (request, context) => {
    if (request.method === 'OPTIONS') return { status: 204, headers: corsHeaders() };
    const authError = requireAdminKey(request);
    if (authError) return authError;

    const campaign = request.query.get('campaign');
    const pool = await getPool();
    const req = pool.request();
    let where = 'WHERE 1=1';
    if (campaign) {
      where += ' AND m.campaign = @campaign';
      req.input('campaign', sql.NVarChar(255), campaign);
    }

    const result = await req.query(`
      SELECT
        m.publisher_id, m.publisher_name, m.rtb_id, m.campaign, m.campaign_id,
        m.enabled, m.synced_at, m.last_ping_at, m.created_at,
        COUNT(p.id)                  AS total_pings,
        MAX(p.created_at)            AS last_seen_at
      FROM publisher_rtb_mappings m
      LEFT JOIN inbound_pings p ON p.publisher_id = m.publisher_id
      ${where}
      GROUP BY m.publisher_id, m.publisher_name, m.rtb_id, m.campaign, m.campaign_id,
               m.enabled, m.synced_at, m.last_ping_at, m.created_at
      ORDER BY m.campaign, m.publisher_name
    `);

    const rows = result.recordset.map(r => ({
      ...r,
      ...buildUrls(r.rtb_id),
    }));

    return {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders() },
      body: JSON.stringify(rows),
    };
  },
});

// POST /mgmt/rtb-mappings/sync
// Lovable calls this daily with the full mapping array.
// Body: Array of { publisher_id, publisher_name, rtb_id, campaign, campaign_id, enabled }
app.http('adminRtbMappingsSync', {
  methods: ['POST', 'OPTIONS'],
  route: 'mgmt/rtb-mappings/sync',
  authLevel: 'anonymous',
  handler: async (request, context) => {
    if (request.method === 'OPTIONS') return { status: 204, headers: corsHeaders() };
    const authError = requireAdminKey(request);
    if (authError) return authError;

    let mappings;
    try {
      mappings = await request.json();
    } catch {
      return { status: 400, headers: corsHeaders(), body: JSON.stringify({ error: 'invalid json' }) };
    }

    if (!Array.isArray(mappings) || mappings.length === 0) {
      return { status: 400, headers: corsHeaders(), body: JSON.stringify({ error: 'expected non-empty array' }) };
    }

    const pool = await getPool();
    let upserted = 0;

    for (const m of mappings) {
      if (!m.publisher_id || !m.rtb_id) continue;
      await pool.request()
        .input('publisher_id',   sql.NVarChar(255), m.publisher_id)
        .input('publisher_name', sql.NVarChar(255), m.publisher_name ?? null)
        .input('rtb_id',         sql.NVarChar(255), m.rtb_id)
        .input('campaign',       sql.NVarChar(255), m.campaign ?? null)
        .input('campaign_id',    sql.NVarChar(255), m.campaign_id ?? null)
        .input('enabled',        sql.Bit,           m.enabled !== false ? 1 : 0)
        .query(`
          MERGE publisher_rtb_mappings AS target
          USING (SELECT @publisher_id AS publisher_id) AS src ON target.publisher_id = src.publisher_id
          WHEN MATCHED THEN UPDATE SET
            publisher_name = @publisher_name,
            rtb_id         = @rtb_id,
            campaign       = @campaign,
            campaign_id    = @campaign_id,
            enabled        = @enabled,
            synced_at      = SYSDATETIMEOFFSET()
          WHEN NOT MATCHED THEN INSERT
            (publisher_id, publisher_name, rtb_id, campaign, campaign_id, enabled)
          VALUES
            (@publisher_id, @publisher_name, @rtb_id, @campaign, @campaign_id, @enabled);
        `);
      upserted++;
    }

    invalidatePublisherMappingsCache();

    return {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders() },
      body: JSON.stringify({ synced: upserted, synced_at: new Date().toISOString() }),
    };
  },
});

// PATCH /mgmt/rtb-mappings/:publisher_id
// Instant single-field update — UI calls this directly on every toggle.
// Body: { enabled?: boolean, sms_enabled?: boolean }
// Invalidates the in-memory publisher cache so the next ping sees the change within 1s.
app.http('adminRtbMappingsPatch', {
  methods: ['PATCH', 'OPTIONS'],
  route: 'mgmt/rtb-mappings/{publisher_id}',
  authLevel: 'anonymous',
  handler: async (request, context) => {
    if (request.method === 'OPTIONS') return { status: 204, headers: corsHeaders() };
    const authError = requireAdminKey(request);
    if (authError) return authError;

    const publisher_id = request.params.publisher_id;
    if (!publisher_id) {
      return {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders() },
        body: JSON.stringify({ error: 'publisher_id is required' }),
      };
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders() },
        body: JSON.stringify({ error: 'invalid json' }),
      };
    }

    // Build SET clause dynamically — only patch fields that were supplied
    const updates = [];
    const req = (await getPool()).request().input('publisher_id', sql.NVarChar(255), publisher_id);

    if (typeof body.enabled === 'boolean') {
      updates.push('enabled = @enabled');
      req.input('enabled', sql.Bit, body.enabled ? 1 : 0);
    }
    if (typeof body.sms_enabled === 'boolean') {
      updates.push('sms_enabled = @sms_enabled');
      req.input('sms_enabled', sql.Bit, body.sms_enabled ? 1 : 0);
    }

    if (updates.length === 0) {
      return {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders() },
        body: JSON.stringify({ error: 'no valid fields to update (supported: enabled, sms_enabled)' }),
      };
    }

    updates.push('synced_at = SYSDATETIMEOFFSET()');
    await req.query(
      `UPDATE publisher_rtb_mappings SET ${updates.join(', ')} WHERE publisher_id = @publisher_id`,
    );

    // Immediately bust cache so the next ping reflects the new state
    invalidatePublisherMappingsCache();

    return {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders() },
      body: JSON.stringify({ ok: true, publisher_id, updated: Object.keys(body), updated_at: new Date().toISOString() }),
    };
  },
});
