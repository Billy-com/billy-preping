const { app } = require('@azure/functions');
const { v4: uuidv4 } = require('uuid');
const { getPool, sql } = require('../lib/db');
const { normalizePhone, normalizeZip } = require('../lib/normalize');
const { forwardToRingba } = require('../lib/ringba');
const { fanout } = require('../lib/fanout');
const { getConfig, isPublisherEnabled } = require('../lib/config-cache');
const { writeMidTermStorage } = require('../lib/mts');

// Pre-warm DB pool and config cache on module load so the first real ping
// doesn't pay the connection + cache-miss cost on the critical path.
Promise.all([
  getPool(),
  getConfig('ringba_rtb_url'),
]).catch(err => console.error('[ping] pre-warm failed:', err.message));

// ── /api/ping/{rtbId} — RTB-ID-routed ping ───────────────────────────────────
app.http('pingByRtbId', {
  methods: ['POST', 'GET'],
  route: 'ping/{rtbId}',
  authLevel: 'anonymous',
  handler: async (request, context) => {
    const receivedAt = Date.now();
    const rtbId  = request.params.rtbId;
    const rtbUrl = `https://rtb.ringba.com/v1/production/${rtbId}.json`;

    let body = {};
    if (request.method === 'POST') {
      try { body = await request.json(); } catch {
        return { status: 400, body: JSON.stringify({ error: 'invalid json' }) };
      }
    } else {
      request.query.forEach((val, key) => { body[key] = val; });
    }

    const rawPhone     = body.phone ?? body.CID;
    const rawZip       = body.zip   ?? body.zipcode;
    const publisher_id = body.publisher_id ?? body.source ?? rtbId;
    const subid        = body.subid ?? body.source ?? null;
    const campaign     = body.campaign ?? rtbId;
    const ip           = body.ip ?? null;

    // Publisher enabled gate — checked against Azure SQL cache (60s TTL).
    // Return a no-bid 200 so publishers don't flag the integration as broken.
    if (!(await isPublisherEnabled(publisher_id))) {
      return {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ won: false, bid: 0, reason: 'publisher_disabled' }),
      };
    }

    if (!rawPhone) {
      return { status: 400, body: JSON.stringify({ error: 'phone or CID is required' }) };
    }

    const phone = normalizePhone(rawPhone);
    if (!phone) {
      return { status: 400, body: JSON.stringify({ error: 'invalid phone number' }) };
    }

    const zip        = normalizeZip(rawZip);
    const zipSource  = zip ? 'publisher' : null;
    const preRingba  = Date.now();

    const pingPayload = { phone, zip, publisher_id, subid, campaign, ip };
    const { status: ringbaStatus, statusCode, outboundPayload, response: ringbaResponse, responseTimeMs } =
      await forwardToRingba(pingPayload, rtbUrl);

    const totalMs    = Date.now() - receivedAt;
    const overheadMs = totalMs - (responseTimeMs ?? 0); // our cost excluding Ringba

    const pingId = uuidv4();
    const won    = ringbaResponse?.won ?? false;
    const responseBody = ringbaResponse ? JSON.stringify(ringbaResponse) : '{}';

    // All DB work is fire-and-forget — response returns immediately after Ringba
    writePingToDb({
      pingId, phone, zip, zipSource, publisher_id, subid, campaign, ip,
      rawPayload: body, ringbaResponse, ringbaStatus, statusCode, outboundPayload, responseTimeMs, won,
    }).catch((err) => console.error('[ping/rtbId] db write failed:', err.message));

    writeMidTermStorage({
      pingId, phone, zip, publisher_id, subid, campaign,
      ringbaStatus, won,
      bidAmount:     ringbaResponse?.bidAmount ?? ringbaResponse?.bid_amount ?? null,
      buyerId:       ringbaResponse?.buyerId   ?? ringbaResponse?.buyer_id   ?? null,
      routingNumber: ringbaResponse?.phoneNumber ?? ringbaResponse?.phoneNumberNoPlus ?? ringbaResponse?.routingNumber ?? ringbaResponse?.routing_number ?? null,
    }).catch((err) => console.error('[ping/rtbId] mts write failed:', err.message));

    fanout({
      ping_id: pingId, phone, zip, zip_source: zipSource, publisher_id, subid, campaign, ip,
      won, bid_amount: ringbaResponse?.bid_amount ?? null, buyer_id: ringbaResponse?.buyer_id ?? null,
    }).catch((err) => console.error('[ping/rtbId] fanout failed:', err.message));

    return {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'x-billy-overhead-ms': String(overheadMs),
        'x-ringba-latency-ms': String(responseTimeMs ?? 0),
        'x-total-ms':          String(totalMs),
      },
      body: responseBody,
    };
  },
});

// ── /api/ping — generic ping ──────────────────────────────────────────────────
app.http('ping', {
  methods: ['POST', 'GET'],
  route: 'ping',
  authLevel: 'anonymous',
  handler: async (request, context) => {
    const receivedAt = Date.now();

    let body = {};
    if (request.method === 'POST') {
      try { body = await request.json(); } catch {
        return { status: 400, body: JSON.stringify({ error: 'invalid json' }) };
      }
    } else {
      request.query.forEach((val, key) => { body[key] = val; });
    }

    // Accept Ringba RTB field aliases: CID → phone, zipcode → zip, source → subid
    const rawPhone     = body.phone ?? body.CID;
    const rawZip       = body.zip   ?? body.zipcode;
    const publisher_id = body.publisher_id ?? body.source ?? null;
    const subid        = body.subid ?? body.source ?? null;
    const campaign     = body.campaign ?? null;
    const ip           = body.ip ?? null;

    // Publisher enabled gate — same 60s-cached check as the rtbId handler
    if (!(await isPublisherEnabled(publisher_id))) {
      return {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ won: false, bid: 0, reason: 'publisher_disabled' }),
      };
    }

    // Only phone/CID is required — all other field requirements are enforced at the UI level
    if (!rawPhone) {
      return { status: 400, body: JSON.stringify({ error: 'phone or CID is required' }) };
    }

    const phone = normalizePhone(rawPhone);
    if (!phone) {
      return { status: 400, body: JSON.stringify({ error: 'invalid phone number' }) };
    }

    const zip       = normalizeZip(rawZip);
    const zipSource = zip ? 'publisher' : null;

    // getConfig is served from in-memory cache after first call — no DB hit on hot path
    const rtbUrl      = await getConfig('ringba_rtb_url');
    const pingPayload = { phone, zip, publisher_id, subid, campaign, ip };
    const { status: ringbaStatus, statusCode, outboundPayload, response: ringbaResponse, responseTimeMs } =
      await forwardToRingba(pingPayload, rtbUrl);

    const totalMs    = Date.now() - receivedAt;
    const overheadMs = totalMs - (responseTimeMs ?? 0);

    const pingId = uuidv4();
    const won    = ringbaResponse?.won ?? false;
    const responseBody = ringbaResponse ? JSON.stringify(ringbaResponse) : '{}';

    // All DB work is fire-and-forget — response returns immediately after Ringba
    writePingToDb({
      pingId, phone, zip, zipSource, publisher_id, subid, campaign, ip,
      rawPayload: body, ringbaResponse, ringbaStatus, statusCode, outboundPayload, responseTimeMs, won,
    }).catch((err) => console.error('[ping] db write failed:', err.message));

    writeMidTermStorage({
      pingId, phone, zip, publisher_id, subid, campaign,
      ringbaStatus, won,
      bidAmount:     ringbaResponse?.bidAmount ?? ringbaResponse?.bid_amount ?? null,
      buyerId:       ringbaResponse?.buyerId   ?? ringbaResponse?.buyer_id   ?? null,
      routingNumber: ringbaResponse?.phoneNumber ?? ringbaResponse?.phoneNumberNoPlus ?? ringbaResponse?.routingNumber ?? ringbaResponse?.routing_number ?? null,
    }).catch((err) => console.error('[ping] mts write failed:', err.message));

    fanout({
      ping_id: pingId, phone, zip, zip_source: zipSource, publisher_id, subid, campaign, ip,
      won, bid_amount: ringbaResponse?.bid_amount ?? null, buyer_id: ringbaResponse?.buyer_id ?? null,
    }).catch((err) => console.error('[ping] fanout failed:', err.message));

    return {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'x-billy-overhead-ms': String(overheadMs),
        'x-ringba-latency-ms': String(responseTimeMs ?? 0),
        'x-total-ms':          String(totalMs),
      },
      body: responseBody,
    };
  },
});

async function writePingToDb({
  pingId, phone, zip, zipSource, publisher_id, subid, campaign, ip,
  rawPayload, ringbaResponse, ringbaStatus, statusCode, outboundPayload, responseTimeMs, won,
}) {
  const pool = await getPool();

  await pool.request()
    .input('id',           sql.UniqueIdentifier,  pingId)
    .input('phone',        sql.NVarChar(20),       phone)
    .input('zip',          sql.NVarChar(10),       zip ?? null)
    .input('zip_source',   sql.NVarChar(20),       zipSource ?? null)
    .input('publisher_id', sql.NVarChar(255),      publisher_id)
    .input('subid',        sql.NVarChar(255),      subid ?? null)
    .input('campaign',     sql.NVarChar(255),      campaign)
    .input('ip',           sql.NVarChar(50),       ip ?? null)
    .input('is_duplicate', sql.Bit,                0)
    .input('raw_payload',  sql.NVarChar(sql.MAX),  JSON.stringify(rawPayload))
    .query(`
      INSERT INTO inbound_pings
        (id, phone, zip, zip_source, publisher_id, subid, campaign, ip, is_duplicate, raw_payload)
      VALUES
        (@id, @phone, @zip, @zip_source, @publisher_id, @subid, @campaign, @ip, @is_duplicate, @raw_payload)
    `);

  // Always write ringba_responses — captures full OUT journey for every ping
  const responseId = uuidv4();
  await pool.request()
    .input('id',                 sql.UniqueIdentifier,  responseId)
    .input('ping_id',            sql.UniqueIdentifier,  pingId)
    .input('bid_amount',         sql.Decimal(10, 4),    ringbaResponse?.bidAmount ?? ringbaResponse?.bid_amount ?? null)
    .input('buyer_id',           sql.NVarChar(255),     ringbaResponse?.buyerId ?? ringbaResponse?.buyer_id ?? null)
    // Ringba RTB returns routing number as phoneNumber (not routingNumber)
    .input('routing_number',     sql.NVarChar(50),      ringbaResponse?.phoneNumber ?? ringbaResponse?.phoneNumberNoPlus ?? ringbaResponse?.routingNumber ?? ringbaResponse?.routing_number ?? null)
    .input('won',                sql.Bit,               won ? 1 : 0)
    .input('response_time_ms',   sql.Int,               responseTimeMs ?? null)
    .input('raw_response',       sql.NVarChar(sql.MAX), ringbaResponse ? JSON.stringify(ringbaResponse) : null)
    .input('ringba_status',      sql.NVarChar(20),      ringbaStatus ?? 'no_bid')
    .input('ringba_status_code', sql.Int,               statusCode ?? null)
    .input('outbound_payload',   sql.NVarChar(sql.MAX), outboundPayload ? JSON.stringify(outboundPayload) : null)
    .query(`
      INSERT INTO ringba_responses
        (id, ping_id, bid_amount, buyer_id, routing_number, won, response_time_ms,
         raw_response, ringba_status, ringba_status_code, outbound_payload)
      VALUES
        (@id, @ping_id, @bid_amount, @buyer_id, @routing_number, @won, @response_time_ms,
         @raw_response, @ringba_status, @ringba_status_code, @outbound_payload)
    `);
}
