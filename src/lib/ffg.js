/**
 * Fast Facts Group (FFG) identity spine lookup.
 *
 * Billy pipeline step 3 — fires BEFORE the Ringba RTB bid.
 * Validates phone line type (mobile vs landline/VoIP) so we don't
 * waste Ringba bid budget on unroutable numbers, and attaches
 * demographic signals for SMS targeting.
 *
 * Fail-open: any error (timeout, API down, no data) lets the ping
 * continue to Ringba — we never block revenue on an enrichment hiccup.
 *
 * Endpoints:
 *   /spine/lookup         — direct lookup (billed to Billy account)
 *   /partner/spine/lookup — partner lookup (requires afid, billed to affiliate)
 *
 * Auth: x-customer-id + x-api-key headers
 * Config keys (system_config): ffg_enabled, ffg_customer_id, ffg_api_key,
 *                               ffg_sandbox, ffg_timeout_ms
 */

const FFG_BASE        = 'https://data.fastfactsgroup.com';
const DEFAULT_TIMEOUT = 2000; // 2s — well within our response budget

// Phone line types that should never route to a live buyer.
const BLOCKED_LINE_TYPES = new Set(['landline', 'voip', 'invalid', 'tollfree', 'pager', 'payphone']);

/**
 * Look up a phone number against the FFG identity spine.
 *
 * @param {Object} opts
 * @param {string}  opts.phone       — E.164 or 10-digit phone
 * @param {string}  opts.customerId  — FFG customer UUID
 * @param {string}  opts.apiKey      — FFG API key
 * @param {boolean} [opts.sandbox]   — if true, use fixture data (not billed)
 * @param {number}  [opts.timeoutMs] — hard cap in ms (default 2000)
 * @param {string}  [opts.afid]      — affiliate ID for /partner/spine/lookup
 *
 * @returns {{ pass: boolean, reason: string, lineType: string|null,
 *             spineId: string|null, demographic: object|null, latencyMs: number,
 *             rawResponse: object|null }}
 */
async function lookupPhone({ phone, customerId, apiKey, sandbox = false, timeoutMs = DEFAULT_TIMEOUT, afid = null }) {
  const start      = Date.now();
  const controller = new AbortController();
  const timer      = setTimeout(() => controller.abort(), timeoutMs);

  const endpoint = afid ? '/partner/spine/lookup' : '/spine/lookup';

  try {
    const reqBody = {
      phone,
      domains:     ['phone', 'demographic'],
      latest:      true,
      mobile_only: false, // ask for everything; we gate on line_type ourselves
      ...(sandbox ? { sandbox: true } : {}),
      ...(afid    ? { afid }         : {}),
    };

    const res = await fetch(`${FFG_BASE}${endpoint}`, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'x-customer-id': customerId,
        'x-api-key':     apiKey,
      },
      body:   JSON.stringify(reqBody),
      signal: controller.signal,
    });

    const latencyMs = Date.now() - start;

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      console.warn(`[FFG] HTTP ${res.status} — fail-open: ${text.slice(0, 200)}`);
      return { pass: true, reason: `FFG HTTP ${res.status} — fail-open`, lineType: null, spineId: null, demographic: null, latencyMs, rawResponse: null };
    }

    const body    = await res.json();
    const spineId = body.spine_id ?? null;
    const domains = body.domains ?? {};

    // ── Phone domain ─────────────────────────────────────────────────────────
    const phoneDomain = domains.phone ?? null;

    if (!phoneDomain || phoneDomain.status !== 'found') {
      // No data — fail open (never block an unknown number)
      return { pass: true, reason: 'FFG: phone not in spine — fail-open', lineType: null, spineId, demographic: null, latencyMs, rawResponse: body };
    }

    // Phone data may be an array (multiple records) or a single object.
    // We asked for latest=true so the API returns the best record.
    const phoneRecords  = Array.isArray(phoneDomain.data) ? phoneDomain.data : (phoneDomain.data ? [phoneDomain.data] : []);
    const phoneRecord   = phoneRecords[0] ?? {};
    const lineType      = (phoneRecord.line_type ?? phoneRecord.lineType ?? phoneRecord.type ?? '').toLowerCase();

    if (BLOCKED_LINE_TYPES.has(lineType)) {
      console.log(`[FFG] Rejected ${phone.slice(-4)}: ${lineType} (${latencyMs}ms)`);
      return { pass: false, reason: `Phone type rejected: ${lineType}`, lineType, spineId, demographic: null, latencyMs, rawResponse: body };
    }

    // ── Demographic domain ───────────────────────────────────────────────────
    const demoDomain   = domains.demographic ?? null;
    const demoRecords  = (demoDomain?.status === 'found')
      ? (Array.isArray(demoDomain.data) ? demoDomain.data : (demoDomain.data ? [demoDomain.data] : []))
      : [];
    const demographic  = demoRecords[0] ?? null;

    console.log(`[FFG] ${phone.slice(-4)} → ${lineType || 'unknown'} | spine=${spineId?.slice(0, 8) ?? '?'} (${latencyMs}ms)`);

    return { pass: true, reason: `Mobile confirmed: ${lineType || 'cell'}`, lineType, spineId, demographic, latencyMs, rawResponse: body };

  } catch (err) {
    const latencyMs = Date.now() - start;
    const isTimeout = err.name === 'AbortError';
    const reason    = isTimeout ? `FFG timeout (${timeoutMs}ms) — fail-open` : `FFG error (fail-open): ${err.message}`;
    console.warn(`[FFG] ${reason}`);
    return { pass: true, reason, lineType: null, spineId: null, demographic: null, latencyMs, rawResponse: null };
  } finally {
    clearTimeout(timer);
  }
}

module.exports = { lookupPhone, BLOCKED_LINE_TYPES };
