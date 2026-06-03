const ENRICH_TIMEOUT_MS = 1500; // 1.5s — sufficient for ip-api.com, well within fanout budget

/**
 * Look up zip code from caller IP via ip-api.com.
 * Hard 10ms timeout — returns null on timeout or any error.
 * No API key required.
 */
async function enrichZipFromIp(ip) {
  if (!ip) return null;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ENRICH_TIMEOUT_MS);

  try {
    const res = await fetch(
      `http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,zip`,
      { signal: controller.signal }
    );
    const data = await res.json();
    if (data.status === 'success' && data.zip) {
      return String(data.zip).replace(/\D/g, '').slice(0, 5) || null;
    }
    return null;
  } catch {
    return null; // timeout or network error — continue without zip
  } finally {
    clearTimeout(timer);
  }
}

module.exports = { enrichZipFromIp };
