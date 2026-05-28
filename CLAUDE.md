# Billy Pre-Ping — Claude Context

> Azure Functions RTB pre-ping pipeline for Billy.com.
> Built and maintained by Flaming Owl Consulting (Grant Jones, Interim CTO).

---

## What This Is

Real-time bidding (RTB) pre-ping pipeline deployed as Azure Functions at `https://api.billy.com/rtb`.

Publishers send pings → pipeline evaluates lead quality → bids via Ringba → triggers SMS via CampaignKit → stores all data for LTS analytics.

---

## Live Endpoints

| Base | `https://api.billy.com/rtb` |
|---|---|
| Auth | `x-admin-key: Billy!Admin2026#` |
| Azure Function App | `func-billy-prepng` |
| Azure SQL Server | `sqlsrv-billy-dev.database.windows.net` |
| Azure SQL DB | `sqldb-billy-prepng` |

---

## Data Flow

```
Publisher ping
    │
    ▼
inbound_pings (Azure SQL)                   ← every ping logged
    │
    ├──► Ringba RTB bid ──► ringba_responses
    │
    ├──► mts_auto / mts_health / mts_medicare / mts_home
    │         (mid-term storage — raw lead data by vertical)
    │
    └──► sequence_triggers ──► CampaignKit SMS enrollment
                    │
         Nightly LTS (02:00 UTC)
                    │
         lt_daily_stats
         lt_publisher_quality
         lt_phone_profile
         lt_geo_intelligence
         lt_lead_scores
         lt_outcome_events
```

---

## Azure SQL Tables

| Table | Purpose |
|---|---|
| `inbound_pings` | Every RTB ping received (676K+ rows) |
| `ringba_responses` | Ringba bid responses |
| `mts_auto` | Mid-term storage — auto vertical |
| `mts_health` | Mid-term storage — health vertical |
| `mts_medicare` | Mid-term storage — medicare vertical |
| `mts_home` | Mid-term storage — home vertical |
| `sequence_triggers` | SMS routing decisions |
| `phone_categories` | DNC / category flags |
| `lt_daily_stats` | LTS: daily rollup |
| `lt_publisher_quality` | LTS: lifetime publisher scores |
| `lt_phone_profile` | LTS: per-phone analytics |
| `lt_geo_intelligence` | LTS: zip-level performance |
| `lt_lead_scores` | LTS: lead scoring model |
| `lt_outcome_events` | LTS: postback + SMS delta log |
| `vw_ping_master` | View joining all pipeline tables |

> **Capacity:** Azure SQL Basic (2 GB) ~99.9% full.
> Run `POST /mgmt/db/trim` to NULL out old `raw_payload` JSON and reclaim quota.
> Long-term: upgrade to Standard S2 in Azure portal.

---

## Source Structure

```
src/functions/admin/
  lts-aggregation.js     — Nightly LTS + manual trigger + migrations
  lts-api.js             — 6 read-only LTS GET endpoints
  middleware.js          — Admin key auth
src/lib/
  db.js                  — Azure SQL pool (120s timeout, max 5 connections)
migrations/
  010_catchup.sql        — Schema migrations
scripts/
  migrate.js             — Migration runner (handles GO separators)
```

---

## Admin API Reference

### Stats & Config
```bash
GET  /mgmt/stats?period=7d          # pipeline stats
GET  /mgmt/config                   # current config
POST /mgmt/config                   # update config
```

### LTS (Long-Term Storage Analytics)
```bash
GET  /mgmt/lts/daily-stats          # daily aggregated KPIs
GET  /mgmt/lts/publisher-quality    # lifetime publisher scores
GET  /mgmt/lts/phone-profiles       # per-phone analytics
GET  /mgmt/lts/geo-intelligence     # zip-level performance
GET  /mgmt/lts/lead-scores          # hot/warm/cold/dormant
GET  /mgmt/lts/outcome-events       # postback + SMS delta log
POST /mgmt/lts/run                  # manual aggregation {"days": N}
POST /mgmt/lts/migrate              # run catchup SQL migrations via live pool
```

### Database Ops
```bash
GET  /mgmt/db/storage               # storage diagnostic
POST /mgmt/db/trim                  # NULL out old raw_payload (reclaim quota)
POST /mgmt/db/cleanup               # delete expired MTS rows
```

### Pipeline / Pings
```bash
GET  /mgmt/pings                    # recent pings (limit, campaign, publisher_id, won, since)
POST /mgmt/pings/:id/replay         # replay a specific ping
GET  /mgmt/pipeline/postback-log    # postback events
```

### RTB Mappings
```bash
GET  /mgmt/rtb-mappings             # publisher × campaign × CK source URL
POST /mgmt/rtb-mappings/sync        # update mappings
```

### Fanout Endpoints
```bash
GET    /mgmt/fanout-endpoints
POST   /mgmt/fanout-endpoints
PATCH  /mgmt/fanout-endpoints/:id
DELETE /mgmt/fanout-endpoints/:id
```

### DNC
```bash
GET    /mgmt/dnc
POST   /mgmt/dnc              {"phone": "+15551234567"}
DELETE /mgmt/dnc/:phone
```

### SMS
```bash
GET  /mgmt/sms/batch/preview        # preview batch (limit param)
POST /mgmt/sms/batch                # run batch {"limit": 500, "dry_run": false}
POST /mgmt/sms/trigger              # single SMS {"ping_id", "phone", "campaign"}
```

---

## MCP Access (via perf-marketing-mcp)

Claude can call all admin endpoints directly through the billy.cjs gateway:

```js
await billy.getStats({ period: "7d" });
await billy.runLtsAggregation({ days: 7 });
await billy.trimDatabase({ keep_days: 30 });
await billy.getPings({ limit: 100, campaign: "flights" });
await billy.getLtsPublisherQuality({ limit: 50 });
await billy.getDnc();
await billy.addDnc("+15551234567");
await billy.previewSmsBatch({ limit: 200 });
await billy.runSmsBatch({ limit: 500, dryRun: false });
```

Gateway path: `~/perf-marketing-mcp/gateway/billy.cjs`

---

## Deployment

```bash
# Deploy Azure Functions
func azure functionapp publish func-billy-prepng

# Run migrations
node scripts/migrate.js

# Seed LTS (after DB trim)
curl -X POST https://api.billy.com/rtb/mgmt/lts/run \
  -H "x-admin-key: Billy!Admin2026#" \
  -H "Content-Type: application/json" \
  -d '{"days": 30}'
```

---

## Open Tasks

- [ ] **DB trim**: `POST /mgmt/db/trim` — running in background, frees raw_payload space
- [ ] **LTS seed**: After trim → `POST /mgmt/lts/run {"days":7}` → `{"days":30}` → no limit
- [ ] **CampaignKit**: Enter `CAMPAIGNKIT_API_TOKEN` — token stored in Azure Function App config
- [ ] **Azure upgrade**: When trim isn't enough → Standard S2 in Azure portal

---

## Key Alerts

- Azure SQL `inbound_pings` at 676K+ rows — primary storage consumer
- `mts_*` tables accumulate raw JSON — `POST /mgmt/db/cleanup` removes expired rows
- LTS aggregation runs nightly at 02:00 UTC — if Strategy tab is empty, run manually
- Admin key is in env only — never commit `Billy!Admin2026#` to source code
