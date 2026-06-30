# Tutorial 05: Exploring the REST API

**Goal:** Tour every endpoint via the interactive Swagger UI, understand the auth model, and call the API directly with curl.

**Time:** ~30 minutes

**Prerequisites:** Tutorial 01 complete — stack running.

---

## The interactive docs

Open `http://localhost:8000/docs` — this is Swagger UI, auto-generated from the FastAPI code.

Every endpoint is documented with its request schema, response schema, and HTTP status codes. You can call them directly from the browser using the **Try it out** button.

---

## Authentication in dev mode

The platform uses identity middleware that supports two modes:

**Dev mode** (current): Pass a custom header `X-Authenticated-User` with the format:

```
X-Authenticated-User: <username>:<ROLE>:<REGION>
```

Available roles:
| Role | Can do |
|------|--------|
| `ADMIN` | Everything |
| `ENGINEER` | Submit workflows, approve HITL |
| `NOC_OPERATOR` | Read-only — status and list only |
| `SERVICE_ACCOUNT` | Webhook ingestion only |

**Production mode**: The header is ignored; a JWT from Apigee is validated instead (currently a `NotImplementedError` stub — Apigee provides the token in the real deployment).

For all exercises below, use:

```
X-Authenticated-User: engineer:ENGINEER:SOUTH
```

---

## Endpoint map

```
GET  /health                              — liveness check (no auth required)
GET  /metrics                             — Prometheus metrics scrape endpoint

POST /v1/devices/{device_id}/bootstrap    — Day 0: trigger bootstrap
POST /v1/devices/{device_id}/provision    — Day 1: trigger provisioning

POST /v1/sites/{site_id}/scan             — Day 2: compliance scan

POST /v1/webhooks/nautobot                — Nautobot webhook receiver (HMAC verified)

GET  /v1/workflows                        — list recent workflows
GET  /v1/workflows/{workflow_id}          — get status of one workflow
POST /v1/workflows/{workflow_id}/approve  — send HITL decision signal

POST /v1/onboarding/bulk                  — Day 0.5: bulk site onboarding
POST /v1/onboarding/sites/{site_id}       — Day 0.5: single site onboarding
GET  /v1/onboarding/status                — bulk onboarding progress counts
```

---

## Walk through each endpoint

### Health check

```bash
curl http://localhost:8000/health
```

No auth header needed. Returns `{"status": "ok"}`. This is what a load balancer health probe calls.

---

### Trigger Day 0 bootstrap

```bash
curl -s -X POST http://localhost:8000/v1/devices/DEV001/bootstrap \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" \
  -d '{"requested_by": "tutorial"}' | python3 -m json.tool
```

Response:

```json
{
  "workflow_id": "day0-DEV001",
  "status_url": "/v1/workflows/day0-DEV001"
}
```

The Day 0 workflow runs: DHCP reservation → render ZTP script → publish → wait up to 8 hours for the device to phone home. In mock mode it completes quickly.

---

### Trigger Day 1 provision

```bash
curl -s -X POST http://localhost:8000/v1/devices/DEV001/provision \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" \
  -d '{"requested_by": "tutorial"}' | python3 -m json.tool
```

If `day1-DEV001` is already running, you get a 409 (idempotency guard):

```json
{
  "type": "about:blank",
  "title": "Conflict",
  "status": 409,
  "detail": "Workflow day1-DEV001 already exists",
  "instance": "/v1/devices/DEV001/provision"
}
```

Error responses follow RFC 7807 (Problem Details for HTTP APIs) — `type`, `title`, `status`, `detail`, `instance` fields.

---

### List workflows

```bash
curl -s http://localhost:8000/v1/workflows \
  -H "X-Authenticated-User: noc:NOC_OPERATOR:SOUTH" | python3 -m json.tool
```

NOC_OPERATOR can read but not write. Try with a POST to see the 403:

```bash
curl -s -X POST http://localhost:8000/v1/devices/DEV002/provision \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: noc:NOC_OPERATOR:SOUTH" \
  -d '{"requested_by": "noc"}' | python3 -m json.tool
```

Response:

```json
{
  "type": "about:blank",
  "title": "Forbidden",
  "status": 403,
  "detail": "Insufficient permissions",
  "instance": "/v1/devices/DEV002/provision"
}
```

---

### Get one workflow's status

```bash
curl -s http://localhost:8000/v1/workflows/day1-DEV001 \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" | python3 -m json.tool
```

---

### Send HITL approval

```bash
curl -s -X POST http://localhost:8000/v1/workflows/day1-DEV001/approve \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" \
  -d '{"decision": "approved"}' | python3 -m json.tool
```

Valid decisions: `"approved"` or `"rejected"`.

---

### Trigger Day 2 compliance scan

```bash
curl -s -X POST http://localhost:8000/v1/sites/SITE001/scan \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" \
  -d '{"requested_by": "tutorial", "device_ids": []}' | python3 -m json.tool
```

`device_ids: []` means scan all devices in the site. Pass specific IDs to scope the scan.

---

### Onboard a single site (Day 0.5)

```bash
curl -s -X POST http://localhost:8000/v1/onboarding/sites/SITE001 \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" | python3 -m json.tool
```

No request body needed for single-site onboarding.

---

### Bulk onboard multiple sites

```bash
curl -s -X POST http://localhost:8000/v1/onboarding/bulk \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" \
  -d '{
    "device_ids": ["DEV010", "DEV011", "DEV012"],
    "requested_by": "tutorial",
    "sites_per_hour": 30,
    "max_concurrent": 3
  }' | python3 -m json.tool
```

`sites_per_hour` controls throttle rate. `max_concurrent` limits how many child workflows run simultaneously.

---

### Check bulk onboarding status

```bash
curl -s "http://localhost:8000/v1/onboarding/status?requested_by=tutorial" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" | python3 -m json.tool
```

Response:

```json
{
  "pending": 1,
  "in_flight": 2,
  "managed": 0,
  "failed": 0
}
```

---

### The Nautobot webhook endpoint

In production, Nautobot sends signed webhooks when a device's provisioning intent changes. The endpoint verifies an HMAC-SHA256 signature:

```bash
# This will return 400 (missing signature) — expected behavior
curl -s -X POST http://localhost:8000/v1/webhooks/nautobot \
  -H "Content-Type: application/json" \
  -d '{"device_id": "DEV001"}' | python3 -m json.tool
```

To generate a valid HMAC for testing, the secret lives in `NAUTOBOT_WEBHOOK_SECRET` in `.env`. The signature header name is `X-Nautobot-Signature`.

---

## Prometheus metrics endpoint

```bash
curl http://localhost:8000/metrics
```

This returns raw Prometheus text format — all the `ztp_*` counters, gauges, and histograms that Prometheus scrapes every 15 seconds.

---

## Using Swagger UI for interactive calls

Go to `http://localhost:8000/docs`. Click any endpoint, click **Try it out**, fill in the fields, and hit **Execute**.

The UI doesn't support custom headers in the browser. For endpoints that require `X-Authenticated-User`, use curl or a tool like `httpie` or Postman.

---

## What you now know

- All write operations require `ENGINEER` or `ADMIN` role — reads work with `NOC_OPERATOR`
- Auth in dev mode is a single header: `X-Authenticated-User: username:ROLE:REGION`
- All errors are RFC 7807 Problem Details with `type`, `title`, `status`, `detail`, `instance`
- Re-submitting the same workflow ID returns 409 (idempotency by design)
- The `/metrics` endpoint at port 8000 is what Prometheus scrapes
- Interactive docs live at `/docs`

---

## Where to go next

You've now seen the full operational surface of the platform. Some areas to explore further:

- **Add a new activity**: See `CLAUDE.md` → "How to add a new activity" for the exact steps
- **Look at the workflow code**: `temporal/workflows/provision_site.py` is the most interesting — HITL signal pattern, retry policies, child workflow execution
- **Run the test suite**: `make test` — 99 tests, no live server needed
- **Understand the mock data**: `temporal/activities/nautobot_activities.py` → `_mock_graphql_response()` shows what Nautobot would return in production
