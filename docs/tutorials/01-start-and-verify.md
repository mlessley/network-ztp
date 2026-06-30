# Tutorial 01: Start the Stack and Verify Health

**Goal:** Get all services running and confirm every piece is healthy before you touch any workflows.

**Time:** ~15 minutes

**Prerequisites:** Docker Desktop (or Docker Engine + Compose plugin) installed and running.

---

## Step 1: Copy the environment file

The services need a `.env` file for configuration. An example is provided:

```bash
cp .env.example .env
```

Open `.env` and take a look — most values have sensible defaults for local development. The important ones:

| Variable | Default | What it does |
|----------|---------|-------------|
| `ZTP_USE_MOCK` | `true` | Skips real Nautobot/Ansible calls; uses built-in mock data |
| `TEMPORAL_HOST` | `temporal:7233` | Where the worker connects to Temporal (use container name inside Docker) |
| `OTLP_ENDPOINT` | `http://tempo:4317` | Where OTel traces are sent |

**Leave everything as-is for these tutorials.** Mock mode means the platform runs end-to-end with no external dependencies.

---

## Step 2: Build the images

The worker and API are built from local Dockerfiles. Build them once (takes ~2 minutes the first time):

```bash
make build
```

You'll see Docker building two images: `ztp-api` and `ztp-worker`.

---

## Step 3: Start all services

```bash
make dev
```

This runs `docker compose` with the development override (which explicitly sets `ZTP_USE_MOCK=true`). You'll see all 10 services start in order:

```
postgresql   → starts first (Temporal depends on it)
temporal     → waits for postgresql to be healthy
temporal-ui  → starts after temporal
ztp-worker   → waits for temporal to be healthy
ztp-api      → waits for temporal to be healthy
prometheus   → starts any time
tempo        → starts any time
loki         → starts any time
promtail     → starts any time
grafana      → waits for prometheus, tempo, loki
```

> **Tip:** If you want services in the background (detached), use `make up` instead. Then use `make logs` to tail the worker and API logs.

---

## Step 4: Verify all services are up

Open a new terminal and check container states:

```bash
docker compose ps
```

Expected — every service should show `healthy` or `running`:

```
NAME          STATUS              PORTS
postgresql    running (healthy)   5432/tcp
temporal      running (healthy)   0.0.0.0:7233->7233/tcp
temporal-ui   running             0.0.0.0:8080->8080/tcp
ztp-worker    running             0.0.0.0:9091->9091/tcp
ztp-api       running             0.0.0.0:8000->8000/tcp
prometheus    running             0.0.0.0:9090->9090/tcp
tempo         running             0.0.0.0:4317->4317/tcp, 0.0.0.0:3200->3200/tcp
loki          running             0.0.0.0:3100->3100/tcp
promtail      running
grafana       running             0.0.0.0:3000->3000/tcp
```

If something shows `unhealthy` or `exited`, check its logs:

```bash
docker compose logs <service-name>
```

---

## Step 5: Hit the health endpoint

The API exposes a health check at `/health`:

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{"status": "ok"}
```

If you get a connection refused, the API container is still starting — wait 10 seconds and try again.

---

## Step 6: Open the UIs

You now have four browser interfaces:

### Temporal UI — `http://localhost:8080`

This is where you watch workflows execute, inspect their event history, and debug failures. It connects to the Temporal server running on port 7233. You should see the default namespace with no workflows yet — that's expected.

Click **Workflows** in the left nav. The list will be empty until you run something in Tutorial 02.

### Grafana — `http://localhost:3000`

Login: `admin` / `admin` (it will ask you to change the password — you can skip this for local dev).

The dashboards are pre-provisioned under **Dashboards → ZTP**:
- **Worker Overview** — workflow throughput, activity latency
- **Pipeline Latency** — per-phase timing
- **Compliance Health** — drift rates
- **Onboarding Progress** — Day 0.5 funnel

All panels will show "No Data" until workflows run.

### API Interactive Docs — `http://localhost:8000/docs`

Swagger UI with every endpoint documented. You can call endpoints directly from the browser. You'll need to add an auth header first — covered in Tutorial 05.

### Prometheus — `http://localhost:9090`

Raw metric browser. Try querying `ztp_workflow_started_total` once you've run a workflow. The Prometheus UI is mainly useful for checking scrape status.

---

## Step 7: Check Prometheus is scraping successfully

In the Prometheus UI (`http://localhost:9090`), go to **Status → Targets**.

You should see three targets:
- `ztp-api:8000` — the FastAPI `/metrics` endpoint
- `ztp-worker:9091` — the Temporal worker metrics
- `temporal:9090` — the Temporal server (this one may show as "down" in some setups — that's OK)

Green = actively scraping. If `ztp-api` is red, the API container isn't ready yet.

---

## Step 8: Tail the logs

To see what the worker and API are doing in real time:

```bash
make logs
```

You should see structured JSON logs from both services. Each log line includes `event`, `level`, `timestamp`, and a `request_id` for tracing API calls. Example:

```json
{"event": "worker started", "task_queue": "ztp-queue", "level": "info", "timestamp": "..."}
```

Press `Ctrl+C` to stop tailing (services keep running).

---

## Stopping the stack

```bash
make down          # stop containers, preserve volumes (Temporal history survives)
make reset         # stop containers AND wipe volumes (clean slate)
```

---

## What you now know

- The stack has 10 services across three tiers: infrastructure (Postgres, Temporal), application (worker, API), and observability (Prometheus, Tempo, Loki, Promtail, Grafana)
- Mock mode lets everything run end-to-end with no external systems
- Health lives at `GET /health`; Temporal workflow state lives in the Temporal UI
- Logs are structured JSON, scraped by Promtail → Loki → visible in Grafana

**Next:** [Tutorial 02 — Trigger Your First Workflow](02-your-first-workflow.md)
