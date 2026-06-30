# network-ztp Developer Tutorials

A progressive series for getting familiar with the platform — start here, work through them in order.

---

## Learning Path

| # | Tutorial | What You Learn | Time |
|---|----------|---------------|------|
| [01](01-start-and-verify.md) | **Start the Stack and Verify Health** | Boot all services, confirm they're healthy, find the UIs | ~15 min |
| [02](02-your-first-workflow.md) | **Trigger Your First Workflow** | Submit a Day 1 provision job, watch it run in Temporal UI, read logs | ~20 min |
| [03](03-reading-observability.md) | **Reading the Observability Stack** | Grafana dashboards, Prometheus metrics, distributed traces in Tempo | ~25 min |
| [04](04-human-in-the-loop.md) | **Human-in-the-Loop Escalation** | Trigger a drift condition, receive an alert, approve or reject via CLI | ~20 min |
| [05](05-exploring-the-api.md) | **Exploring the REST API** | Interactive API docs, auth headers, call every endpoint with curl | ~30 min |

---

## Architecture Quick Reference

```
                         ┌──────────────┐
  CLI / API client ──►  │  FastAPI :8000│
                         └──────┬───────┘
                                │ Temporal SDK
                         ┌──────▼───────┐
                         │  Temporal    │  :7233 (gRPC)
                         │  Server      │  :8080 (UI)
                         └──────┬───────┘
                                │ task queue: ztp-queue
                         ┌──────▼───────┐
                         │  ZTP Worker  │
                         │  (activities)│
                         └──────────────┘

Observability sidecar:
  Prometheus :9090  ←  scrapes :8000/metrics and :9091/metrics
  Tempo      :4317  ←  receives OTel traces from API + worker
  Loki       :3100  ←  receives logs via Promtail
  Grafana    :3000  →  dashboards over all three
```

## The Four Phases

| Phase | Trigger | What Happens |
|-------|---------|-------------|
| **Day 0** — Bootstrap | Device PXE boots | DHCP reservation → ZTP script render → publish → wait for reachability |
| **Day 0.5** — Onboard | Site commissioning | Discover state → reconcile Nautobot → generate remediation plan → HITL |
| **Day 1** — Provision | Intent exists in Nautobot | Fetch intent → render config → push via Ansible → validate drift |
| **Day 2** — Compliance | Scheduled (every 4-6h) | Scan all devices → detect drift → escalate if needed |
