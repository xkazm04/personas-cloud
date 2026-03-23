# personas-cloud — Infrastructure Evolution Plan

Four-phase migration from the current multi-VM + Kafka architecture to a simplified, cost-efficient deployment using Fly.io Machines for sandboxed Claude CLI execution.

## Phase Overview

| Phase | Goal | Effort | Cost Impact | Trigger |
|---|---|---|---|---|
| [Phase 1](phase-1-drop-kafka.md) | Drop Kafka dependency | 1-2 days | -$150/month | Immediate |
| [Phase 2](phase-2-fly-machines-workers.md) | Replace always-on workers with Fly Machines | 3-5 days | -$250/month | After Phase 1 |
| [Phase 3](phase-3-global-event-subscriptions.md) | Global (cross-client) event subscriptions | 2-3 days | $0 | After Phase 2 |
| [Phase 4](phase-4-scale-to-postgresql.md) | Multi-orchestrator HA via PostgreSQL | 5-8 days | +$0-25/month | When >5,000 agents |

## Cost Trajectory

```
Current:     $550/month  (3 orch VMs + 10 worker VMs + Kafka + Supabase)
After Ph1:   $400/month  (3 orch VMs + 10 worker VMs + Supabase)
After Ph2:    $48/month  (1 Fly orchestrator + Fly Machines on-demand)
After Ph3:    $48/month  (no infra change)
After Ph4:    $86/month  (3 regional orchestrators + Neon Pro + Fly Machines)
```

All phases assume BYOK (Bring Your Own Key) — users provide their own Claude API keys. Platform pays only infrastructure.

## Architecture After Phase 2 (Target for Most Deployments)

```
Event Sources → Orchestrator (Fly.io, $10) → Fly Machines (on-demand, ~$36)
                     │                              │
                SQLite (Fly volume)          Claude CLI sandbox
                     │                      (boots in ~1s, runs 3 min, dies)
                WebSocket ──► Desktop clients
```

## Key Decisions

- **Claude CLI over API** — CLI provides full agentic runtime (tool execution, MCP, sessions). Not replaceable by API calls.
- **BYOK over platform-paid LLM** — Users bring their own Claude key. Platform cost is fixed infrastructure regardless of usage.
- **Fly Machines over containers** — Firecracker microVMs give full isolation per execution with sub-second boot and per-second billing.
- **SQLite-first** — PostgreSQL only when multi-instance coordination is needed (Phase 4). SQLite handles 5,000+ agents on a single orchestrator.
- **No Kafka** — The SQLite event bus is the primary event path. Kafka was a secondary sink that added $150/month and operational complexity.
