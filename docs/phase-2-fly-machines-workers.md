# Phase 2: Replace Always-On Workers with Fly Machines

> **Goal:** Replace always-on worker VMs with on-demand Fly Machine sandboxes that boot per execution and auto-stop after completion.
> **Estimated effort:** 3-5 days
> **Risk:** Medium — changes the worker lifecycle model but keeps the existing protocol
> **Cost impact:** Workers drop from $200-375/month → ~$36/month (3,000 agents × 3 min/day)
> **Dependency:** Phase 1 (Kafka removed simplifies deployment)

---

## Current Worker Architecture

```
Orchestrator                          Worker VM (always-on)
    │                                     │
    │◄── WebSocket ──────────────────────►│
    │    worker-ready (slots: 3)          │
    │    exec-assign ──────────────►      │
    │                                spawn("claude", ["-p", "-", ...])
    │    ◄── exec-stdout ───────────      │    ├── isolated HOME dir
    │    ◄── exec-complete ─────────      │    ├── temp exec dir
    │                                     │    └── sanitized env vars
    │    exec-assign ──────────────►      │
    │    ...                              │
```

Workers are **long-lived processes** that:
1. Connect to orchestrator via WebSocket (`connection.ts`)
2. Announce available slots (`sendReady`)
3. Receive assignments (`ExecAssign`)
4. Spawn Claude CLI as a child process (`executor.ts`)
5. Create isolated temp dir + HOME per execution (`cleanup.ts`)
6. Stream output back via WebSocket micro-batching (`OutputBatcher`)
7. Wait for next assignment

**The problem:** Workers pay for 24/7 uptime but only execute for minutes per day.

---

## Target Architecture

```
Orchestrator (Fly.io, always-on)
    │
    │  POST /v1/machines (Fly Machines API)
    │  ──────────────────────────────────►  Fly Machine (boots in ~1s)
    │                                        │
    │◄── WebSocket ─────────────────────────►│  Worker process starts
    │    worker-ready                         │  Same code as today
    │    exec-assign ──────────────────►      │
    │                                    spawn("claude", [...])
    │    ◄── exec-stdout ────────────────     │
    │    ◄── exec-complete ──────────────     │
    │                                         │  Process exits
    │  Machine auto-stops (no idle cost)      X
```

Each execution gets a **fresh VM** that:
- Boots from pre-built Docker image with Claude CLI
- Connects to orchestrator via WebSocket (existing protocol)
- Runs exactly one execution
- Auto-stops after completion (or timeout)

---

## Implementation Plan

### Step 1: Build the Fly Machine Docker image

Create a Dockerfile that extends the current worker image with Claude CLI pre-installed:

**File:** `packages/worker/Dockerfile.fly`

```dockerfile
FROM node:22-slim

# Install Claude CLI
RUN npm install -g @anthropic-ai/claude-code

# Copy worker code
WORKDIR /app
COPY package.json ./
RUN npm install --production
COPY dist/ ./dist/

# Single execution mode: connect, run one assignment, exit
ENV SINGLE_EXECUTION=1
ENV MAX_CONCURRENT_EXECUTIONS=1

CMD ["node", "dist/index.js"]
```

### Step 2: Add single-execution mode to the worker

**File:** `packages/worker/src/index.ts`

Add a mode where the worker exits after completing one execution instead of waiting for more:

```typescript
const singleExecution = process.env['SINGLE_EXECUTION'] === '1';

// In the onAssign callback, after execution completes:
if (singleExecution) {
  logger.info({ executionId: msg.executionId }, 'Single-execution mode — shutting down');
  // Brief delay to ensure complete message is flushed
  setTimeout(() => shutdown(), 500);
}
```

This is a minimal change — the worker already handles graceful shutdown via `connection.disconnect()`. The only addition is triggering shutdown after the first completed execution.

### Step 3: Create the MachinePool (replaces WorkerPool for dispatch)

**File:** `packages/orchestrator/src/machinePool.ts` (new)

The `MachinePool` implements the same interface that `WorkerPool` exposes to the `Dispatcher`, but instead of managing persistent WebSocket connections, it creates Fly Machines on demand:

```typescript
import type { ExecAssign, WorkerInfo } from '@dac-cloud/shared';
import type { Logger } from 'pino';

interface FlyMachineConfig {
  appName: string;        // Fly app name
  apiToken: string;       // Fly API token
  region: string;         // e.g., 'iad'
  imageRef: string;       // Docker image reference
  orchestratorWsUrl: string; // WSS URL machines connect back to
  workerToken: string;    // Auth token for WS connection
}

interface MachineRequest {
  executionId: string;
  machineId?: string;     // Fly Machine ID once created
  createdAt: number;
  status: 'creating' | 'running' | 'failed';
}

export class MachinePool {
  private pending = new Map<string, MachineRequest>();
  private readonly FLY_API = 'https://api.machines.dev/v1';

  constructor(
    private config: FlyMachineConfig,
    private logger: Logger,
  ) {}

  /**
   * Request a new Fly Machine for a specific execution.
   * The machine boots, connects back via WebSocket, receives
   * the assignment through the existing WorkerPool protocol,
   * and auto-stops after completion.
   */
  async requestMachine(executionId: string, env: Record<string, string>): Promise<string> {
    const response = await fetch(
      `${this.FLY_API}/apps/${this.config.appName}/machines`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.config.apiToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          config: {
            image: this.config.imageRef,
            env: {
              ORCHESTRATOR_URL: this.config.orchestratorWsUrl,
              WORKER_TOKEN: this.config.workerToken,
              WORKER_ID: `fly-${executionId}`,
              SINGLE_EXECUTION: '1',
              MAX_CONCURRENT_EXECUTIONS: '1',
              ...env,
            },
            guest: {
              cpu_kind: 'performance',
              cpus: 1,
              memory_mb: 1024,
            },
            auto_destroy: true, // Remove machine after it stops
            restart: { policy: 'no' }, // Don't restart on exit
          },
          region: this.config.region,
        }),
      },
    );

    if (!response.ok) {
      throw new Error(`Fly Machine creation failed: ${response.status} ${await response.text()}`);
    }

    const machine = await response.json();
    this.pending.set(executionId, {
      executionId,
      machineId: machine.id,
      createdAt: Date.now(),
      status: 'running',
    });

    this.logger.info({
      executionId,
      machineId: machine.id,
      region: this.config.region,
    }, 'Fly Machine created for execution');

    return machine.id;
  }

  /**
   * Force-stop a machine (for cancellation or timeout).
   */
  async stopMachine(executionId: string): Promise<void> {
    const req = this.pending.get(executionId);
    if (!req?.machineId) return;

    await fetch(
      `${this.FLY_API}/apps/${this.config.appName}/machines/${req.machineId}/stop`,
      {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${this.config.apiToken}` },
      },
    );

    this.pending.delete(executionId);
  }

  /** Called when execution completes — clean up tracking. */
  onComplete(executionId: string): void {
    this.pending.delete(executionId);
  }
}
```

### Step 4: Modify Dispatcher to use MachinePool

**File:** `packages/orchestrator/src/dispatcher.ts`

The dispatcher currently calls `this.pool.assignExecution(workerId, assignment)` to send work to an already-connected worker. With Fly Machines, the flow becomes:

1. Dispatcher resolves persona (credentials, prompt, env) — this part is unchanged
2. Instead of finding a ready worker in `WorkerPool`, create a Fly Machine via `MachinePool`
3. The machine boots, connects to `WorkerPool` via WebSocket (existing protocol)
4. `WorkerPool` receives the connection and emits `worker-ready`
5. Dispatcher assigns the execution to this dedicated worker
6. Execution runs, completes, machine auto-stops

The key insight: **WorkerPool stays as-is**. It still manages WebSocket connections from workers. The only change is WHO creates the workers — instead of manually deployed VMs, it's the `MachinePool` creating them on demand.

```typescript
// In Dispatcher, add alongside existing pool:
private machinePool: MachinePool | null = null;

setMachinePool(pool: MachinePool): void {
  this.machinePool = pool;
}

// In the dispatch method, when no worker is ready:
private async dispatchToMachine(request: ExecRequest, resolved: ResolvedPersona): Promise<void> {
  if (!this.machinePool) {
    // Fallback: queue for traditional worker pool
    return;
  }

  // Create a machine — it will connect back via WebSocket
  const machineId = await this.machinePool.requestMachine(
    request.executionId,
    resolved.env,
  );

  // The machine will connect to WorkerPool, announce ready,
  // and the existing dispatch loop will assign the queued execution.
}
```

### Step 5: Handle the machine connection lifecycle

**File:** `packages/orchestrator/src/workerPool.ts`

Add awareness that some workers are ephemeral Fly Machines:

```typescript
// In WorkerPool, track machine-backed workers
private machineWorkers = new Set<string>();

// When a worker connects with ID starting with 'fly-':
if (workerId.startsWith('fly-')) {
  this.machineWorkers.add(workerId);
}

// On disconnect, notify MachinePool for cleanup
// (auto_destroy handles the actual machine removal)
```

### Step 6: Timeout and failure handling

Fly Machines have a `max_duration` setting. Set this to match execution timeout + buffer:

```typescript
config: {
  // ... existing config
  stop_config: {
    timeout: '30s',  // Grace period for SIGTERM
  },
  // Machine auto-stops if execution takes too long
  // This is a safety net — the CLI timeout (from ExecAssign) handles it first
}
```

For machine creation failures:
- If the Fly API is unavailable, fall back to queuing for any traditional workers
- If the machine fails to connect within 30 seconds, mark the execution as failed
- The existing retry logic in `eventProcessor.ts` handles re-dispatch

### Step 7: Deploy orchestrator to Fly.io

**File:** `packages/orchestrator/fly.toml` (new)

```toml
app = "personas-orchestrator"
primary_region = "iad"

[build]
  dockerfile = "Dockerfile"

[env]
  LOG_LEVEL = "info"
  FLY_MACHINES_ENABLED = "true"

[http_service]
  internal_port = 3001
  force_https = true

[[services]]
  internal_port = 3002
  protocol = "tcp"
  [services.concurrency]
    type = "connections"
    hard_limit = 1000

[mounts]
  source = "personas_data"
  destination = "/data"
```

The SQLite database lives on a Fly volume (`/data/personas.db`). This persists across deploys.

---

## Migration Strategy

### Parallel operation period

Run both systems simultaneously during migration:

1. Deploy orchestrator to Fly.io alongside existing deployment
2. Deploy worker image to Fly Machines registry
3. Keep existing always-on workers connected
4. Enable `FLY_MACHINES_ENABLED=true` — new executions go to Fly Machines
5. Existing workers handle overflow / fallback
6. After 1 week of stable operation, decommission always-on workers

### Rollback

Set `FLY_MACHINES_ENABLED=false`. Executions queue for traditional workers. Reconnect always-on worker VMs.

---

## Cost Calculation

| Component | Calculation | Monthly |
|---|---|---|
| Orchestrator (Fly, performance-1x, 1GB) | Always-on | $10.70 |
| Fly volume (10GB for SQLite) | $0.15/GB | $1.50 |
| Fly Machines (90K exec × 3 min) | 270K min × $0.0022/min | $35.64 |
| **Total** | | **$47.84** |

vs current: $275-450 for orchestrator VMs + worker VMs.

---

## Validation Checklist

- [ ] Worker Docker image builds with Claude CLI functional
- [ ] Single-execution mode: worker exits cleanly after one execution
- [ ] Fly Machine boots and connects to orchestrator within 5 seconds
- [ ] Execution runs to completion, output streams via WebSocket
- [ ] Machine auto-destroys after execution
- [ ] Execution timeout kills machine correctly
- [ ] Machine creation failure falls back to queued state
- [ ] Concurrent executions each get their own machine
- [ ] SQLite database persists across orchestrator redeploys
- [ ] Desktop clients see no difference in behavior
