import type { Logger } from 'pino';

// ---------------------------------------------------------------------------
// Fly Machines API integration — on-demand worker VMs
// ---------------------------------------------------------------------------

export interface FlyMachineConfig {
  /** Fly.io app name that hosts the worker machines. */
  appName: string;
  /** Fly API token (FLY_API_TOKEN). */
  apiToken: string;
  /** Preferred region (e.g. 'iad', 'cdg'). */
  region: string;
  /** Docker image reference for the worker (from Fly registry). */
  imageRef: string;
  /** Public WSS URL the machine connects back to (e.g. wss://personas-orchestrator.fly.dev:8443). */
  orchestratorWsUrl: string;
  /** Worker token for WebSocket authentication. */
  workerToken: string;
  /** CPU kind: 'shared' (cheaper) or 'performance'. */
  cpuKind?: 'shared' | 'performance';
  /** Number of CPUs per machine. */
  cpus?: number;
  /** Memory in MB per machine. */
  memoryMb?: number;
}

interface MachineRequest {
  executionId: string;
  machineId: string;
  createdAt: number;
  status: 'creating' | 'running' | 'completed' | 'failed';
}

const FLY_API = 'https://api.machines.dev/v1';

/** Timeout for machine creation API call (10s). */
const CREATE_TIMEOUT_MS = 10_000;

/** How long to wait for a machine to connect back before marking it failed (30s). */
const CONNECT_TIMEOUT_MS = 30_000;

export class MachinePool {
  private pending = new Map<string, MachineRequest>();
  private connectTimers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(
    private config: FlyMachineConfig,
    private logger: Logger,
  ) {}

  /**
   * Create a new Fly Machine for a specific execution.
   * The machine boots (~1s), connects to orchestrator via WebSocket,
   * receives the assignment through the existing WorkerPool protocol,
   * and auto-stops after completion.
   */
  async requestMachine(executionId: string, env: Record<string, string> = {}): Promise<string> {
    const workerEnv: Record<string, string> = {
      ORCHESTRATOR_URL: this.config.orchestratorWsUrl,
      WORKER_TOKEN: this.config.workerToken,
      WORKER_ID: `fly-${executionId}`,
      SINGLE_EXECUTION: '1',
      MAX_CONCURRENT_EXECUTIONS: '1',
      ALLOW_INSECURE_WS: this.config.orchestratorWsUrl.startsWith('ws://') ? '1' : '0',
      ...env,
    };

    const body = {
      config: {
        image: this.config.imageRef,
        env: workerEnv,
        guest: {
          cpu_kind: this.config.cpuKind ?? 'shared',
          cpus: this.config.cpus ?? 1,
          memory_mb: this.config.memoryMb ?? 1024,
        },
        auto_destroy: true,
        restart: { policy: 'no' },
      },
      region: this.config.region,
    };

    const response = await fetch(
      `${FLY_API}/apps/${this.config.appName}/machines`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.config.apiToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(CREATE_TIMEOUT_MS),
      },
    );

    if (!response.ok) {
      const detail = await response.text().catch(() => '(no body)');
      throw new Error(`Fly Machine creation failed: ${response.status} ${detail}`);
    }

    const machine = (await response.json()) as { id: string };

    this.pending.set(executionId, {
      executionId,
      machineId: machine.id,
      createdAt: Date.now(),
      status: 'running',
    });

    // Safety timer — if the machine never connects, mark execution for re-queue
    const timer = setTimeout(() => {
      const req = this.pending.get(executionId);
      if (req && req.status === 'running') {
        this.logger.warn({ executionId, machineId: machine.id }, 'Fly Machine did not connect within timeout');
        req.status = 'failed';
        this.pending.delete(executionId);
      }
      this.connectTimers.delete(executionId);
    }, CONNECT_TIMEOUT_MS);
    this.connectTimers.set(executionId, timer);

    this.logger.info({
      executionId,
      machineId: machine.id,
      region: this.config.region,
    }, 'Fly Machine created for execution');

    return machine.id;
  }

  /**
   * Called when a fly-* worker connects via WebSocket — clear the connect timeout.
   */
  onMachineConnected(executionId: string): void {
    const timer = this.connectTimers.get(executionId);
    if (timer) {
      clearTimeout(timer);
      this.connectTimers.delete(executionId);
    }
  }

  /**
   * Force-stop a machine (for cancellation or execution timeout).
   */
  async stopMachine(executionId: string): Promise<void> {
    const req = this.pending.get(executionId);
    if (!req?.machineId) return;

    try {
      await fetch(
        `${FLY_API}/apps/${this.config.appName}/machines/${req.machineId}/stop`,
        {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${this.config.apiToken}` },
          signal: AbortSignal.timeout(5_000),
        },
      );
      this.logger.info({ executionId, machineId: req.machineId }, 'Fly Machine stopped');
    } catch (err) {
      this.logger.warn({ err, executionId, machineId: req.machineId }, 'Failed to stop Fly Machine (auto_destroy will clean up)');
    }

    this.cleanup(executionId);
  }

  /** Called when execution completes — clean up tracking. */
  onComplete(executionId: string): void {
    this.cleanup(executionId);
  }

  /** Check if an execution is backed by a Fly Machine. */
  isFlyExecution(executionId: string): boolean {
    return this.pending.has(executionId);
  }

  /** Number of machines currently pending/running. */
  get activeCount(): number {
    return this.pending.size;
  }

  private cleanup(executionId: string): void {
    this.pending.delete(executionId);
    const timer = this.connectTimers.get(executionId);
    if (timer) {
      clearTimeout(timer);
      this.connectTimers.delete(executionId);
    }
  }
}
