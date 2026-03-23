// ============================================================
// dac-wire: WebSocket protocol between orchestrator and workers
// ============================================================

import type { ProtocolEventType } from './protocolRegistry.js';

// --- Worker → Orchestrator ---

export interface WorkerHello {
  type: 'hello';
  workerId: string;
  version: string;
  capabilities: string[];
  imageDigest?: string;
  claudeCliVersion?: string;
}

export interface WorkerReady {
  type: 'ready';
}

export interface ExecStdout {
  type: 'stdout';
  executionId: string;
  chunk: string;
  timestamp: number;
}

export interface ExecStderr {
  type: 'stderr';
  executionId: string;
  chunk: string;
  timestamp: number;
}

export interface PhaseTimings {
  sandboxSetupMs: number;
  credentialInjectionMs: number;
  proxyStartupMs: number | null;
  cliRuntimeMs: number;
  cleanupMs: number;
}

export interface ExecComplete {
  type: 'complete';
  executionId: string;
  status: 'completed' | 'failed' | 'cancelled';
  exitCode: number;
  durationMs: number;
  sessionId?: string;
  totalCostUsd?: number;
  phaseTimings?: PhaseTimings;
}

export interface WorkerEvent {
  type: 'event';
  executionId: string;
  eventType: ProtocolEventType;
  payload: unknown;
}

export interface ExecProgress {
  type: 'progress';
  executionId: string;
  phase: string;
  percent: number;
  detail?: string;
  timestamp: number;
}

export interface WorkerHeartbeat {
  type: 'heartbeat';
  timestamp: number;
}

export interface WorkerBusy {
  type: 'busy';
  executionId: string;
  reason: string;
}

export type WorkerMessage =
  | WorkerHello
  | WorkerReady
  | ExecStdout
  | ExecStderr
  | ExecComplete
  | WorkerEvent
  | ExecProgress
  | WorkerHeartbeat
  | WorkerBusy;

// --- Orchestrator → Worker ---

export interface OrchestratorAck {
  type: 'ack';
  workerId: string;
  sessionToken: string;
}

export interface ExecAssign {
  type: 'assign';
  executionId: string;
  personaId: string;
  prompt: string;
  env: Record<string, string>;
  config: {
    timeoutMs: number;
    maxOutputBytes: number;
    networkPolicy?: import('./networkPolicy.js').NetworkPolicy;
    maxBudgetUsd?: number;
    maxTurns?: number;
  };
}

export interface ExecCancel {
  type: 'cancel';
  executionId: string;
}

export interface OrchestratorShutdown {
  type: 'shutdown';
  reason: string;
  gracePeriodMs: number;
}

export interface OrchestratorHeartbeat {
  type: 'heartbeat';
  timestamp: number;
}

export type OrchestratorMessage =
  | OrchestratorAck
  | ExecAssign
  | ExecCancel
  | OrchestratorShutdown
  | OrchestratorHeartbeat;

// --- Runtime validators ---

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function hasStr(o: Record<string, unknown>, k: string): boolean {
  return typeof o[k] === 'string';
}

function hasNum(o: Record<string, unknown>, k: string): boolean {
  return typeof o[k] === 'number';
}

function hasStrArr(o: Record<string, unknown>, k: string): boolean {
  return Array.isArray(o[k]) && (o[k] as unknown[]).every(v => typeof v === 'string');
}

export function validateWorkerMessage(msg: unknown): WorkerMessage | null {
  if (!isObj(msg) || !hasStr(msg, 'type')) return null;
  const m = msg as Record<string, unknown> & { type: string };
  switch (m.type) {
    case 'hello':
      return hasStr(m, 'workerId') && hasStr(m, 'version') && hasStrArr(m, 'capabilities')
        ? (m as unknown as WorkerHello) : null;
    case 'ready':
      return m as unknown as WorkerReady;
    case 'stdout':
    case 'stderr':
      return hasStr(m, 'executionId') && hasStr(m, 'chunk') && hasNum(m, 'timestamp')
        ? (m as unknown as ExecStdout & ExecStderr) : null;
    case 'complete':
      return hasStr(m, 'executionId') && hasStr(m, 'status') && hasNum(m, 'exitCode') && hasNum(m, 'durationMs')
        ? (m as unknown as ExecComplete) : null;
    case 'event':
      return hasStr(m, 'executionId') && hasStr(m, 'eventType') && 'payload' in m
        ? (m as unknown as WorkerEvent) : null;
    case 'progress':
      return hasStr(m, 'executionId') && hasStr(m, 'phase') && hasNum(m, 'percent') && hasNum(m, 'timestamp')
        ? (m as unknown as ExecProgress) : null;
    case 'heartbeat':
      return hasNum(m, 'timestamp') ? (m as unknown as WorkerHeartbeat) : null;
    case 'busy':
      return hasStr(m, 'executionId') && hasStr(m, 'reason')
        ? (m as unknown as WorkerBusy) : null;
    default:
      return null;
  }
}

export function validateOrchestratorMessage(msg: unknown): OrchestratorMessage | null {
  if (!isObj(msg) || !hasStr(msg, 'type')) return null;
  const m = msg as Record<string, unknown> & { type: string };
  switch (m.type) {
    case 'ack':
      return hasStr(m, 'workerId') && hasStr(m, 'sessionToken')
        ? (m as unknown as OrchestratorAck) : null;
    case 'assign':
      return hasStr(m, 'executionId') && hasStr(m, 'personaId') && hasStr(m, 'prompt') && isObj(m.env) && isObj(m.config)
        ? (m as unknown as ExecAssign) : null;
    case 'cancel':
      return hasStr(m, 'executionId') ? (m as unknown as ExecCancel) : null;
    case 'shutdown':
      return hasStr(m, 'reason') && hasNum(m, 'gracePeriodMs')
        ? (m as unknown as OrchestratorShutdown) : null;
    case 'heartbeat':
      return hasNum(m, 'timestamp') ? (m as unknown as OrchestratorHeartbeat) : null;
    default:
      return null;
  }
}

// --- Helpers ---

export function parseWorkerMessage(raw: string): WorkerMessage | null {
  try {
    return validateWorkerMessage(JSON.parse(raw));
  } catch {
    return null;
  }
}

export function parseOrchestratorMessage(raw: string): OrchestratorMessage | null {
  try {
    return validateOrchestratorMessage(JSON.parse(raw));
  } catch {
    return null;
  }
}

export function serializeMessage(msg: WorkerMessage | OrchestratorMessage): string {
  return JSON.stringify(msg);
}
