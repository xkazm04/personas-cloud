import type { Logger } from 'pino';
import type { ProtocolEventType, ExecutionStage, ExecutionProgress } from '@dac-cloud/shared';

/** Legacy progress info extracted from inline JSON in assistant text blocks. */
export interface ProgressInfo {
  phase: string;
  percent: number;
  detail?: string;
  updatedAt: number;
}
import { PROTOCOL_EVENT_KEYS } from '@dac-cloud/shared';

export interface ParsedLine {
  raw: string;
  type?: string;
  sessionId?: string;
  totalCostUsd?: number;
  isResult?: boolean;
}

export interface PersonaProtocolEvent {
  eventType: ProtocolEventType;
  payload: unknown;
}

export interface StreamEvents {
  raw: string;
  type?: string;
  sessionId?: string;
  totalCostUsd?: number;
  personaEvents: PersonaProtocolEvent[];
  progress: ProgressInfo | null;
}

/**
 * Try to parse a line as JSON. Returns the parsed object on success, or null
 * if the line is not valid JSON. Centralises the single JSON.parse call so
 * all downstream consumers can share the result.
 */
export function tryParseJson(line: string): Record<string, unknown> | null {
  try {
    return JSON.parse(line);
  } catch {
    return null;
  }
}

// Parse a single line of Claude CLI stream-json output.
// Accepts an optional pre-parsed JSON object to avoid redundant JSON.parse calls.
export function parseStreamLine(line: string, parsed?: Record<string, unknown> | null): ParsedLine {
  const result: ParsedLine = { raw: line };

  const obj = parsed !== undefined ? parsed : tryParseJson(line);
  if (!obj) return result;

  result.type = obj.type as string | undefined;

  // Extract session ID from various locations
  if (obj.session_id) {
    result.sessionId = obj.session_id as string;
  }
  const resultObj = obj.result as Record<string, unknown> | undefined;
  if (resultObj?.session_id) {
    result.sessionId = resultObj.session_id as string;
  }

  // Extract completion data
  if (obj.type === 'result') {
    result.isResult = true;
    result.totalCostUsd = (obj.total_cost_usd as number | undefined) ?? (resultObj?.total_cost_usd as number | undefined);
  }

  return result;
}

const JSON_BLOCK_PATTERN = /\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}/g;

// Extract all structured artifacts from a single stream line in one JSON parse pass
// (legacy single-pass approach — kept for backward compatibility)
export function extractStreamEvents(line: string, logger: Logger): StreamEvents {
  const result: StreamEvents = { raw: line, personaEvents: [], progress: null };

  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(line);
  } catch {
    return result; // raw text line — nothing to extract
  }

  result.type = parsed.type as string | undefined;

  // Session ID and completion data (from top-level result messages)
  if (parsed.session_id) result.sessionId = parsed.session_id as string;
  const resultObj = parsed.result as Record<string, unknown> | undefined;
  if (resultObj?.session_id) result.sessionId = resultObj.session_id as string;
  if (parsed.type === 'result') {
    result.totalCostUsd = (parsed.total_cost_usd ?? resultObj?.total_cost_usd) as number | undefined;
  }

  // Persona protocol events and progress events (from assistant message content blocks)
  const message = parsed.message as Record<string, unknown> | undefined;
  if (parsed.type === 'assistant' && Array.isArray(message?.content)) {
    for (const block of message!.content as Array<Record<string, unknown>>) {
      if (block.type !== 'text' || typeof block.text !== 'string') continue;

      const matches = block.text.match(JSON_BLOCK_PATTERN);
      if (!matches) continue;

      for (const match of matches) {
        let obj: Record<string, unknown>;
        try { obj = JSON.parse(match); } catch { continue; }

        for (const key of PROTOCOL_EVENT_KEYS) {
          if (obj[key]) {
            result.personaEvents.push({ eventType: key as ProtocolEventType, payload: obj[key] });
            logger.info({ eventType: key }, 'Detected persona protocol event');
          }
        }

        if (!result.progress) {
          const prog = obj.progress as Record<string, unknown> | undefined;
          if (prog && typeof prog.phase === 'string' && typeof prog.percent === 'number') {
            logger.debug({ phase: prog.phase, percent: prog.percent }, 'Detected progress event');
            result.progress = {
              phase: prog.phase,
              percent: Math.max(0, Math.min(100, prog.percent)),
              detail: prog.detail as string | undefined,
              updatedAt: Date.now(),
            };
          }
        }
      }
    }
  }

  return result;
}

/**
 * Mapping from tool_use names to persona protocol event types.
 * Claude tool_use blocks whose `name` matches a key here are treated as
 * structured persona protocol events — no regex/bracket-counting needed.
 * The `input` field of the tool_use block becomes the event payload.
 *
 * The `persona__` prefix is a namespace convention that avoids collisions
 * with user-defined tools.
 */
const TOOL_NAME_TO_PROTOCOL: Record<string, PersonaProtocolEvent['eventType']> = {
  'persona__manual_review': 'manual_review',
  'persona__user_message': 'user_message',
  'persona__persona_action': 'persona_action',
  'persona__emit_event': 'emit_event',
  // Also accept un-prefixed names for backward compatibility
  'manual_review': 'manual_review',
  'user_message': 'user_message',
  'persona_action': 'persona_action',
  'emit_event': 'emit_event',
};

const PROTOCOL_KEYS = new Set(['manual_review', 'user_message', 'persona_action', 'emit_event'] as const);

/**
 * Detect persona protocol events from Claude CLI stream-json output.
 *
 * Two detection strategies, in priority order:
 * 1. **tool_use blocks** (preferred) — deterministic, guaranteed JSON structure,
 *    no regex. Triggered when a tool_use content block has a name matching a
 *    known persona protocol tool. The `input` field is the event payload.
 * 2. **text block fallback** (legacy) — bracket-counting JSON extraction from
 *    assistant text blocks. Only runs when no tool_use protocol events were
 *    found in the same message, preserving backward compatibility with older
 *    prompt formats that embed protocol events as JSON in text output.
 */
export function detectPersonaEvents(line: string, logger: Logger, parsed?: Record<string, unknown> | null): PersonaProtocolEvent[] {
  const events: PersonaProtocolEvent[] = [];

  const obj = parsed !== undefined ? parsed : tryParseJson(line);
  if (!obj) return events;

  // --- Strategy 1: tool_use content blocks (structured, deterministic) ---

  if (obj.type === 'assistant') {
    const message = obj.message as Record<string, unknown> | undefined;
    const content = message?.content as Array<Record<string, unknown>> | undefined;
    if (content) {
      // First pass: extract tool_use-based protocol events
      for (const block of content) {
        if (block.type === 'tool_use') {
          const toolName = block.name as string | undefined;
          if (toolName && toolName in TOOL_NAME_TO_PROTOCOL) {
            const eventType = TOOL_NAME_TO_PROTOCOL[toolName];
            events.push({ eventType, payload: block.input ?? null });
            logger.info({ eventType, toolName }, 'Detected persona protocol event via tool_use');
          }
        }
      }

      // Second pass (fallback): if no tool_use protocol events found,
      // check text blocks for legacy JSON-in-text protocol events.
      if (events.length === 0) {
        for (const block of content) {
          if (block.type === 'text' && block.text) {
            const extracted = extractJsonBlocks(block.text as string, logger);
            events.push(...extracted);
          }
        }
      }
    }
  }

  return events;
}

/**
 * Extract top-level JSON object blocks from text using bracket-counting.
 * Handles arbitrary nesting depth, unlike the previous single-level regex.
 * Returns the raw substrings that form balanced `{ ... }` blocks.
 */
function extractBalancedJsonSubstrings(text: string): string[] {
  const results: string[] = [];
  let depth = 0;
  let start = -1;
  let inString = false;
  let escape = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    if (escape) {
      escape = false;
      continue;
    }

    if (ch === '\\' && inString) {
      escape = true;
      continue;
    }

    if (ch === '"') {
      inString = !inString;
      continue;
    }

    if (inString) continue;

    if (ch === '{') {
      if (depth === 0) start = i;
      depth++;
    } else if (ch === '}') {
      depth--;
      if (depth === 0 && start !== -1) {
        results.push(text.slice(start, i + 1));
        start = -1;
      }
      // Guard against unbalanced closing braces
      if (depth < 0) depth = 0;
    }
  }

  return results;
}

function extractJsonBlocks(text: string, logger: Logger): PersonaProtocolEvent[] {
  const events: PersonaProtocolEvent[] = [];
  const candidates = extractBalancedJsonSubstrings(text);

  for (const candidate of candidates) {
    try {
      const obj = JSON.parse(candidate);
      if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) continue;

      // Only treat as a protocol event if the object contains exactly one
      // known protocol key, reducing false positives from coincidental JSON.
      const keys = Object.keys(obj);
      const matchingKeys = keys.filter((k) => PROTOCOL_KEYS.has(k as any));
      if (matchingKeys.length !== 1) continue;

      const key = matchingKeys[0] as PersonaProtocolEvent['eventType'];
      events.push({
        eventType: key,
        payload: obj[key],
      });
      logger.info({ eventType: key }, 'Detected persona protocol event');
    } catch {
      // Not valid JSON block
    }
  }

  return events;
}

/**
 * Tracks execution progress by interpreting Claude CLI stream-json message types.
 * Maintains a running state machine that derives the current semantic stage from
 * the stream of typed messages (system, assistant, tool_use, tool_result, result).
 */
export class ProgressTracker {
  private _stage: ExecutionStage = 'initializing';
  private _activeTool: string | null = null;
  private _toolCallsCompleted = 0;
  private _stageStartedAt = Date.now();
  private _changed = false;

  /** Process a stream-json line and update internal stage. Returns true if stage changed.
   *  Accepts an optional pre-parsed JSON object to avoid redundant JSON.parse calls. */
  update(line: string, parsed?: Record<string, unknown> | null): boolean {
    this._changed = false;

    const obj = parsed !== undefined ? parsed : tryParseJson(line);
    if (!obj) return false;

    const msgType = obj['type'] as string | undefined;
    if (!msgType) return false;

    switch (msgType) {
      case 'system':
        this.setStage('initializing');
        break;

      case 'assistant': {
        // Check content blocks for tool_use
        const message = obj['message'] as Record<string, unknown> | undefined;
        const content = message?.['content'] as Array<Record<string, unknown>> | undefined;
        if (content) {
          let hasToolUse = false;
          let toolName: string | null = null;
          for (const block of content) {
            if (block['type'] === 'tool_use') {
              hasToolUse = true;
              toolName = (block['name'] as string) ?? null;
            }
          }
          if (hasToolUse) {
            this._activeTool = toolName;
            this.setStage('tool_calling');
          } else {
            // Assistant text without tool use — generating output
            this._activeTool = null;
            this.setStage('generating');
          }
        } else {
          // Assistant message with no content blocks — thinking
          this._activeTool = null;
          this.setStage('thinking');
        }
        break;
      }

      case 'content_block_start': {
        const contentBlock = obj['content_block'] as Record<string, unknown> | undefined;
        if (contentBlock?.['type'] === 'tool_use') {
          this._activeTool = (contentBlock['name'] as string) ?? null;
          this.setStage('tool_calling');
        } else if (contentBlock?.['type'] === 'text') {
          this._activeTool = null;
          this.setStage('generating');
        }
        break;
      }

      case 'content_block_stop': {
        // A content block completed — if we were tool_calling, the call is done
        if (this._stage === 'tool_calling') {
          this._activeTool = null;
          this.setStage('thinking');
        }
        break;
      }

      case 'tool_use': {
        this._activeTool = (obj['name'] as string) ?? null;
        this.setStage('tool_calling');
        break;
      }

      case 'tool_result': {
        this._toolCallsCompleted++;
        this._activeTool = null;
        this.setStage('tool_result');
        break;
      }

      case 'result':
        this._activeTool = null;
        this.setStage('completed');
        break;
    }

    return this._changed;
  }

  /** Get current progress snapshot. */
  getProgress(): ExecutionProgress {
    return {
      stage: this._stage,
      percentEstimate: this.estimatePercent(),
      activeTool: this._activeTool,
      message: this.buildMessage(),
      toolCallsCompleted: this._toolCallsCompleted,
      stageStartedAt: this._stageStartedAt,
    };
  }

  private setStage(stage: ExecutionStage): void {
    if (stage !== this._stage) {
      this._stage = stage;
      this._stageStartedAt = Date.now();
      this._changed = true;
    }
  }

  private estimatePercent(): number | null {
    switch (this._stage) {
      case 'initializing': return 5;
      case 'thinking': return 15 + Math.min(this._toolCallsCompleted * 10, 40);
      case 'tool_calling': return 20 + Math.min(this._toolCallsCompleted * 10, 40);
      case 'tool_result': return 25 + Math.min(this._toolCallsCompleted * 10, 40);
      case 'generating': return 70 + Math.min(this._toolCallsCompleted * 3, 20);
      case 'completed': return 100;
      case 'failed': return null;
      default: return null;
    }
  }

  private buildMessage(): string {
    switch (this._stage) {
      case 'initializing': return 'Setting up execution environment';
      case 'thinking': return this._toolCallsCompleted > 0
        ? `Analyzing results from ${this._toolCallsCompleted} tool call${this._toolCallsCompleted > 1 ? 's' : ''}`
        : 'Thinking...';
      case 'tool_calling': return this._activeTool
        ? `Calling tool: ${this._activeTool}`
        : 'Calling tool...';
      case 'tool_result': return `Processing tool result (${this._toolCallsCompleted} completed)`;
      case 'generating': return 'Generating response';
      case 'completed': return 'Execution completed';
      case 'failed': return 'Execution failed';
      default: return 'Processing...';
    }
  }
}

// ---------------------------------------------------------------------------
// StreamEvent — discriminated union emitted by StreamProcessor.
// Each variant represents a typed output from the parsing pipeline, making
// every stage independently testable without spawning a child process.
// ---------------------------------------------------------------------------

export type StreamEvent =
  | { type: 'stdout'; line: string; timestamp: number }
  | { type: 'stderr'; line: string; timestamp: number }
  | { type: 'session_id'; sessionId: string }
  | { type: 'cost'; totalCostUsd: number }
  | { type: 'progress'; progress: ExecutionProgress }
  | { type: 'persona_event'; eventType: string; payload: unknown };

/**
 * Composable stream-parsing pipeline that consumes raw stdout/stderr chunks
 * and yields typed StreamEvent values.  Each stage (line buffering, JSON
 * parsing, event detection, progress tracking) is applied in sequence,
 * eliminating inline closures from the Executor.
 *
 * The Executor becomes a thin process manager: spawn -> pipe through
 * StreamProcessor -> forward events to callbacks.
 */
export class StreamProcessor {
  private stdoutBuffer = new LineBuffer();
  private stderrBuffer = new LineBuffer();
  private progressTracker = new ProgressTracker();

  constructor(private logger: Logger) {}

  /** Process a chunk of stdout data and return all parsed events. */
  pushStdout(data: string): StreamEvent[] {
    const events: StreamEvent[] = [];
    const lines = this.stdoutBuffer.push(data);
    for (const line of lines) {
      this.processStdoutLine(line, events);
    }
    return events;
  }

  /** Process a chunk of stderr data and return all parsed events. */
  pushStderr(data: string): StreamEvent[] {
    const events: StreamEvent[] = [];
    const lines = this.stderrBuffer.push(data);
    for (const line of lines) {
      events.push({ type: 'stderr', line, timestamp: Date.now() });
    }
    return events;
  }

  /** Flush remaining buffered data (call after process exit). */
  flush(): StreamEvent[] {
    const events: StreamEvent[] = [];
    for (const line of this.stdoutBuffer.flush()) {
      this.processStdoutLine(line, events);
    }
    for (const line of this.stderrBuffer.flush()) {
      events.push({ type: 'stderr', line, timestamp: Date.now() });
    }
    return events;
  }

  /** Get the current progress snapshot (for final flush). */
  getProgress(): ExecutionProgress {
    return this.progressTracker.getProgress();
  }

  private processStdoutLine(line: string, events: StreamEvent[]): void {
    events.push({ type: 'stdout', line, timestamp: Date.now() });

    // Parse JSON once and share across all consumers
    const jsonObj = tryParseJson(line);

    // Extract session ID and cost from structured data
    const parsed = parseStreamLine(line, jsonObj);
    if (parsed.sessionId) {
      events.push({ type: 'session_id', sessionId: parsed.sessionId });
    }
    if (parsed.totalCostUsd !== undefined) {
      events.push({ type: 'cost', totalCostUsd: parsed.totalCostUsd });
    }

    // Track execution progress stage
    if (this.progressTracker.update(line, jsonObj)) {
      events.push({ type: 'progress', progress: this.progressTracker.getProgress() });
    }

    // Detect persona protocol events (skip for non-JSON lines)
    if (jsonObj) {
      const personaEvents = detectPersonaEvents(line, this.logger, jsonObj);
      for (const pe of personaEvents) {
        events.push({ type: 'persona_event', eventType: pe.eventType, payload: pe.payload });
      }
    }
  }
}

/** Maximum bytes allowed in the line buffer before a forced flush (1 MB). */
const MAX_LINE_BUFFER_BYTES = 1024 * 1024;

// Buffered line reader for streaming data
export class LineBuffer {
  private buffer = '';

  push(chunk: string): string[] {
    this.buffer += chunk;
    const lines: string[] = [];
    let newlineIdx: number;

    while ((newlineIdx = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newlineIdx).trim();
      this.buffer = this.buffer.slice(newlineIdx + 1);
      if (line.length > 0) {
        lines.push(line);
      }
    }

    // Cap: if the buffer exceeds the limit without a newline, emit the
    // accumulated content as a line and reset to prevent memory exhaustion.
    if (this.buffer.length > MAX_LINE_BUFFER_BYTES) {
      const oversized = this.buffer.trim();
      this.buffer = '';
      if (oversized.length > 0) {
        lines.push(oversized);
      }
    }

    return lines;
  }

  flush(): string[] {
    const remaining = this.buffer.trim();
    this.buffer = '';
    return remaining.length > 0 ? [remaining] : [];
  }
}
