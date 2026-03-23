import type { Logger } from 'pino';
import type { ProgressInfo, ProtocolEventType } from '@dac-cloud/shared';
import { PROTOCOL_EVENT_KEYS } from '@dac-cloud/shared';

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

const JSON_BLOCK_PATTERN = /\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}/g;

// Extract all structured artifacts from a single stream line in one JSON parse pass
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

    return lines;
  }

  flush(): string[] {
    const remaining = this.buffer.trim();
    this.buffer = '';
    return remaining.length > 0 ? [remaining] : [];
  }
}
