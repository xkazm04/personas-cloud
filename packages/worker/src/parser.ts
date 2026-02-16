import type { Logger } from 'pino';

export interface ParsedLine {
  raw: string;
  type?: string;
  sessionId?: string;
  durationMs?: number;
  totalCostUsd?: number;
  isResult?: boolean;
}

export interface PersonaProtocolEvent {
  eventType: 'manual_review' | 'user_message' | 'persona_action' | 'emit_event';
  payload: unknown;
}

// Parse a single line of Claude CLI stream-json output
export function parseStreamLine(line: string): ParsedLine {
  const result: ParsedLine = { raw: line };

  try {
    const parsed = JSON.parse(line);
    result.type = parsed.type;

    // Extract session ID from various locations
    if (parsed.session_id) {
      result.sessionId = parsed.session_id;
    }
    if (parsed.result?.session_id) {
      result.sessionId = parsed.result.session_id;
    }

    // Extract completion data
    if (parsed.type === 'result') {
      result.isResult = true;
      result.durationMs = parsed.duration_ms || parsed.result?.duration_ms;
      result.totalCostUsd = parsed.total_cost_usd || parsed.result?.total_cost_usd;
    }
  } catch {
    // Not valid JSON — raw text line, that's fine
  }

  return result;
}

// Detect persona protocol events embedded in assistant text
export function detectPersonaEvents(line: string, logger: Logger): PersonaProtocolEvent[] {
  const events: PersonaProtocolEvent[] = [];

  try {
    const parsed = JSON.parse(line);

    // Check for persona events in assistant message content blocks
    if (parsed.type === 'assistant' && parsed.message?.content) {
      for (const block of parsed.message.content) {
        if (block.type === 'text' && block.text) {
          const extracted = extractJsonBlocks(block.text, logger);
          events.push(...extracted);
        }
      }
    }
  } catch {
    // Not JSON or not assistant message
  }

  return events;
}

function extractJsonBlocks(text: string, logger: Logger): PersonaProtocolEvent[] {
  const events: PersonaProtocolEvent[] = [];
  const protocolKeys = ['manual_review', 'user_message', 'persona_action', 'emit_event'] as const;

  // Find JSON-like blocks in text
  const jsonPattern = /\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}/g;
  const matches = text.match(jsonPattern);

  if (!matches) return events;

  for (const match of matches) {
    try {
      const obj = JSON.parse(match);

      for (const key of protocolKeys) {
        if (obj[key]) {
          events.push({
            eventType: key,
            payload: obj[key],
          });
          logger.info({ eventType: key }, 'Detected persona protocol event');
        }
      }
    } catch {
      // Not valid JSON block
    }
  }

  return events;
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
