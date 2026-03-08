/**
 * Lightweight 5-field cron next-time calculator.
 *
 * Supports standard cron syntax:
 *   minute hour day-of-month month day-of-week
 *
 * Supported field values:
 *   - Specific values: 5, 10
 *   - Wildcards: *
 *   - Ranges: 1-5
 *   - Steps: *​/15, 1-30/5
 *   - Lists: 1,5,10
 *   - Combined: 1-5,10,15-20/2
 *
 * Also supports the shorthand "every Xs/Xm/Xh/Xd" syntax used by
 * the Personas trigger UI.
 */

/** Minimum allowed interval between trigger firings (60 seconds). */
const MIN_INTERVAL_MS = 60_000;

/**
 * Compute the next trigger time from a cron expression or interval shorthand.
 *
 * @param cron - A standard 5-field cron expression or "every Xm" shorthand
 * @param from - Base date to compute from (default: now)
 * @returns ISO string of next trigger time, or null if unparseable
 */
export function computeNextCron(cron: string, from?: Date): string | null {
  const trimmed = cron.trim();

  // Handle shorthand: "every Xm", "every Xh", "every Xs", "every Xd"
  const shorthand = trimmed.match(/^every\s+(\d+)([smhd])$/i);
  if (shorthand) {
    const value = parseInt(shorthand[1]!, 10);
    const unit = shorthand[2]!.toLowerCase();
    const multipliers: Record<string, number> = { s: 1000, m: 60000, h: 3600000, d: 86400000 };
    const ms = Math.max(value * (multipliers[unit] ?? 60000), MIN_INTERVAL_MS);
    return new Date((from ?? new Date()).getTime() + ms).toISOString();
  }

  // Standard 5-field cron: minute hour dom month dow
  const fields = trimmed.split(/\s+/);
  if (fields.length !== 5) return null;

  const minuteSet = parseField(fields[0]!, 0, 59);
  const hourSet = parseField(fields[1]!, 0, 23);
  const domSet = parseField(fields[2]!, 1, 31);
  const monthSet = parseField(fields[3]!, 1, 12);
  const dowSet = parseField(fields[4]!, 0, 6);

  if (!minuteSet || !hourSet || !domSet || !monthSet || !dowSet) return null;

  const base = from ?? new Date();
  // Start searching from the next minute
  const candidate = new Date(base);
  candidate.setSeconds(0, 0);
  candidate.setMinutes(candidate.getMinutes() + 1);

  // Search up to 366 days ahead to find a match
  const limit = 366 * 24 * 60; // max minutes to search
  for (let i = 0; i < limit; i++) {
    const month = candidate.getMonth() + 1; // 1-12
    const dom = candidate.getDate();
    const dow = candidate.getDay(); // 0=Sunday
    const hour = candidate.getHours();
    const minute = candidate.getMinutes();

    if (
      monthSet.has(month) &&
      domSet.has(dom) &&
      dowSet.has(dow) &&
      hourSet.has(hour) &&
      minuteSet.has(minute)
    ) {
      // Enforce minimum interval
      const diff = candidate.getTime() - base.getTime();
      if (diff >= MIN_INTERVAL_MS) {
        return candidate.toISOString();
      }
    }

    candidate.setMinutes(candidate.getMinutes() + 1);
  }

  // No match found within search window
  return null;
}

/**
 * Parse a single cron field into a set of allowed integer values.
 *
 * Supports: *, specific values, ranges (1-5), steps (*​/15, 1-30/5), lists (1,5,10).
 */
function parseField(field: string, min: number, max: number): Set<number> | null {
  const result = new Set<number>();

  // Split by comma for list support
  const parts = field.split(',');

  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) return null;

    // Step: */N or M-N/S
    const stepMatch = trimmed.match(/^(\*|(\d+)-(\d+))\/(\d+)$/);
    if (stepMatch) {
      const step = parseInt(stepMatch[4]!, 10);
      if (step <= 0) return null;
      let start: number;
      let end: number;
      if (stepMatch[1] === '*') {
        start = min;
        end = max;
      } else {
        start = parseInt(stepMatch[2]!, 10);
        end = parseInt(stepMatch[3]!, 10);
      }
      if (start < min || end > max || start > end) return null;
      for (let v = start; v <= end; v += step) {
        result.add(v);
      }
      continue;
    }

    // Wildcard
    if (trimmed === '*') {
      for (let v = min; v <= max; v++) {
        result.add(v);
      }
      continue;
    }

    // Range: M-N
    const rangeMatch = trimmed.match(/^(\d+)-(\d+)$/);
    if (rangeMatch) {
      const start = parseInt(rangeMatch[1]!, 10);
      const end = parseInt(rangeMatch[2]!, 10);
      if (start < min || end > max || start > end) return null;
      for (let v = start; v <= end; v++) {
        result.add(v);
      }
      continue;
    }

    // Single value
    const singleMatch = trimmed.match(/^(\d+)$/);
    if (singleMatch) {
      const val = parseInt(singleMatch[1]!, 10);
      if (val < min || val > max) return null;
      result.add(val);
      continue;
    }

    // Unknown format
    return null;
  }

  return result.size > 0 ? result : null;
}
