import Database from 'better-sqlite3';

// ---------------------------------------------------------------------------
// Prepared statement cache — compile SQL once per (db, sql) pair
// ---------------------------------------------------------------------------

const _stmtCache = new WeakMap<Database.Database, Map<string, Database.Statement>>();

export function stmt(db: Database.Database, sql: string): Database.Statement {
  let map = _stmtCache.get(db);
  if (!map) {
    map = new Map();
    _stmtCache.set(db, map);
  }
  let s = map.get(sql);
  if (!s) {
    s = db.prepare(sql);
    map.set(sql, s);
  }
  return s;
}

// ---------------------------------------------------------------------------
// Dynamic WHERE-clause builder
// ---------------------------------------------------------------------------

/**
 * Build a WHERE clause from a filter object and a mapping of filter keys to SQL column names.
 * Only keys present (non-undefined) in `filters` are included.
 */
export function buildWhereClause(
  filters: Record<string, unknown>,
  columnMap: Record<string, string>,
): { sql: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];
  for (const [key, col] of Object.entries(columnMap)) {
    const value = filters[key];
    if (value !== undefined) {
      clauses.push(`${col} = ?`);
      params.push(value);
    }
  }
  const sql = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  return { sql, params };
}

// ---------------------------------------------------------------------------
// Merge-update helpers
// ---------------------------------------------------------------------------

/** Convert an optional boolean update into a SQLite 0/1 integer, falling back to the current value. */
export function mergeEnabled(update: boolean | null | undefined, current: boolean): number {
  return update !== undefined && update !== null ? (update ? 1 : 0) : (current ? 1 : 0);
}

/** Run an UPDATE … SET <columns>, updated_at = ? WHERE id = ? with the given column→value map. */
export function mergeUpdate(db: Database.Database, table: string, id: string, columns: Record<string, unknown>): void {
  const now = new Date().toISOString();
  const setClauses = Object.keys(columns).map(col => `${col} = ?`).join(', ');
  const values = Object.values(columns);
  stmt(db, `UPDATE ${table} SET ${setClauses}, updated_at = ? WHERE id = ?`).run(...values, now, id);
}

// ---------------------------------------------------------------------------
// Generic row mapper — replaces per-entity rowTo* boilerplate
// ---------------------------------------------------------------------------

export interface FieldSpec {
  col: string;
  bool?: boolean;
  json?: boolean;
  nullable?: boolean;
  optional?: boolean;
  default?: unknown;
}

export type ColumnMap<T> = { [K in keyof T]-?: FieldSpec };

export function createRowMapper<T>(columnMap: ColumnMap<T>): (row: Record<string, unknown>) => T {
  const entries = Object.entries(columnMap) as [string, FieldSpec][];
  return (row: Record<string, unknown>): T => {
    const result: Record<string, unknown> = {};
    for (const [key, spec] of entries) {
      const val = row[spec.col];
      if (spec.bool) {
        result[key] = val === 1;
      } else if (spec.json) {
        result[key] = JSON.parse((val as string) || (spec.default as string ?? '[]'));
      } else if (spec.nullable) {
        result[key] = val ?? null;
      } else if (spec.optional) {
        result[key] = val ?? undefined;
      } else if ('default' in spec) {
        result[key] = val ?? spec.default;
      } else {
        result[key] = val;
      }
    }
    return result as T;
  };
}
