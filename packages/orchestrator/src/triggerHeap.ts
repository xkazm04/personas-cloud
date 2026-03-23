/**
 * In-memory min-heap keyed by next_trigger_at (epoch ms).
 *
 * The heap avoids full-table DB scans on every scheduler tick.
 * The database remains the source of truth — the heap is rebuilt on startup
 * from the DB and updated incrementally on trigger CRUD operations.
 */

export interface HeapEntry {
  triggerId: string;
  tenantId: string; // 'default' for single-tenant mode
  nextTriggerAt: number; // epoch ms
}

export class TriggerHeap {
  private heap: HeapEntry[] = [];
  /** triggerId → index in heap array. Enables O(log n) remove/update. */
  private index = new Map<string, number>();

  get size(): number {
    return this.heap.length;
  }

  /** Look at the earliest entry without removing it. */
  peek(): HeapEntry | undefined {
    return this.heap[0];
  }

  /** Add an entry (or update if triggerId already exists). */
  push(entry: HeapEntry): void {
    const existing = this.index.get(entry.triggerId);
    if (existing !== undefined) {
      // Update in place and re-heapify
      this.heap[existing] = entry;
      this.siftUp(existing);
      this.siftDown(existing);
      return;
    }
    const i = this.heap.length;
    this.heap.push(entry);
    this.index.set(entry.triggerId, i);
    this.siftUp(i);
  }

  /** Remove and return the earliest entry. */
  pop(): HeapEntry | undefined {
    if (this.heap.length === 0) return undefined;
    const top = this.heap[0]!;
    this.removeAt(0);
    return top;
  }

  /** Remove an entry by triggerId. Returns true if found. */
  remove(triggerId: string): boolean {
    const i = this.index.get(triggerId);
    if (i === undefined) return false;
    this.removeAt(i);
    return true;
  }

  /** Pop all entries whose nextTriggerAt <= now. Returns them in ascending order. */
  popDue(now: number): HeapEntry[] {
    const result: HeapEntry[] = [];
    while (this.heap.length > 0 && this.heap[0]!.nextTriggerAt <= now) {
      result.push(this.pop()!);
    }
    return result;
  }

  /** Remove all entries. */
  clear(): void {
    this.heap.length = 0;
    this.index.clear();
  }

  // ---- internal heap operations ----

  private removeAt(i: number): void {
    const last = this.heap.length - 1;
    const removed = this.heap[i]!;
    this.index.delete(removed.triggerId);

    if (i === last) {
      this.heap.pop();
      return;
    }

    // Move last element into the gap and re-heapify
    const moved = this.heap.pop()!;
    this.heap[i] = moved;
    this.index.set(moved.triggerId, i);
    this.siftUp(i);
    this.siftDown(i);
  }

  private siftUp(i: number): void {
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.heap[parent]!.nextTriggerAt <= this.heap[i]!.nextTriggerAt) break;
      this.swap(i, parent);
      i = parent;
    }
  }

  private siftDown(i: number): void {
    const n = this.heap.length;
    while (true) {
      let smallest = i;
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      if (left < n && this.heap[left]!.nextTriggerAt < this.heap[smallest]!.nextTriggerAt) {
        smallest = left;
      }
      if (right < n && this.heap[right]!.nextTriggerAt < this.heap[smallest]!.nextTriggerAt) {
        smallest = right;
      }
      if (smallest === i) break;
      this.swap(i, smallest);
      i = smallest;
    }
  }

  private swap(a: number, b: number): void {
    const entryA = this.heap[a]!;
    const entryB = this.heap[b]!;
    this.heap[a] = entryB;
    this.heap[b] = entryA;
    this.index.set(entryA.triggerId, b);
    this.index.set(entryB.triggerId, a);
  }
}
