import { GroupDispatchError } from './group-dispatcher';

/** Conservative bounded estimate of retained request data, without serialization. */
export function requestSize(
  value: unknown,
  limit: number,
  retainBacking = false,
  backing?: {
    retained: ReadonlyMap<ArrayBufferLike, number>;
    found: Set<ArrayBufferLike>;
  },
): number {
  let size = 0;
  const seen = new WeakSet<object>();
  const add = (amount: number) => {
    size += amount;
    if (size > limit) throw new GroupDispatchError('capacity');
  };
  const visit = (item: unknown, depth: number): void => {
    add(32);
    if (typeof item === 'string') {
      add(item.length * 3);
      return;
    }
    if (!item || typeof item !== 'object') return;
    if (item instanceof Uint8Array) {
      // A small view can keep its entire backing allocation alive.
      if (retainBacking && backing) {
        if (!backing.found.has(item.buffer)) {
          backing.found.add(item.buffer);
          if (!backing.retained.has(item.buffer)) add(item.buffer.byteLength);
        }
      } else {
        add(retainBacking ? item.buffer.byteLength : item.byteLength);
      }
      return;
    }
    if (depth > 64 || seen.has(item)) throw new GroupDispatchError('capacity');
    seen.add(item);
    for (const key in item) {
      if (!Object.prototype.hasOwnProperty.call(item, key)) continue;
      add(key.length * 3 + 32);
      visit((item as Record<string, unknown>)[key], depth + 1);
    }
  };
  visit(value, 0);
  return size;
}
