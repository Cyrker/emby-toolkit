import { describe, expect, it } from 'vitest';
import { p115RemainingSpace } from './p115Space';

describe('115 remaining space', () => {
  it('keeps provider formatting and supports a raw byte count including zero', () => {
    expect(p115RemainingSpace({ rt_space_info: { all_remain: { size_format: '1.2TB' } } })).toBe('1.2TB');
    expect(p115RemainingSpace({ rt_space_info: { all_remain: { size: '1099511627776' } } })).toBe('1.00 TiB');
    expect(p115RemainingSpace({ rt_space_info: { all_remain: { size: 0 } } })).toBe('0 B');
  });
  it('does not turn missing or malformed capacity into zero', () => {
    for (const size of [undefined, null, '', 'bad', -1, Infinity]) {
      expect(p115RemainingSpace({ rt_space_info: { all_remain: { size } } })).toContain('未获取');
    }
    expect(p115RemainingSpace(null)).toContain('未获取');
  });
});
