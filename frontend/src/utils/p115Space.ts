export function p115RemainingSpace(userInfo: Record<string, any> | null | undefined): string {
  const remain = userInfo?.rt_space_info?.all_remain;
  if (typeof remain?.size_format === 'string' && remain.size_format.trim()) {
    return remain.size_format;
  }
  const raw = remain?.size;
  if ((typeof raw === 'number' || (typeof raw === 'string' && /^\d+$/.test(raw))) &&
      Number.isFinite(Number(raw)) && Number(raw) >= 0) {
    let size = Number(raw);
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit++; }
    return `${unit === 0 ? size : size.toFixed(2)} ${units[unit]}`;
  }
  return '未获取（请查看上方授权状态）';
}
