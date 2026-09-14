export function formatTaskLogs(logs: unknown): string {
  if (!Array.isArray(logs)) return ''
  return logs.map((entry: unknown) => {
    if (typeof entry === 'string') return entry
    if (!entry || typeof entry !== 'object') return ''
    const row = entry as Record<string, unknown>
    const message = typeof row.message === 'string' ? row.message : ''
    if (!message) return ''
    const timestamp = typeof row.timestamp === 'string' ? row.timestamp : ''
    const level = typeof row.level === 'string' ? `[${row.level.toUpperCase()}]` : ''
    return [timestamp, level, message].filter(Boolean).join(' ')
  }).filter(Boolean).join('\n')
}
