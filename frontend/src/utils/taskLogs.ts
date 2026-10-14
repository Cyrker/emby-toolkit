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

export interface LogEntry {
  timestamp: string
  level: string
  source: string
  message: string
  summary: string
  category: string
  technical: boolean
}

export function parseTaskLogs(input: unknown): LogEntry[] {
  const chunks: unknown[] = typeof input === 'string' ? input.split('\n') : Array.isArray(input) ? input : []
  const rows: { timestamp: string; level: string; source: string; message: string }[] = []
  for (const chunk of chunks) {
    if (chunk && typeof chunk === 'object') {
      const row = chunk as Record<string, unknown>
      if (typeof row.message !== 'string' || !row.message.trim()) continue
      rows.push({ timestamp: String(row.timestamp || ''), level: String(row.level || 'INFO').toUpperCase(), source: String(row.source || ''), message: row.message })
      continue
    }
    if (typeof chunk !== 'string' || !chunk.trim()) continue
    const legacy = chunk.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:[,.]\d+)? - (.*?) - (DEBUG|INFO|WARNING|ERROR|CRITICAL) - ([\s\S]*)$/)
    const native = chunk.match(/^(\d{4}-\d{2}-\d{2}T\S+)\s+\[(debug|info|warning|error|critical)\s*\]\s+([\s\S]*)$/i)
    if (legacy) rows.push({ timestamp: legacy[1], source: legacy[2], level: legacy[3], message: legacy[4] })
    else if (native) rows.push({ timestamp: native[1], source: '', level: native[2].toUpperCase(), message: native[3] })
    else if (typeof input === 'string' && rows.length) rows[rows.length - 1].message += '\n' + chunk
    else rows.push({ timestamp: '', source: '', level: 'INFO', message: chunk })
  }
  return rows.map(row => {
    const newline = row.message.indexOf('\n')
    const headline = newline < 0 ? row.message : row.message.slice(0, newline)
    const http = /(?:^|\b)(?:HTTP Request:|HTTP Response:)|(?:httpx|httpcore|uvicorn\.access)/i.test(row.source + ' ' + headline)
      || /"(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+\S+.*HTTP\/\d/.test(headline)
    const httpFailure = http && /(?:HTTP\/[^"\s]+\s+|"\s+|status[=: ]+)[45]\d\d\b/i.test(headline)
    const error = ['ERROR', 'CRITICAL', 'WARNING'].includes(row.level) || httpFailure
    const text = row.source + ' ' + headline
    const category = error ? '异常' : http ? 'HTTP 请求'
      : /播放|playback|租约|转码/i.test(text) ? '播放'
      : /整理|organize|入库|STRM|MoviePilot|文件监控/i.test(text) ? '整理与入库'
      : /刮削|元数据|补图|图片|演员|metadata|reprocess/i.test(text) ? '刮削与媒体'
      : /任务|task|job|celery|调度/i.test(text) ? '任务' : '系统明细'
    const task = headline.match(/^Task\s+(\S+)\s+(received|succeeded|failed|retry)/i)
    const summary = task ? `${({ received: '任务已接收', succeeded: '任务执行器已返回', failed: '任务执行失败', retry: '任务正在重试' } as Record<string, string>)[task[2].toLowerCase()]} · ${task[1]}` : headline
    return { ...row, level: httpFailure ? 'ERROR' : row.level, summary: summary.length > 240 ? summary.slice(0, 240) + '…' : summary, category, technical: category === 'HTTP 请求' || category === '系统明细' }
  })
}
