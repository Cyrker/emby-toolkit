import { describe, expect, it } from 'vitest'
import { formatTaskLogs } from './taskLogs'

describe('task log rendering', () => {
  it('renders structured native logs without implicit object conversion', () => {
    expect(formatTaskLogs([
      { timestamp: '2026-09-12T04:00:00+00:00', level: 'info', message: '正在同步媒体元数据 (1/2)' },
      { level: 'error', message: '请求失败\n详细信息' },
    ])).toBe('2026-09-12T04:00:00+00:00 [INFO] 正在同步媒体元数据 (1/2)\n[ERROR] 请求失败\n详细信息')
  })

  it('retains legacy lines and ignores malformed records', () => {
    expect(formatTaskLogs(['旧日志', null, {}, 1, { message: {} }, { message: '完成' }])).toBe('旧日志\n完成')
    expect(formatTaskLogs(undefined)).toBe('')
  })
})
