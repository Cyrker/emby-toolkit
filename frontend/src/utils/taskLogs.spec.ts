import { describe, expect, it } from 'vitest'
import { formatTaskLogs, parseTaskLogs } from './taskLogs'

describe('task log rendering', () => {
  it('collapses successful HTTP details but keeps failures and business events visible', () => {
    const rows = parseTaskLogs([
      { level: 'INFO', source: 'httpx', message: 'HTTP Request: GET https://fixture.invalid "HTTP/1.1 200 OK"' },
      { level: 'INFO', source: 'httpx', message: 'HTTP Request: GET https://fixture.invalid "HTTP/1.1 503 Unavailable"' },
      { level: 'INFO', message: '开始整理：示例影片' },
      { level: 'INFO', message: '正在刮削元数据' },
      { level: 'INFO', message: '播放已开始' },
      { level: 'ERROR', message: 'unknown service failed' },
    ])
    expect(rows.map(row => row.technical)).toEqual([true, false, false, false, false, false])
    expect(rows.map(row => row.category)).toEqual(['HTTP 请求', '异常', '整理与入库', '刮削与媒体', '播放', '异常'])
  })

  it('keeps traceback grouped and summarizes worker receipts without claiming business success', () => {
    const rows = parseTaskLogs('2026-09-17 12:00:00,100 - worker - ERROR - 刮削失败\nTraceback\nValueError: detail\n2026-09-17 12:00:01 - celery - INFO - Task etk.jobs.execute[1] succeeded in 2s: None')
    expect(rows).toHaveLength(2)
    expect(rows[0].message).toContain('Traceback\nValueError')
    expect(rows[1].summary).toContain('任务执行器已返回')
    expect(rows[1].message).toContain('succeeded in 2s')
  })
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
