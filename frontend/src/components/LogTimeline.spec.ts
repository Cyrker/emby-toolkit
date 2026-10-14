import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import LogTimeline from './LogTimeline.vue'
import { NSelect, NPagination } from 'naive-ui'

describe('visual logs', () => {
  it('shows business cards, collapses transport details and renders messages as text', () => {
    const wrapper = mount(LogTimeline, { props: { logs: [
      { level: 'INFO', message: '整理完成 <script>bad()</script>' },
      { level: 'INFO', source: 'httpx', message: 'HTTP Request: GET fixture "HTTP/1.1 200 OK"' },
      { level: 'ERROR', source: 'httpx', message: 'connection refused' },
    ] } })
    expect(wrapper.findAll('.log-scroll > .log-event')).toHaveLength(2)
    expect(wrapper.find('.technical-logs').attributes('open')).toBeUndefined()
    expect(wrapper.find('.technical-logs').findAll('article')).toHaveLength(0)
    expect(wrapper.find('script').exists()).toBe(false)
    expect(wrapper.text()).toContain('整理完成 <script>bad()</script>')
    wrapper.unmount()
  })

  it('bounds DOM size, filters all records and only mounts details on demand', async () => {
    const start = performance.now()
    const wrapper = mount(LogTimeline, { props: { logs: Array.from({ length: 4000 }, (_, i) => ({
      level: i === 1999 ? 'ERROR' : 'INFO',
      source: i >= 2000 ? 'httpx' : 'worker',
      message: i >= 2000 ? 'HTTP Request: GET fixture "HTTP/1.1 200 OK"' : `任务记录 ${i}`,
    })) } })
    console.info('log-render baseline', { ms: Math.round(performance.now() - start), articles: wrapper.findAll('article').length })
    expect(wrapper.findAll('article')).toHaveLength(50)
    expect(wrapper.findAll('pre')).toHaveLength(0)
    wrapper.findAllComponents(NSelect)[1].vm.$emit('update:value', 'ERROR')
    await wrapper.vm.$nextTick()
    expect(wrapper.findAll('article')).toHaveLength(1)
    expect(wrapper.find('article').text()).toContain('任务记录 1999')
    const details = wrapper.find('article details')
    ;(details.element as HTMLDetailsElement).open = true
    await details.trigger('toggle')
    expect(wrapper.findAll('pre')).toHaveLength(1)
    ;(details.element as HTMLDetailsElement).open = false
    await details.trigger('toggle')
    expect(wrapper.findAll('pre')).toHaveLength(0)
    wrapper.findAllComponents(NSelect)[1].vm.$emit('update:value', '')
    await wrapper.vm.$nextTick()
    const technical = wrapper.find('.technical-logs')
    ;(technical.element as HTMLDetailsElement).open = true
    await technical.trigger('toggle')
    expect(technical.findAll('article')).toHaveLength(50)
    expect(technical.findAll('pre')).toHaveLength(0)
    wrapper.unmount()
  })

  it('pages business records and resets the page when the filter changes', async () => {
    const wrapper = mount(LogTimeline, { props: { logs: Array.from({ length: 120 }, (_, i) => ({ level: 'ERROR', message: `失败 ${i}` })) } })
    wrapper.findComponent(NPagination).vm.$emit('update:page', 3)
    await wrapper.vm.$nextTick()
    expect(wrapper.findAll('article')).toHaveLength(20)
    expect(wrapper.find('article').text()).toContain('失败 100')
    wrapper.findAllComponents(NSelect)[1].vm.$emit('update:value', 'ERROR')
    await wrapper.vm.$nextTick()
    expect(wrapper.findAll('article')).toHaveLength(50)
    expect(wrapper.find('article').text()).toContain('失败 0')
    wrapper.unmount()
  })

  it('pages long error text without dropping the original message', async () => {
    const message = 'x'.repeat(20000) + '结尾'
    const wrapper = mount(LogTimeline, { props: { logs: [{ level: 'ERROR', message }] } })
    expect(wrapper.find('.log-summary').text().length).toBeLessThan(300)
    expect(wrapper.find('pre').exists()).toBe(false)
    const details = wrapper.find('article details')
    ;(details.element as HTMLDetailsElement).open = true
    await details.trigger('toggle')
    expect(wrapper.find('pre').text()).toHaveLength(4000)
    wrapper.findComponent(NPagination).vm.$emit('update:page', 6)
    await wrapper.vm.$nextTick()
    expect(wrapper.find('pre').text()).toBe('结尾')
    wrapper.unmount()
  })

  it('preserves pagination, filters, expanded details and scroll position across polling updates', async () => {
    const logs = [
      ...Array.from({ length: 120 }, (_, i) => ({ level: 'INFO', message: `任务记录 ${i}` })),
      ...Array.from({ length: 110 }, (_, i) => ({ level: 'INFO', source: 'httpx', message: `HTTP Request: GET fixture/${i} "HTTP/1.1 200 OK"` })),
    ]
    const wrapper = mount(LogTimeline, { props: { logs } })
    const selects = wrapper.findAllComponents(NSelect)
    selects[0].vm.$emit('update:value', '任务')
    selects[1].vm.$emit('update:value', 'INFO')
    await wrapper.vm.$nextTick()
    wrapper.findComponent(NPagination).vm.$emit('update:page', 2)
    await wrapper.vm.$nextTick()
    const detail = wrapper.find('article details')
    ;(detail.element as HTMLDetailsElement).open = true
    await detail.trigger('toggle')
    const technical = wrapper.find('.technical-logs')
    ;(technical.element as HTMLDetailsElement).open = true
    await technical.trigger('toggle')
    technical.findComponent(NPagination).vm.$emit('update:page', 2)
    await wrapper.vm.$nextTick()
    const scroll = wrapper.find('.log-scroll').element as HTMLElement
    scroll.scrollTop = 200

    for (const incoming of [logs.map(row => ({ ...row })), [...logs, { level: 'INFO', message: '任务新记录' }]]) {
      await wrapper.setProps({ logs: incoming })
      expect(wrapper.find('.log-scroll > article').text()).toContain('任务记录 50')
      expect(technical.find('article').text()).toContain('fixture/50')
      expect((technical.element as HTMLDetailsElement).open).toBe(true)
      expect(wrapper.find('.log-scroll > article pre').exists()).toBe(true)
      expect(selects[0].props('value')).toBe('任务')
      expect(selects[1].props('value')).toBe('INFO')
      expect(scroll.scrollTop).toBe(200)
    }
    wrapper.unmount()
  })

  it('only clamps pagination when refreshed logs no longer contain the selected page', async () => {
    const logs = Array.from({ length: 120 }, (_, i) => ({ level: 'INFO', message: `任务记录 ${i}` }))
    const wrapper = mount(LogTimeline, { props: { logs } })
    wrapper.findComponent(NPagination).vm.$emit('update:page', 3)
    await wrapper.vm.$nextTick()
    await wrapper.setProps({ logs: logs.slice(0, 70) })
    expect(wrapper.findComponent(NPagination).props('page')).toBe(2)
    expect(wrapper.find('article').text()).toContain('任务记录 50')
    await wrapper.setProps({ logs: [] })
    expect(wrapper.findAll('article')).toHaveLength(0)
    wrapper.unmount()
  })
})
