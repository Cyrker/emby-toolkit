import { readdir, readFile } from 'node:fs/promises'
import { join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

const srcRoot = fileURLToPath(new URL('../src', import.meta.url))
const failures = []

async function visit(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) {
      if (entry.name !== 'api') await visit(path)
      continue
    }
    if (!/\.(vue|ts|js)$/.test(entry.name)) continue
    const source = await readFile(path, 'utf8')
    const file = relative(srcRoot, path)
    if (entry.name.endsWith('.js') && !['components/settings/quickDeployIntent.js', 'composables/configDiff.js'].includes(file)) {
      failures.push(`${file}: JavaScript source is outside the explicit Node-test compatibility allowlist`)
    }
    if (/\baxios\b/.test(source)) failures.push(`${file}: direct axios reference`)
    if (/\bfetch\s*\(/.test(source)) failures.push(`${file}: direct fetch call`)
    if (/(?:'|"|`)\/api\//.test(source)) failures.push(`${file}: literal API URL outside adapter`)
    if (entry.name.endsWith('.vue') && !source.includes('<script setup lang="ts">')) {
      failures.push(`${file}: Vue component is not using script setup lang=ts`)
    }
  }
}

await visit(srcRoot)
if (failures.length) {
  console.error(failures.join('\n'))
  process.exitCode = 1
} else {
  console.log('frontend_api_boundary=clean')
}
