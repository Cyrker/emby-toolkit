import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'

const root = fileURLToPath(new URL('../..', import.meta.url))
const sourcePath = join(root, 'verification/contracts/http/p4-openapi.json')
const outputPath = join(root, 'frontend/src/api/generated/schema.ts')

function collectAndRewrite(node, schemas) {
  if (Array.isArray(node)) {
    node.forEach((item) => collectAndRewrite(item, schemas))
    return
  }
  if (!node || typeof node !== 'object') return
  if (node.$defs && typeof node.$defs === 'object') {
    for (const [name, definition] of Object.entries(node.$defs)) {
      if (!(name in schemas)) schemas[name] = definition
    }
    delete node.$defs
  }
  if (typeof node.$ref === 'string' && node.$ref.startsWith('#/$defs/')) {
    node.$ref = `#/components/schemas/${node.$ref.slice('#/$defs/'.length)}`
  }
  Object.values(node).forEach((value) => collectAndRewrite(value, schemas))
}

const source = JSON.parse(await readFile(sourcePath, 'utf8'))
const schemas = source.components?.schemas ?? (source.components = { schemas: {} }).schemas
collectAndRewrite(source, schemas)

const temporaryDir = await mkdtemp(join(tmpdir(), 'etk-openapi-'))
const temporarySpec = join(temporaryDir, 'openapi.json')
await writeFile(temporarySpec, JSON.stringify(source, null, 2), 'utf8')

const npmExecutable = process.platform === 'win32' ? 'npx.cmd' : 'npx'
const child = spawn(npmExecutable, ['openapi-typescript', temporarySpec, '-o', outputPath], {
  cwd: join(root, 'frontend'),
  stdio: 'inherit',
})
const exitCode = await new Promise((resolve, reject) => {
  child.on('error', reject)
  child.on('exit', (code) => resolve(code ?? 1))
})
await rm(temporaryDir, { recursive: true, force: true })
if (exitCode !== 0) process.exit(exitCode)
