# ETK Frontend

The ETK frontend keeps the existing Vue 3/Naive UI experience and is built as
strict TypeScript at the project boundary. API types are generated once from
the frozen backend contract at
`verification/contracts/http/p4-openapi.json`; generated files under
`src/api/generated/` are never edited by hand.

```bash
npm ci
npm run typecheck
npm run test:unit
npm run build
npm run test:e2e
```

All page API calls use the adapter in `src/api/`. The adapter preserves the
legacy response envelope, credentials, upload/download behavior, and bounded
timeouts while Vue Query owns server-state caching and invalidation. Pinia is
limited to authentication and client-only UI state.
