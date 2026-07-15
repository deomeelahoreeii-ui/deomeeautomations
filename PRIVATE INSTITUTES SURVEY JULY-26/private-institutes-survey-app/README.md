# Private Institutes Survey July 2026

Astro + Cloudflare D1 app for collecting the columns from `PRIVATE INSTITUTES SURVEY JULY-26.xlsx`.

## Commands

```sh
bun install
bun run db:local:apply
bun run dev
```

Deploy:

```sh
bun run deploy:production
```

Admin dashboard:

```text
/admin
```

## Structure

```text
src/config/survey.ts              Survey steps, fields, D1 insert order, CSV headers
src/components/survey/            Public survey form UI components
src/lib/d1.ts                     Shared Cloudflare D1 binding helper
src/lib/http.ts                   JSON response/body helpers
src/lib/survey-validation.ts      Backend normalization and validation from field config
src/pages/index.astro             Public survey page shell
src/pages/api/survey.ts           Survey submission endpoint
src/pages/admin.astro             Dashboard and CSV export
```

To add or rename a survey field, start in `src/config/survey.ts`. The form, backend validation, insert order, and CSV export all read from that config.

Authentication is temporarily disabled with `AUTH_ENABLED="false"` in `wrangler.jsonc` and `.dev.vars`.
