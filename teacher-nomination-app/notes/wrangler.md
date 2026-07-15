bunx wrangler d1 execute pectaa_db --local --file schema.sql --yes
bun seed.ts

bunx wrangler d1 execute pectaa_db --local --command="SELECT COUNT(\*) AS count FROM teachers;" --yes

✔ Wrangler detected the following AI coding agents: Codex, GitHub Copilot, Gemini CLI, Antigravity. Would you like to install Cloudflare skills for them? … yes
Installing cloudflare/skills...
Resolved cloudflare/skills -> main (branch)

bun run build
bunx wrangler d1 execute pectaa_db --local --file schema.sql --yes

https://console.cloud.google.com/auth/clients/993104029957-aufamdrskv0mhic8c72ukhfpna607sni.apps.googleusercontent.com?organizationId=0&project=abiding-bongo-498119-c1

PUBLIC_CLERK_PUBLISHABLE_KEY=pk_test_dHJ1c3Rpbmcta3JpbGwtOTYuY2xlcmsuYWNjb3VudHMuZGV2JA
CLERK_SECRET_KEY=sk_test_OIlwBaBtJjcjKVZvv0hU6nYUN4cy0yo6YqEZQ5eSRy
