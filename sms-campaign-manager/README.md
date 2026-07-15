# SMS Campaign Manager

Durable Excel-to-SMS sender for [SMSGate for Android](https://docs.sms-gate.app/). It discovers workbook columns, normalizes Pakistani numbers to E.164, submits one recipient per request, and records every reservation/outcome in SQLite.

## Setup

```bash
cp .env.example .env
uv sync
uv run sms-campaign inspect --file ../add-to-whatsapp-group/lahore-ht-test.xlsx --json
uv run sms-campaign devices
```

Fill `.env` with your gateway endpoint and credentials. Do not commit it. Because credentials were visible in a screenshot, rotate the gateway password before use.

Private-server endpoints must end in `/api` (for example, `https://private.example/api`). The public cloud base URL is `https://api.sms-gate.app` and does not take the extra suffix.

Preview first:

```bash
uv run sms-campaign send --file ../add-to-whatsapp-group/lahore-ht-test.xlsx --column contact_no --message "Office update"
```

Execution additionally requires `--consent-confirmed --execute`. Defaults are conservative operational safeguards: 5 SMS segments/minute, 50/hour, 200/day and 12 seconds between recipients. Multipart messages consume multiple units. These are not statutory PTA quotas.

Campaign idempotency is recipient-based. Keep the same `--campaign-id` while adding rows or correcting the message; every number already queued, sent, delivered, failed, unknown, or interrupted is skipped. Use a new Campaign ID only when intentionally starting a new broadcast that may contact the same recipients again. When omitted, the stable identity is the resolved file path, sheet, and number column (file contents and message text are not part of it).

To retry failures safely, first run `sms-campaign reconcile`, review the phone numbers and gateway reasons, then preview the original campaign with `--retry-failed`. Execute only after verification. Retry mode selects only status `failed`, never queued/sent/delivered/unknown, and `--max-attempts` defaults to 3.

## Reliability and compliance

- A recipient is reserved in SQLite before the HTTP request. Gateway timeouts become `unknown` and are never automatically retried.
- Re-running the same file content, sheet, column, and message resumes only pending recipients.
- Use only consented office recipients; maintain an internal suppression list and honor opt-outs. A normal SIM/API does not provide a public DNCR lookup.
- For promotional campaigns, obtain advice from your telecom provider/PTA about registered sender/short-code requirements.

## Pakistan compliance note

Research performed on 2026-06-22 did not find an authoritative PTA publication defining a universal per-SIM numeric quota per minute, hour, or day. PTA's framework addresses spam/unsolicited communications, recipient protections, reporting and approved business messaging routes; operators may apply additional unpublished anti-abuse controls. The configurable defaults in this application are therefore conservative engineering safeguards, not a claim of legal compliance or a way to bypass carrier controls.

References: [PTA/NTCERT cyber-crime guidance](https://www.ntcert.pta.gov.pk/sops/FIA_Cyber_Crimes_Guideline.pdf), [Protection from Spam regulations reproduction](https://joshandmakinternational.com/resources/laws-of-pakistan/telecommunications-telecoms-laws-of-pakistan/the-protetection-from-spam-unsolicited-fraudulent-and-obnoxious-communication-regulations-2009/), and [SMSGate sending documentation](https://docs.sms-gate.app/features/sending-messages/).
