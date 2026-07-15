# AntiDengue PocketBase

Local PocketBase database for AntiDengue master data, report runs, and delivery audit history.

## Start

```bash
./pocketbase serve --http 127.0.0.1:8090
```

Admin dashboard:

```text
http://127.0.0.1:8090/_/
```

## Import Current Officers CSV

Apply migrations first:

```bash
./pocketbase --dir pb_data --migrationsDir pb_migrations migrate up
```

Then import the existing CSV:

```bash
python scripts/import_officers_csv.py
```

The importer upserts schools, officers, DDEO/AEO jurisdictions, fixed WhatsApp recipients, and an import batch record from `../antidengue/officers_list.csv` and `../antidengue/whatsapp_recipients.csv`.

## Schema Shape

Master data is normalized for production use:

- `districts`, `departments`, `wings`, `tehsils`, and `markazes` store the administrative hierarchy.
- `schools` keeps EMIS, name, shift/head fields, report-facing school fields, and relation fields to the hierarchy.
- `ddeo_officers` and `aeo_officers` are separate role-specific officer tables linked to their wing/tehsil/markaz context through relation fields.
- `ddeo_jurisdictions` maps each DDEO to their wing/tehsil responsibility.
- `aeo_jurisdictions` maps each AEO to their wing/tehsil/markaz responsibility.
- `school_ddeo_overrides` and `school_aeo_overrides` are only for exceptions where a single school does not follow the normal hierarchy.
- `whatsapp_recipients` stores fixed/group/contact recipients used by the runtime instead of reading the CSV during every run.
- Master-data collections are relation-only for administrative hierarchy. Duplicate text columns such as school `district`, `wing`, `tehsil`, and `markaz` are intentionally removed.
- `dormant_records` stores `school_ref` plus raw portal `row_data`; report history can be cleared/rebuilt because operational source files and generated Excel outputs remain on disk.

## Health Checks

The Automation Management GUI database screen checks for production-risk data issues:

- missing school/officer/jurisdiction relation refs
- schools that cannot resolve to a DDEO or AEO
- duplicate active DDEO/AEO jurisdiction owners
- invalid officer mobile numbers
- invalid or missing fixed WhatsApp recipients

Critical hierarchy and jurisdiction relation fields are required at the PocketBase schema level so new manual records cannot silently miss their district, wing, tehsil, markaz, or officer link.
