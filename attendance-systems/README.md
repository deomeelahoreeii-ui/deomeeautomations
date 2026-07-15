# Attendance Apps Script Files

## Push with clasp

This folder is configured for `clasp` with project ID `1Eq1mUz9Pc7AiCJoUlQm6wWtt4VJ-k3luX-edDTKKBGV5r6AAWVpPgANL`.

Install dependencies once:

```bash
BUN_TMPDIR=/tmp BUN_INSTALL=/tmp/bun-install /home/ahmad/.bun/bin/bun install
```

Log in to Google once:

```bash
/home/ahmad/.bun/bin/bun run login
```

Push all Apps Script source files:

```bash
/home/ahmad/.bun/bin/bun run push:force
```

Important deployment note: `clasp push` updates the project files, but the public `/exec` web app URL keeps serving the last deployed version until you edit the deployment and select a new version. After pushing production changes, go to Apps Script -> Deploy -> Manage deployments -> Edit -> Version -> New version -> Deploy.

Only `.gs`, `.html`, and `appsscript.json` are uploaded. Local files like `ConsolidateAttendance.xlsx`, `README.md`, `package.json`, `bun.lock`, and `node_modules` are ignored.

## Manual setup

Create these `.gs` files in the Apps Script editor:

- `Config.gs`
- `Code.gs`
- `Participants.gs`
- `Attendance.gs`
- `BulkAttendance.gs`
- `ParticipantAdjustments.gs`
- `OriginalAllocations.gs`
- `Reports.gs`
- `Dashboard.gs`
- `Sheets.gs`
- `Cache.gs`
- `Audit.gs`
- `Users.gs`
- `AbsenceReasons.gs`
- `Utils.gs`

Create these HTML files in Apps Script. When creating an HTML file in Apps Script, use the name without `.html`:

- `Index` from `Index.html`
- `Styles` from `Styles.html`
- `MarkView` from `MarkView.html`
- `AdjustView` from `AdjustView.html`
- `DashboardView` from `DashboardView.html`
- `ReportsView` from `ReportsView.html`
- `AdminToolsView` from `AdminToolsView.html`
- `ClientScript` from `ClientScript.html`
- `AccessDenied` from `AccessDenied.html`

`Index.html` uses the Apps Script include helper in `Code.gs`, so all partial names must match exactly.

Run `setupAttendanceSystem()` once, approve permissions, then deploy as a Web App.

Performance notes:

- `setupAttendanceSystem()` is intentionally not run on every web request. Run it manually after deploying structural changes.
- Participant data, participant CNIC index, bootstrap lists, and dashboard summaries are cached with Apps Script `CacheService`.
- The `Users` sheet is not cached because role, active-state, and access changes must apply immediately.
- `setupAttendanceSystem()` syncs Google Drive permissions for every active user in `Users`.
- After editing the `Users` sheet later, run `syncUsersToSpreadsheetPermissions()` to sync Google Drive permissions without doing the full setup/repair flow.
- If you need to force all portal caches to refresh without changing data, run `clearAttendanceSystemCache()`.
- Dashboard summary totals are precomputed into `Attendance Dashboard Summary` and refreshed after attendance or adjustment writes.
- If participant rows are manually edited directly in the sheet, cache can take up to 10 minutes to expire. Run `setupAttendanceSystem()` to clear caches immediately.
- `Participant Original Allocation` is an immutable baseline used for adjusted-in/out and original-vs-current reports.
- The Reports page supports absence, attendance summary, adjusted-in, adjusted-out, and movement-history previews plus Excel export.
- `setupAttendanceSystem()` also creates/repairs the `Absence Reasons` sheet and seeds `Will Full Absent` plus `Other`.

For pre-production testing cleanup, run `resetAttendanceSystem()` from the Apps Script function dropdown. It restores participants moved through the adjustment module back to their earliest logged allocation, then clears:

- `Attendance Log`
- `Attendance Audit Log`
- `Participant Adjustment Log`

It does not delete users or settings.

After reset, you can also run `deleteAllSystemLogs()` to wipe remaining operational log/summary sheets while keeping users, settings, and participant data intact. Use this only after `resetAttendanceSystem()` if you need adjustment reversals, because reset needs `Participant Adjustment Log` to restore moved participants.

Access control notes:

- `setupAttendanceSystem()` creates a `Users` sheet.
- If the `Users` sheet is empty, the script effective user is seeded as `admin` with `Allowed Centers = *`.
- When the web app is deployed as `User accessing the web app`, each active user must also have Google Drive access to the spreadsheet. `setupAttendanceSystem()` syncs this during setup, and `syncUsersToSpreadsheetPermissions()` can be run after adding users later.
- If a user sees a Google Drive permission error, confirm their email is active in `Users`, run `syncUsersToSpreadsheetPermissions()`, then ask the user to fully reopen the portal URL. `clearAttendanceSystemCache()` is also safe to run after permission changes.
- For one user, run `diagnoseUserAccess("user@gmail.com")` to see whether they are active in `Users` and whether the spreadsheet is shared with them.
- To sync one user only, run `syncSingleUserToSpreadsheetPermissions("user@gmail.com")`.
- Admins can also use the web app `Admin Tools` tab to diagnose one user, sync one user, sync all users, run setup/repair, and clear cache without opening the Apps Script function dropdown.
- Each user may also need to approve the Apps Script OAuth scopes the first time they open the app. If Google shows an authorization screen, the user should allow it. Missing OAuth authorization is different from missing spreadsheet Drive access.
- Google will not show a spreadsheet Drive sharing request popup inside this web app. With `Execute as: User accessing the web app`, missing spreadsheet Drive access fails immediately until an admin shares the spreadsheet through the sync function.
- Roles are `viewer`, `editor`, and `admin`.
- `viewer` can view/search dashboard records only.
- `editor` can mark, bulk mark, and update attendance for allowed centers.
- `admin` can do everything, including delete.
- `Allowed Centers` can be `*`, one center, or comma-separated centers.

Trainer workflow notes:

- The selected center, batch, slot, and attendance date are saved in the browser.
- They stay selected for repeated CNIC entries until the focal person clicks `Reset`.
- Searching a CNIC shows the candidate details plus previous attendance records.
- Marking attendance writes to `Attendance Log` with duplicate protection for `CNIC + Attendance Date`.
- `Load Bulk` shows all expected candidates for the selected center, batch, slot, and date.
- Bulk rows default to `Present`; the focal person changes only absent candidates to `Absent`.
- When a bulk row is marked `Absent`, the focal person must select an absence reason. If `Other` is selected, a custom typed reason is required.
- Admins can add, edit, and delete absence reasons from the `Admin Tools` tab.
- Editing or deleting an absence reason affects only future attendance. Existing `Attendance Log` rows keep the exact reason text saved at the time.
- Deleted absence reasons are archived by setting them inactive; they are not physically removed from the `Absence Reasons` sheet.
- Absence reasons use stable internal keys, so setup does not recreate a default reason just because an admin renamed its display text.
- Archived absence reasons appear separately in Admin Tools and can be restored if needed.
- Bulk submit upserts records, so existing CNIC/date rows are updated instead of duplicated.
- `Attendance Log` includes `Attendance Status`, `Absence Reason`, and `Absence Reason Detail`.
- Previous attendance rows include `Edit` and `Delete` action buttons.
- `Adjust Participant` lets authorized users move a participant's current allocation to a valid center, batch, and slot.
- Participant adjustments update only the master participant allocation for future bulk attendance loading.
- Existing `Attendance Log` rows are never rewritten by participant adjustments; old attendance stays historical.
- `Participant Adjustment Log` records every move with full before/after JSON, changed-by email, role, reason, and old/new allocation details.
- The dashboard includes batch-wise and center-wise adjustment reports with moved-in, moved-out, and net-change counts.
- Feedback and confirmations use SweetAlert2 from `https://cdn.jsdelivr.net/npm/sweetalert2@11`.
- Create, update, and delete operations are recorded in `Attendance Audit Log`.

Useful deployed URLs:

- Trainer portal: `YOUR_WEB_APP_URL?view=mark`
- Center portal: `YOUR_WEB_APP_URL?view=mark&center=GGHS%20Walton`
- Center and batch portal: `YOUR_WEB_APP_URL?view=mark&center=GGHS%20Walton&batch=B1`
- Participant adjustment page: `YOUR_WEB_APP_URL?view=adjust`
- Admin tools page: `YOUR_WEB_APP_URL?view=admin`
- Lahore dashboard: `YOUR_WEB_APP_URL?view=dashboard`

trainer1@gmail.com | Trainer One | editor | GGHS Walton | TRUE | Walton trainer
viewer1@gmail.com | Viewer One | viewer | _ | TRUE | Lahore dashboard viewer
admin@gmail.com | Admin User | admin | _ | TRUE | Full access
