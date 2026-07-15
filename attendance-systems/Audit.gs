function logAudit_(action, key, cnic, attendanceDate, center, before, after) {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.AUDIT_SHEET);
  if (!sheet) return;

  const user = getAuditUserContext_();
  sheet.appendRow([
    new Date(),
    action,
    key,
    cnic,
    attendanceDate,
    user.email,
    before ? JSON.stringify(before) : '',
    after ? JSON.stringify(after) : '',
    user.role,
    clean_(center)
  ]);
}
