/**
 * PECTAA Lahore Attendance Portal
 *
 * Apps Script files are split by responsibility. Paste each local file into
 * Apps Script with the same filename shown here.
 */

function doGet(e) {
  const params = (e && e.parameter) || {};
  const view = String(params.view || 'mark').toLowerCase();
  try {
    requirePermission_(view === 'admin' ? 'admin' : 'view');
    assertSpreadsheetAccess_();
  } catch (error) {
    if (isAuthorizationRequiredError_(error)) {
      throw error;
    }
    return renderAccessDenied_(error, e);
  }

  const pageConfig = {
    title: CONFIG.PORTAL_TITLE,
    view: ['dashboard', 'adjust', 'reports', 'admin'].indexOf(view) !== -1 ? view : 'mark',
    center: params.center || '',
    batch: params.batch || '',
    slot: params.slot || '',
    date: params.date || today_(),
    hasCenterParam: Boolean(params.center),
    hasBatchParam: Boolean(params.batch),
    hasSlotParam: Boolean(params.slot),
    hasDateParam: Boolean(params.date),
    cityLabel: CONFIG.CITY_LABEL
  };

  const template = HtmlService.createTemplateFromFile('Index');
  template.pageConfigJson = JSON.stringify(pageConfig).replace(/</g, '\\u003c');

  return template
    .evaluate()
    .setTitle(CONFIG.PORTAL_TITLE)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function renderAccessDenied_(error, e) {
  const retryUrl = getRequestUrl_(e);
  const template = HtmlService.createTemplateFromFile('AccessDenied');
  template.title = CONFIG.PORTAL_TITLE;
  template.email = getSessionEmail_(false) || getSessionEmail_(true) || 'Unknown account';
  template.message = error && error.message ? error.message : String(error);
  template.retryUrl = retryUrl;
  template.switchAccountUrl = 'https://accounts.google.com/AccountChooser?continue=' + encodeURIComponent(retryUrl);

  return template
    .evaluate()
    .setTitle('Access not configured - ' + CONFIG.PORTAL_TITLE)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getRequestUrl_(e) {
  const baseUrl = ScriptApp.getService().getUrl() || '';
  const params = (e && e.parameters) || {};
  const parts = [];
  Object.keys(params).sort().forEach(key => {
    (params[key] || []).forEach(value => {
      parts.push(encodeURIComponent(key) + '=' + encodeURIComponent(value));
    });
  });
  return baseUrl + (parts.length ? '?' + parts.join('&') : '');
}

function requireAdminForConfiguredSystem_() {
  const ss = getSpreadsheet_();
  const usersSheet = ss.getSheetByName(CONFIG.USERS_SHEET);
  if (usersSheet && usersSheet.getLastRow() >= 2) {
    requirePermission_('admin');
  }
}

function setupAttendanceSystem() {
  requireAdminForConfiguredSystem_();
  clearDataCaches_();
  const ss = getSpreadsheet_();
  const participantsSheet = getParticipantsSheet_();
  ensureHeaders_(participantsSheet, PARTICIPANT_COLUMNS, false);
  setupOriginalAllocationSheet_(ss);

  const attendanceSheet = getOrCreateSheet_(ss, CONFIG.ATTENDANCE_SHEET);
  ensureHeaders_(attendanceSheet, ATTENDANCE_COLUMNS, true);

  const auditSheet = getOrCreateSheet_(ss, CONFIG.AUDIT_SHEET);
  ensureHeaders_(auditSheet, AUDIT_COLUMNS, true);

  const adjustmentSheet = getOrCreateSheet_(ss, CONFIG.ADJUSTMENT_SHEET);
  ensureHeaders_(adjustmentSheet, ADJUSTMENT_COLUMNS, true);

  const summarySheet = getOrCreateSheet_(ss, CONFIG.SUMMARY_SHEET);
  ensureHeaders_(summarySheet, DASHBOARD_SUMMARY_COLUMNS, true);

  setupUsersSheet_(ss);
  setupAbsenceReasonsSheet_(ss);
  const permissionSync = syncUsersToSpreadsheetPermissions();

  const settingsSheet = getOrCreateSheet_(ss, CONFIG.SETTINGS_SHEET);
  if (settingsSheet.getLastRow() < 1) {
    settingsSheet.getRange(1, 1, 1, 2).setValues([['Setting', 'Value']]).setFontWeight('bold');
    settingsSheet.getRange(2, 1, 4, 2).setValues([
      ['Notes', 'This sheet is created by the attendance portal.'],
      ['Duplicate rule', 'One attendance record per CNIC per Attendance Date.'],
      ['Participant primary key', 'CNIC'],
      ['Dashboard view', 'Use ?view=dashboard in the deployed web app URL.']
    ]);
    settingsSheet.autoResizeColumns(1, 2);
  }
  refreshDashboardSummaries_();

  return {
    ok: true,
    participantRows: Math.max(0, participantsSheet.getLastRow() - 1),
    attendanceRows: Math.max(0, attendanceSheet.getLastRow() - 1),
    permissionSync
  };
}

function resetAttendanceSystem() {
  requireAdminForConfiguredSystem_();
  const lock = getWriteLock_();
  if (!lock.tryLock(30000)) {
    throw new Error('Attendance system is busy. Please try reset again in a few seconds.');
  }

  try {
    setupAttendanceSystem();
    restoreParticipantsFromAdjustmentLog_();
    clearParticipantCaches_();

    const ss = getSpreadsheet_();
    clearSheetData_(ss.getSheetByName(CONFIG.ATTENDANCE_SHEET), ATTENDANCE_COLUMNS);
    clearSheetData_(ss.getSheetByName(CONFIG.AUDIT_SHEET), AUDIT_COLUMNS);
    clearSheetData_(ss.getSheetByName(CONFIG.ADJUSTMENT_SHEET), ADJUSTMENT_COLUMNS);
    refreshDashboardSummaries_();
    clearDataCaches_();

    return {
      ok: true,
      message: 'Attendance, audit, and participant adjustment test data has been reset.',
      participantRows: Math.max(0, getParticipantsSheet_().getLastRow() - 1)
    };
  } finally {
    lock.releaseLock();
  }
}

function deleteAllSystemLogs() {
  requireAdminForConfiguredSystem_();
  const lock = getWriteLock_();
  if (!lock.tryLock(30000)) {
    throw new Error('Attendance system is busy. Please try deleting logs again in a few seconds.');
  }

  try {
    const ss = getSpreadsheet_();
    clearSheetData_(ss.getSheetByName(CONFIG.AUDIT_SHEET), AUDIT_COLUMNS);
    clearSheetData_(ss.getSheetByName(CONFIG.ADJUSTMENT_SHEET), ADJUSTMENT_COLUMNS);
    clearSheetData_(ss.getSheetByName(CONFIG.SUMMARY_SHEET), DASHBOARD_SUMMARY_COLUMNS);
    refreshDashboardSummaries_();
    clearDataCaches_();

    return {
      ok: true,
      message: 'System logs have been deleted. Attendance data was not modified by this function.'
    };
  } finally {
    lock.releaseLock();
  }
}

function clearAttendanceSystemCache() {
  requireAdminForConfiguredSystem_();
  clearDataCaches_();
  return {
    ok: true,
    message: 'Attendance portal cache cleared. Reopen the web app to fetch fresh users, participants, dashboard, and bootstrap data.'
  };
}

function diagnoseDtsUserAccess() {
  return diagnoseUserAccess('dtsclahoret2@gmail.com');
}

function syncDtsUserSpreadsheetPermissions() {
  return syncSingleUserToSpreadsheetPermissions('dtsclahoret2@gmail.com');
}

function clearSheetData_(sheet, headers) {
  if (!sheet) return;
  sheet.clear();
  if (sheet.getMaxColumns() < headers.length) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), headers.length - sheet.getMaxColumns());
  }
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  ensureHeaders_(sheet, headers, true);
  const extraColumns = sheet.getLastColumn() - headers.length;
  if (extraColumns > 0) {
    sheet.deleteColumns(headers.length + 1, extraColumns);
  }
}

function getBootstrap(context) {
  const user = requirePermission_('view');
  assertSpreadsheetAccess_();
  const base = getBootstrapBase_();
  const centers = filterCentersForUser_(base.centers, user);
  const allocations = filterAllocationsForUser_(base.allocations, user);

  return {
    ok: true,
    context: context || {},
    today: today_(),
    totalCandidates: base.totalCandidates,
    user: userPublic_(user),
    centers,
    batches: base.batches,
    slots: base.slots,
    allocations,
    dateRanges: base.dateRanges,
    absenceReasons: readAbsenceReasons_(),
    absenceReasonCatalog: user.role === ROLES.ADMIN ? readAbsenceReasonRows_(true) : []
  };
}

function getBootstrapBase_() {
  const cached = cacheGetJson_(CACHE_KEYS.BOOTSTRAP_BASE);
  if (cached) return cached;

  const participants = readParticipants_();
  const base = {
    totalCandidates: participants.length,
    centers: uniqueSorted_(participants.map(row => row.trainingCenter)),
    batches: uniqueSorted_(participants.map(row => row.batch)),
    slots: uniqueSorted_(participants.map(row => row.slot)),
    allocations: buildValidAllocationOptions_(participants),
    dateRanges: uniqueSorted_(participants.map(row => batchDateLabel_(row)).filter(Boolean))
  };
  cachePutJson_(CACHE_KEYS.BOOTSTRAP_BASE, base);
  return base;
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}
