function setupUsersSheet_(ss) {
  const sheet = getOrCreateSheet_(ss, CONFIG.USERS_SHEET);
  ensureHeaders_(sheet, USER_COLUMNS, true);

  if (sheet.getLastRow() < 2) {
    const seedEmail = getSessionEmail_(true);
    if (seedEmail) {
      const now = new Date();
      sheet.appendRow([
        seedEmail,
        'Initial Admin',
        ROLES.ADMIN,
        '*',
        'TRUE',
        'Auto-created by setupAttendanceSystem(). Review this row before production use.',
        now,
        now
      ]);
      clearUserCaches_();
    }
  }

  return sheet;
}

function getCurrentUser_() {
  const email = getSessionEmail_(false);
  if (!email) {
    throw new Error('Unable to identify your Google account. Deploy the web app for signed-in Google users, then open it while signed in.');
  }

  const users = readUsers_();
  const user = users.find(row => row.email === email && row.active);
  if (!user) {
    throw new Error('Access denied for ' + email + '. Ask an admin to add this email in the Users sheet.');
  }
  return user;
}

function readUsers_() {
  // This sheet is intentionally not cached. User access, role, and active-state
  // changes must take effect immediately for a deployed attendance portal.
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.USERS_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return [];

  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, USER_COLUMNS.length).getDisplayValues();
  return values
    .filter(row => clean_(row[0]))
    .map(row => ({
      email: clean_(row[0]).toLowerCase(),
      name: clean_(row[1]),
      role: normalizeRole_(row[2]),
      allowedCentersRaw: clean_(row[3]),
      allowedCenters: parseAllowedCenters_(row[3]),
      active: parseBoolean_(row[4]),
      notes: clean_(row[5])
    }));
}

function userPublic_(user) {
  return {
    email: user.email,
    name: user.name,
    role: user.role,
    allowedCenters: user.allowedCenters,
    canViewDashboard: canUser_(user, 'view'),
    canMark: canUser_(user, 'mark'),
    canAdjust: canUser_(user, 'adjust'),
    canEdit: canUser_(user, 'edit'),
    canDelete: canUser_(user, 'delete'),
    canAdmin: user.role === ROLES.ADMIN
  };
}

function requirePermission_(permission, center) {
  const user = getCurrentUser_();
  if (!canUser_(user, permission)) {
    throw new Error('Access denied. Your role (' + user.role + ') cannot perform this action.');
  }
  if (center) assertCenterAccess_(user, center);
  return user;
}

function assertCenterAccess_(user, center) {
  center = clean_(center);
  if (!center) throw new Error('Center is required.');
  if (user.allowedCenters.indexOf('*') !== -1) return true;
  if (user.allowedCenters.indexOf(center) !== -1) return true;
  throw new Error('Access denied. Your account is not allowed to use center: ' + center);
}

function canUser_(user, permission) {
  if (!user || !user.active) return false;
  if (user.role === ROLES.ADMIN) return true;
  if (user.role === ROLES.EDITOR) {
    return ['view', 'search', 'mark', 'bulk', 'edit', 'adjust'].indexOf(permission) !== -1;
  }
  if (user.role === ROLES.VIEWER) {
    return ['view', 'search'].indexOf(permission) !== -1;
  }
  return false;
}

function filterCentersForUser_(centers, user) {
  if (user.allowedCenters.indexOf('*') !== -1) return centers;
  return centers.filter(center => user.allowedCenters.indexOf(center) !== -1);
}

function filterRecordsForUser_(records, user) {
  if (user.allowedCenters.indexOf('*') !== -1) return records;
  return records.filter(record => user.allowedCenters.indexOf(record.markedCenter) !== -1);
}

function getAuditUserContext_() {
  try {
    return getCurrentUser_();
  } catch (error) {
    const email = getSessionEmail_(false) || getSessionEmail_(true) || '';
    return {
      email,
      name: '',
      role: 'unknown',
      allowedCenters: [],
      active: false
    };
  }
}

function getSessionEmail_(allowEffective) {
  const active = Session.getActiveUser().getEmail();
  if (active) return clean_(active).toLowerCase();
  if (allowEffective) {
    const effective = Session.getEffectiveUser().getEmail();
    if (effective) return clean_(effective).toLowerCase();
  }
  return '';
}

function normalizeRole_(value) {
  value = clean_(value).toLowerCase();
  if (value === ROLES.ADMIN) return ROLES.ADMIN;
  if (value === ROLES.EDITOR) return ROLES.EDITOR;
  return ROLES.VIEWER;
}

function parseAllowedCenters_(value) {
  value = clean_(value);
  if (!value || value === '*') return ['*'];
  return value.split(',').map(item => clean_(item)).filter(Boolean);
}

function parseBoolean_(value) {
  value = clean_(value).toLowerCase();
  return ['true', 'yes', 'y', '1', 'active'].indexOf(value) !== -1;
}

function syncUsersToSpreadsheetPermissions() {
  requireAdminForConfiguredSystem_();
  clearUserCaches_();
  const ss = getSpreadsheet_();
  const file = DriveApp.getFileById(ss.getId());
  const users = readUsers_().filter(user => user.active && user.email);
  const report = {
    ok: true,
    spreadsheetId: ss.getId(),
    editorsAdded: [],
    viewersAdded: [],
    skipped: []
  };

  users.forEach(user => {
    try {
      if (user.role === ROLES.VIEWER) {
        file.addViewer(user.email);
        report.viewersAdded.push(user.email);
      } else {
        file.addEditor(user.email);
        report.editorsAdded.push(user.email);
      }
    } catch (error) {
      report.skipped.push({
        email: user.email,
        role: user.role,
        message: error && error.message ? error.message : String(error)
      });
    }
  });

  clearUserCaches_();
  return report;
}

function syncSingleUserToSpreadsheetPermissions(email) {
  requireAdminForConfiguredSystem_();
  clearUserCaches_();
  email = clean_(email).toLowerCase();
  if (!email) throw new Error('Email is required.');

  const ss = getSpreadsheet_();
  const file = DriveApp.getFileById(ss.getId());
  const user = readUsers_().find(row => row.email === email);
  if (!user) {
    throw new Error('User is not listed in the Users sheet: ' + email);
  }
  if (!user.active) {
    throw new Error('User is listed in Users but is not active: ' + email);
  }

  const permission = user.role === ROLES.VIEWER ? 'viewer' : 'editor';
  try {
    if (permission === 'viewer') file.addViewer(email);
    else file.addEditor(email);
  } catch (error) {
    throw new Error('Could not grant spreadsheet ' + permission + ' access to ' + email + '. Original error: ' + (error && error.message ? error.message : String(error)));
  }

  clearUserCaches_();
  return diagnoseUserAccess(email);
}

function diagnoseUserAccess(email) {
  requireAdminForConfiguredSystem_();
  email = clean_(email).toLowerCase();
  if (!email) throw new Error('Email is required.');

  const ss = getSpreadsheet_();
  const file = DriveApp.getFileById(ss.getId());
  const user = readUsers_().find(row => row.email === email);
  const driveAccess = getSpreadsheetDriveAccessForEmail_(file, email);

  return {
    ok: Boolean(user && user.active && driveAccess.hasAccess),
    email,
    listedInUsers: Boolean(user),
    activeInUsers: Boolean(user && user.active),
    role: user ? user.role : '',
    allowedCenters: user ? user.allowedCenters.join(', ') : '',
    spreadsheetAccess: driveAccess.access,
    appAuthorization: 'User must approve Apps Script scopes on first open',
    appAuthorizationAdminAction: 'none',
    message: buildUserAccessDiagnosisMessage_(email, user, driveAccess)
  };
}

function getSpreadsheetDriveAccessForEmail_(file, email) {
  const ownerEmail = clean_(file.getOwner() && file.getOwner().getEmail()).toLowerCase();
  if (ownerEmail === email) return { hasAccess: true, access: 'owner' };

  const editors = file.getEditors().map(user => clean_(user.getEmail()).toLowerCase());
  if (editors.indexOf(email) !== -1) return { hasAccess: true, access: 'editor' };

  const viewers = file.getViewers().map(user => clean_(user.getEmail()).toLowerCase());
  if (viewers.indexOf(email) !== -1) return { hasAccess: true, access: 'viewer' };

  return { hasAccess: false, access: 'none' };
}

function buildUserAccessDiagnosisMessage_(email, user, driveAccess) {
  if (!user) return email + ' is not listed in the Users sheet.';
  if (!user.active) return email + ' is listed in Users but Active is not TRUE.';
  if (!driveAccess.hasAccess) {
    return email + ' is active in Users but does not have Google Drive access to the spreadsheet. Run syncSingleUserToSpreadsheetPermissions("' + email + '") or syncUsersToSpreadsheetPermissions().';
  }
  return email + ' is active in Users and has spreadsheet Drive access as ' + driveAccess.access + '. If Google asks for app authorization on first open, the user must allow it.';
}

function adminDiagnoseUserAccess(email) {
  requirePermission_('admin');
  const result = diagnoseUserAccess(email);
  result.ok = true;
  result.ready = Boolean(result.listedInUsers && result.activeInUsers && result.spreadsheetAccess !== 'none');
  return result;
}

function adminSyncUserAccess(email) {
  requirePermission_('admin');
  return syncSingleUserToSpreadsheetPermissions(email);
}

function adminSyncAllUsers() {
  requirePermission_('admin');
  return syncUsersToSpreadsheetPermissions();
}

function adminRunSetup() {
  requirePermission_('admin');
  return setupAttendanceSystem();
}

function adminClearCache() {
  requirePermission_('admin');
  return clearAttendanceSystemCache();
}

function adminToolAction(action, payload) {
  requirePermission_('admin');
  action = clean_(action);
  payload = payload || {};

  if (action === 'diagnoseUser') return adminDiagnoseUserAccess(payload.email);
  if (action === 'syncUser') return adminSyncUserAccess(payload.email);
  if (action === 'syncAllUsers') return adminSyncAllUsers();
  if (action === 'runSetup') return adminRunSetup();
  if (action === 'clearCache') return adminClearCache();
  if (action === 'addAbsenceReason') return adminAddAbsenceReason(payload.reason);
  if (action === 'updateAbsenceReason') return adminUpdateAbsenceReason(payload.reasonKey, payload.newReason);
  if (action === 'archiveAbsenceReason') return adminArchiveAbsenceReason(payload.reasonKey);
  if (action === 'restoreAbsenceReason') return adminRestoreAbsenceReason(payload.reasonKey);

  throw new Error('Unknown admin tool action: ' + action);
}
