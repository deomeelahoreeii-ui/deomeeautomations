function setupAbsenceReasonsSheet_(ss) {
  const sheet = getOrCreateSheet_(ss, CONFIG.ABSENCE_REASONS_SHEET);
  ensureHeaders_(sheet, ABSENCE_REASON_COLUMNS, true);
  migrateAbsenceReasonKeys_(sheet);

  const existingKeys = {};
  readAbsenceReasonRows_(true).forEach(row => {
    if (row.reasonKey) existingKeys[row.reasonKey] = true;
  });

  const now = new Date();
  const email = getSessionEmail_(false) || getSessionEmail_(true) || 'system';
  const rows = DEFAULT_ABSENCE_REASONS
    .filter(item => !existingKeys[item.key])
    .map(item => buildAbsenceReasonSheetRow_(item.reason, true, email, now, now, email, '', '', item.key));

  if (rows.length) {
    sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, ABSENCE_REASON_COLUMNS.length).setValues(rows);
  }
  clearAbsenceReasonCaches_();
  return sheet;
}

function readAbsenceReasons_() {
  const cached = cacheGetJson_(CACHE_KEYS.ABSENCE_REASONS);
  if (cached) return cached;

  const active = readAbsenceReasonRows_(false);
  cachePutJson_(CACHE_KEYS.ABSENCE_REASONS, active);
  return active;
}

function readAbsenceReasonRows_(includeInactive) {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ABSENCE_REASONS_SHEET);
  if (!sheet || sheet.getLastRow() < 2) {
    return DEFAULT_ABSENCE_REASONS.map(item => absenceReasonObject_(item.reason, true, '', '', '', '', '', '', item.key));
  }

  const width = Math.min(Math.max(sheet.getLastColumn(), 1), ABSENCE_REASON_COLUMNS.length);
  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, width).getDisplayValues();
  const rows = values
    .map(row => padAbsenceReasonRow_(row))
    .filter(row => clean_(row[0]))
    .map(row => absenceReasonObject_(row[0], parseBoolean_(row[1]), row[2], row[3], row[4], row[5], row[6], row[7], row[8]));

  return includeInactive ? rows : rows.filter(row => row.active);
}

function getAbsenceReasonValues_() {
  return readAbsenceReasons_().map(row => row.reason);
}

function adminAddAbsenceReason(reason) {
  const user = requirePermission_('admin');
  reason = clean_(reason);
  if (!reason) {
    return { ok: false, message: 'Absence reason is required.' };
  }
  if (reason.toLowerCase() === 'other') {
    return { ok: false, message: 'Other already exists as the custom reason option.' };
  }

  const ss = getSpreadsheet_();
  const sheet = setupAbsenceReasonsSheet_(ss);
  const activeDuplicate = findAbsenceReasonByLabel_(reason, true, '');
  if (activeDuplicate) {
    return {
      ok: false,
      duplicate: true,
      message: 'An active absence reason already uses this name.',
      absenceReasons: readAbsenceReasons_(),
      absenceReasonCatalog: readAbsenceReasonRows_(true)
    };
  }
  const archivedDuplicate = findAbsenceReasonByLabel_(reason, false, '');
  if (archivedDuplicate) {
    return {
      ok: false,
      duplicate: true,
      archivedDuplicate: true,
      message: 'An archived absence reason already uses this name. Restore it instead of adding a new duplicate.',
      absenceReasons: readAbsenceReasons_(),
      absenceReasonCatalog: readAbsenceReasonRows_(true)
    };
  }

  const now = new Date();
  const reasonKey = makeUniqueAbsenceReasonKey_(reason);
  sheet.appendRow(buildAbsenceReasonSheetRow_(reason, true, user.email, now, now, user.email, '', '', reasonKey));
  clearAbsenceReasonCaches_();
  return absenceReasonAdminResult_('Absence reason added.');
}

function adminUpdateAbsenceReason(reasonKey, newReason) {
  const user = requirePermission_('admin');
  reasonKey = clean_(reasonKey);
  newReason = clean_(newReason);
  if (!reasonKey || !newReason) {
    return { ok: false, message: 'Absence reason key and new reason are required.' };
  }
  if (newReason.toLowerCase() === 'other') {
    return { ok: false, message: 'Other is reserved for custom reasons.' };
  }

  const ss = getSpreadsheet_();
  const sheet = setupAbsenceReasonsSheet_(ss);
  const rowInfo = findAbsenceReasonRowByKey_(sheet, reasonKey);
  if (!rowInfo) {
    return { ok: false, message: 'Absence reason was not found.' };
  }
  const current = absenceReasonObject_(rowInfo.values[0], parseBoolean_(rowInfo.values[1]), rowInfo.values[2], rowInfo.values[3], rowInfo.values[4], rowInfo.values[5], rowInfo.values[6], rowInfo.values[7], rowInfo.values[8]);
  if (current.protected) {
    return { ok: false, message: current.reason + ' is a protected system reason and cannot be edited.' };
  }
  if (!current.active) {
    return { ok: false, message: 'Archived absence reasons cannot be edited. Restore it first.' };
  }
  if (current.reason.toLowerCase() === newReason.toLowerCase()) {
    return { ok: false, message: 'Type a different reason before updating.' };
  }

  const activeDuplicate = findAbsenceReasonByLabel_(newReason, true, reasonKey);
  if (activeDuplicate) {
    return { ok: false, message: 'Another active absence reason already uses this name.' };
  }

  const now = new Date();
  sheet.getRange(rowInfo.rowNumber, 1).setValue(newReason);
  sheet.getRange(rowInfo.rowNumber, 5, 1, 2).setValues([[now, user.email]]);
  clearAbsenceReasonCaches_();
  return absenceReasonAdminResult_('Absence reason updated for future attendance only. Historical attendance rows were not changed.');
}

function adminArchiveAbsenceReason(reasonKey) {
  const user = requirePermission_('admin');
  reasonKey = clean_(reasonKey);
  if (!reasonKey) {
    return { ok: false, message: 'Absence reason key is required.' };
  }

  const ss = getSpreadsheet_();
  const sheet = setupAbsenceReasonsSheet_(ss);
  const rowInfo = findAbsenceReasonRowByKey_(sheet, reasonKey);
  if (!rowInfo) {
    return { ok: false, message: 'Absence reason was not found.' };
  }
  const current = absenceReasonObject_(rowInfo.values[0], parseBoolean_(rowInfo.values[1]), rowInfo.values[2], rowInfo.values[3], rowInfo.values[4], rowInfo.values[5], rowInfo.values[6], rowInfo.values[7], rowInfo.values[8]);
  if (current.protected) {
    return { ok: false, message: current.reason + ' is a protected system reason and cannot be deleted.' };
  }
  if (!current.active) {
    return absenceReasonAdminResult_('Absence reason is already archived.');
  }

  const now = new Date();
  sheet.getRange(rowInfo.rowNumber, 2).setValue('FALSE');
  sheet.getRange(rowInfo.rowNumber, 5, 1, 4).setValues([[now, user.email, now, user.email]]);
  clearAbsenceReasonCaches_();
  return absenceReasonAdminResult_('Absence reason archived for future attendance only. Historical attendance rows were not changed.');
}

function adminRestoreAbsenceReason(reasonKey) {
  const user = requirePermission_('admin');
  reasonKey = clean_(reasonKey);
  if (!reasonKey) {
    return { ok: false, message: 'Absence reason key is required.' };
  }

  const ss = getSpreadsheet_();
  const sheet = setupAbsenceReasonsSheet_(ss);
  const rowInfo = findAbsenceReasonRowByKey_(sheet, reasonKey);
  if (!rowInfo) {
    return { ok: false, message: 'Absence reason was not found.' };
  }
  const current = absenceReasonObject_(rowInfo.values[0], parseBoolean_(rowInfo.values[1]), rowInfo.values[2], rowInfo.values[3], rowInfo.values[4], rowInfo.values[5], rowInfo.values[6], rowInfo.values[7], rowInfo.values[8]);
  const activeDuplicate = findAbsenceReasonByLabel_(current.reason, true, reasonKey);
  if (activeDuplicate) {
    return { ok: false, message: 'Cannot restore because an active absence reason already uses this name.' };
  }

  const now = new Date();
  sheet.getRange(rowInfo.rowNumber, 2).setValue('TRUE');
  sheet.getRange(rowInfo.rowNumber, 5, 1, 4).setValues([[now, user.email, '', '']]);
  clearAbsenceReasonCaches_();
  return absenceReasonAdminResult_('Absence reason restored for future attendance.');
}

function findAbsenceReasonByLabel_(reason, activeOnly, excludeKey) {
  const normalized = normalizeAbsenceReasonLabel_(reason);
  return readAbsenceReasonRows_(true).find(row => {
    if (excludeKey && row.reasonKey === excludeKey) return false;
    if (activeOnly && !row.active) return false;
    return normalizeAbsenceReasonLabel_(row.reason) === normalized;
  }) || null;
}

function findAbsenceReasonRowByKey_(sheet, reasonKey) {
  if (!sheet || sheet.getLastRow() < 2) return null;
  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, ABSENCE_REASON_COLUMNS.length).getDisplayValues();
  for (let i = 0; i < values.length; i++) {
    const row = padAbsenceReasonRow_(values[i]);
    if (clean_(row[8]) === reasonKey) {
      return {
        rowNumber: i + 2,
        values: row
      };
    }
  }
  return null;
}

function normalizeAbsenceReasonPayload_(attendanceStatus, reason, detail, existingReason) {
  const status = normalizeAttendanceStatus_(attendanceStatus);
  if (status !== 'Absent') {
    return { absenceReason: '', absenceReasonDetail: '' };
  }

  reason = clean_(reason);
  detail = clean_(detail);
  existingReason = clean_(existingReason);
  if (!reason) {
    throw new Error('Absence reason is required for every absent candidate.');
  }

  const allowed = getAbsenceReasonValues_();
  const match = allowed.find(value => value.toLowerCase() === reason.toLowerCase());
  if (!match && existingReason && existingReason.toLowerCase() === reason.toLowerCase()) {
    return {
      absenceReason: existingReason,
      absenceReasonDetail: reason.toLowerCase() === 'other' ? detail : clean_(detail)
    };
  }
  if (!match) {
    throw new Error('Invalid absence reason: ' + reason);
  }

  if (match.toLowerCase() === 'other' && !detail) {
    throw new Error('Type the custom reason when Other is selected.');
  }

  return {
    absenceReason: match,
    absenceReasonDetail: match.toLowerCase() === 'other' ? detail : ''
  };
}

function migrateAbsenceReasonKeys_(sheet) {
  if (!sheet || sheet.getLastRow() < 2) return;

  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, ABSENCE_REASON_COLUMNS.length).getDisplayValues()
    .map(row => padAbsenceReasonRow_(row));
  const used = {};
  values.forEach(row => {
    const key = clean_(row[8]);
    if (key) used[key] = true;
  });

  const defaultCandidates = {};
  DEFAULT_ABSENCE_REASONS.forEach(item => defaultCandidates[item.key] = []);
  values.forEach((row, index) => {
    if (clean_(row[8])) return;
    const labelKey = defaultKeyForAbsenceReason_(row[0]);
    if (labelKey) {
      defaultCandidates[labelKey].push({ index, active: parseBoolean_(row[1]) });
    }
  });

  Object.keys(defaultCandidates).forEach(key => {
    if (used[key]) return;
    const candidates = defaultCandidates[key];
    if (!candidates.length) return;
    const preferred = candidates.find(item => item.active) || candidates[0];
    values[preferred.index][8] = key;
    used[key] = true;
  });

  values.forEach(row => {
    if (clean_(row[8])) return;
    const key = makeUniqueAbsenceReasonKeyFromUsed_(row[0], used);
    row[8] = key;
    used[key] = true;
  });

  sheet.getRange(2, 1, values.length, ABSENCE_REASON_COLUMNS.length).setValues(values);
}

function defaultKeyForAbsenceReason_(reason) {
  const compact = compactAbsenceReasonLabel_(reason);
  const match = DEFAULT_ABSENCE_REASONS.find(item => compactAbsenceReasonLabel_(item.reason) === compact);
  return match ? match.key : '';
}

function makeUniqueAbsenceReasonKey_(reason) {
  const used = {};
  readAbsenceReasonRows_(true).forEach(row => {
    if (row.reasonKey) used[row.reasonKey] = true;
  });
  return makeUniqueAbsenceReasonKeyFromUsed_(reason, used);
}

function makeUniqueAbsenceReasonKeyFromUsed_(reason, used) {
  let base = clean_(reason).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
  if (!base) base = 'absence_reason';
  let key = base;
  let i = 2;
  while (used[key]) {
    key = base + '_' + i;
    i++;
  }
  return key;
}

function absenceReasonAdminResult_(message) {
  return {
    ok: true,
    message,
    absenceReasons: readAbsenceReasons_(),
    absenceReasonCatalog: readAbsenceReasonRows_(true)
  };
}

function buildAbsenceReasonSheetRow_(reason, active, createdBy, createdAt, updatedAt, updatedBy, archivedAt, archivedBy, reasonKey) {
  return [reason, active ? 'TRUE' : 'FALSE', createdBy, createdAt, updatedAt, updatedBy, archivedAt, archivedBy, reasonKey];
}

function absenceReasonObject_(reason, active, createdBy, createdAt, updatedAt, updatedBy, archivedAt, archivedBy, reasonKey) {
  reason = clean_(reason);
  reasonKey = clean_(reasonKey);
  const defaultItem = DEFAULT_ABSENCE_REASONS.find(item => item.key === reasonKey);
  return {
    reason,
    active: Boolean(active),
    createdBy: clean_(createdBy),
    createdAt: clean_(createdAt),
    updatedAt: clean_(updatedAt),
    updatedBy: clean_(updatedBy),
    archivedAt: clean_(archivedAt),
    archivedBy: clean_(archivedBy),
    reasonKey,
    protected: Boolean(defaultItem && defaultItem.protected)
  };
}

function padAbsenceReasonRow_(row) {
  row = row || [];
  while (row.length < ABSENCE_REASON_COLUMNS.length) row.push('');
  return row;
}

function normalizeAbsenceReasonLabel_(reason) {
  return clean_(reason).toLowerCase();
}

function compactAbsenceReasonLabel_(reason) {
  return clean_(reason).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function clearAbsenceReasonCaches_() {
  cacheRemove_(CACHE_KEYS.ABSENCE_REASONS);
  cacheRemove_(CACHE_KEYS.BOOTSTRAP_BASE);
}
