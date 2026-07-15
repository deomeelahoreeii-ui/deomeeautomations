function markAttendance(payload) {
  payload = payload || {};
  const cnic = normalizeCnic_(payload.cnic);
  const attendanceDate = payload.attendanceDate || today_();
  if (!cnic) {
    return { ok: false, message: 'CNIC is required.' };
  }
  if (!isIsoDate_(attendanceDate)) {
    return { ok: false, message: 'Attendance date must be in YYYY-MM-DD format.' };
  }
  const user = requirePermission_('mark', payload.markedCenter);

  const candidate = findParticipantByCnic_(cnic);
  if (!candidate) {
    return { ok: false, message: 'No candidate found for this CNIC.' };
  }

  const lock = getWriteLock_();
  if (!lock.tryLock(10000)) {
    return { ok: false, message: 'Attendance system is busy. Please try again in a few seconds.' };
  }

  const attendanceSheet = getAttendanceSheetForWrite_();
  try {
    const key = cnic + '|' + attendanceDate;
    const existing = findAttendanceByKey_(attendanceSheet, key);
    if (existing) {
      return {
        ok: false,
        duplicate: true,
        message: 'Attendance already marked for this CNIC on ' + attendanceDate + '.',
        existing,
        previousAttendance: getCandidateAttendance_(cnic, 25)
      };
    }

    const row = [
      new Date(),
      attendanceDate,
      clean_(payload.markedCenter),
      clean_(payload.markedBatch),
      clean_(payload.markedSlot),
      cnic,
      candidate.teacherName,
      candidate.batch,
      candidate.groupCode,
      candidate.slot,
      candidate.sessionTime,
      candidate.startDate,
      candidate.endDate,
      candidate.school,
      candidate.tehsil,
      candidate.contact,
      candidate.trainingCenter,
      user.email,
      clean_(payload.notes),
      key,
      'Present',
      '',
      ''
    ];

    attendanceSheet.appendRow(row);
    const lastRow = attendanceSheet.getLastRow();
    attendanceSheet.getRange(lastRow, 1, 1, ATTENDANCE_COLUMNS.length).setBorder(true, true, true, true, true, true);
    const attendance = objectFromHeaders_(ATTENDANCE_COLUMNS, row);
    logAudit_('CREATE', key, cnic, attendanceDate, clean_(payload.markedCenter), null, attendance);
    refreshDashboardSummaries_();

    return {
      ok: true,
      message: 'Attendance marked for ' + candidate.teacherName + ' on ' + attendanceDate + '.',
      candidate,
      attendance,
      previousAttendance: getCandidateAttendance_(cnic, 25)
    };
  } finally {
    lock.releaseLock();
  }
}

function updateAttendance(payload) {
  payload = payload || {};

  const originalKey = clean_(payload.attendanceKey);
  const cnic = normalizeCnic_(payload.cnic);
  const attendanceDate = clean_(payload.attendanceDate);
  const attendanceStatus = normalizeAttendanceStatus_(payload.attendanceStatus || 'Present');
  if (!originalKey || !cnic) {
    return { ok: false, message: 'Attendance record key and CNIC are required.' };
  }
  if (!isIsoDate_(attendanceDate)) {
    return { ok: false, message: 'Attendance date must be in YYYY-MM-DD format.' };
  }

  const lock = getWriteLock_();
  if (!lock.tryLock(10000)) {
    return { ok: false, message: 'Attendance system is busy. Please try again in a few seconds.' };
  }

  const sheet = getAttendanceSheetForWrite_();
  try {
    const rowInfo = findAttendanceRowByKey_(sheet, originalKey);
    if (!rowInfo) {
      return { ok: false, message: 'Attendance record was not found. Refresh and try again.' };
    }
    const before = rowInfo.record;
    let absenceInfo;
    try {
      absenceInfo = normalizeAbsenceReasonPayload_(
        attendanceStatus,
        payload.absenceReason,
        payload.absenceReasonDetail,
        before.absenceReason
      );
    } catch (error) {
      return { ok: false, message: error && error.message ? error.message : String(error) };
    }
    const user = requirePermission_('edit', before.markedCenter);
    assertCenterAccess_(user, payload.markedCenter);

    const nextKey = cnic + '|' + attendanceDate;
    if (nextKey !== originalKey) {
      const duplicate = findAttendanceByKey_(sheet, nextKey);
      if (duplicate) {
        return {
          ok: false,
          duplicate: true,
          message: 'Another attendance record already exists for this CNIC on ' + attendanceDate + '.',
          previousAttendance: getCandidateAttendance_(cnic, 25)
        };
      }
    }

    const updates = [
      attendanceDate,
      clean_(payload.markedCenter),
      clean_(payload.markedBatch),
      clean_(payload.markedSlot)
    ];
    sheet.getRange(rowInfo.rowNumber, 2, 1, 4).setValues([updates]);
    sheet.getRange(rowInfo.rowNumber, 18).setValue(user.email);
    sheet.getRange(rowInfo.rowNumber, 19).setValue(clean_(payload.notes));
    sheet.getRange(rowInfo.rowNumber, 20).setValue(nextKey);
    sheet.getRange(rowInfo.rowNumber, 21).setValue(attendanceStatus);
    sheet.getRange(rowInfo.rowNumber, 22, 1, 2).setValues([[
      absenceInfo.absenceReason,
      absenceInfo.absenceReasonDetail
    ]]);
    const after = findAttendanceByKey_(sheet, nextKey);
    logAudit_('UPDATE', nextKey, cnic, attendanceDate, clean_(payload.markedCenter), before, after);
    refreshDashboardSummaries_();

    return {
      ok: true,
      message: 'Attendance record updated.',
      attendance: after,
      previousAttendance: getCandidateAttendance_(cnic, 25)
    };
  } finally {
    lock.releaseLock();
  }
}

function deleteAttendance(payload) {
  payload = payload || {};

  const key = clean_(payload.attendanceKey);
  const cnic = normalizeCnic_(payload.cnic);
  if (!key || !cnic) {
    return { ok: false, message: 'Attendance record key and CNIC are required.' };
  }
  const lock = getWriteLock_();
  if (!lock.tryLock(10000)) {
    return { ok: false, message: 'Attendance system is busy. Please try again in a few seconds.' };
  }

  const sheet = getAttendanceSheetForWrite_();
  try {
    const rowInfo = findAttendanceRowByKey_(sheet, key);
    if (!rowInfo) {
      return { ok: false, message: 'Attendance record was not found. Refresh and try again.' };
    }
    requirePermission_('delete', rowInfo.record.markedCenter);
    logAudit_('DELETE', key, cnic, rowInfo.record.attendanceDate, rowInfo.record.markedCenter, rowInfo.record, null);
    sheet.deleteRow(rowInfo.rowNumber);
    refreshDashboardSummaries_();

    return {
      ok: true,
      message: 'Attendance record deleted.',
      previousAttendance: getCandidateAttendance_(cnic, 25)
    };
  } finally {
    lock.releaseLock();
  }
}

function buildAttendanceRow_(candidate, payload) {
  const status = normalizeAttendanceStatus_(payload.attendanceStatus);
  const absenceInfo = normalizeAbsenceReasonPayload_(
    status,
    payload.absenceReason,
    payload.absenceReasonDetail
  );
  return [
    new Date(),
    clean_(payload.attendanceDate),
    clean_(payload.markedCenter),
    clean_(payload.markedBatch),
    clean_(payload.markedSlot),
    candidate.cnic,
    candidate.teacherName,
    candidate.batch,
    candidate.groupCode,
    candidate.slot,
    candidate.sessionTime,
    candidate.startDate,
    candidate.endDate,
    candidate.school,
    candidate.tehsil,
    candidate.contact,
    candidate.trainingCenter,
    clean_(payload.markedBy),
    clean_(payload.notes),
    clean_(payload.attendanceKey),
    status,
    absenceInfo.absenceReason,
    absenceInfo.absenceReasonDetail
  ];
}

function readAttendance_() {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ATTENDANCE_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return [];

  const values = getAttendanceDisplayRows_(sheet);
  return values
    .filter(row => clean_(row[5]))
    .map(row => ({
      timestamp: clean_(row[0]),
      attendanceDate: clean_(row[1]),
      markedCenter: clean_(row[2]),
      markedBatch: clean_(row[3]),
      markedSlot: clean_(row[4]),
      cnic: normalizeCnic_(row[5]),
      teacherName: clean_(row[6]),
      registeredBatch: clean_(row[7]),
      registeredGroupCode: clean_(row[8]),
      registeredSlot: clean_(row[9]),
      registeredSessionTime: clean_(row[10]),
      registeredStartDate: clean_(row[11]),
      registeredEndDate: clean_(row[12]),
      school: clean_(row[13]),
      tehsil: clean_(row[14]),
      contact: normalizePhone_(row[15]),
      registeredTrainingCenter: clean_(row[16]),
      markedBy: clean_(row[17]),
      notes: clean_(row[18]),
      attendanceKey: clean_(row[19]),
      attendanceStatus: normalizeAttendanceStatus_(row[20] || 'Present'),
      absenceReason: clean_(row[21]),
      absenceReasonDetail: clean_(row[22])
    }));
}

function findAttendanceByKey_(sheet, key) {
  const rowInfo = findAttendanceRowByKey_(sheet, key);
  return rowInfo ? rowInfo.record : null;
}

function findAttendanceRowByKey_(sheet, key) {
  if (!sheet || sheet.getLastRow() < 2) return null;

  const keyColumn = ATTENDANCE_COLUMNS.indexOf('Attendance Key') + 1;
  const range = sheet.getRange(2, keyColumn, sheet.getLastRow() - 1, 1);
  const match = range.createTextFinder(key).matchEntireCell(true).findNext();
  if (!match) return null;

  const rowValues = getAttendanceDisplayRow_(sheet, match.getRow());
  return {
    rowNumber: match.getRow(),
    record: objectFromHeaders_(ATTENDANCE_COLUMNS, rowValues)
  };
}

function getCandidateAttendance_(cnic, limit) {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ATTENDANCE_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return [];

  const normalized = normalizeCnic_(cnic);
  const cnicColumn = ATTENDANCE_COLUMNS.indexOf('CNIC') + 1;
  const range = sheet.getRange(2, cnicColumn, sheet.getLastRow() - 1, 1);
  const matches = range.createTextFinder(normalized).matchEntireCell(true).findAll();
  const start = Math.max(0, matches.length - (limit || 25));

  return matches.slice(start).reverse().map(match => {
    const rowValues = getAttendanceDisplayRow_(sheet, match.getRow());
    return objectFromHeaders_(ATTENDANCE_COLUMNS, rowValues);
  });
}

function getAttendanceDisplayRows_(sheet) {
  const width = Math.min(Math.max(sheet.getLastColumn(), 1), ATTENDANCE_COLUMNS.length);
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, width).getDisplayValues()
    .map(row => padAttendanceRow_(row));
}

function getAttendanceDisplayRow_(sheet, rowNumber) {
  const width = Math.min(Math.max(sheet.getLastColumn(), 1), ATTENDANCE_COLUMNS.length);
  return padAttendanceRow_(sheet.getRange(rowNumber, 1, 1, width).getDisplayValues()[0]);
}

function padAttendanceRow_(row) {
  row = row || [];
  while (row.length < ATTENDANCE_COLUMNS.length) row.push('');
  return row;
}

function getAttendanceSheetForWrite_() {
  const ss = getSpreadsheet_();
  const sheet = getOrCreateSheet_(ss, CONFIG.ATTENDANCE_SHEET);
  ensureHeaders_(sheet, ATTENDANCE_COLUMNS, true);
  return sheet;
}
