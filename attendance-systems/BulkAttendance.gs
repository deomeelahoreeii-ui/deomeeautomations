function getBulkAttendanceCandidates(payload) {
  payload = payload || {};

  const center = clean_(payload.center);
  const batch = clean_(payload.batch);
  const slot = clean_(payload.slot);
  const attendanceDate = clean_(payload.attendanceDate);
  if (!center || !batch || !slot || !isIsoDate_(attendanceDate)) {
    return { ok: false, message: 'Select center, batch, slot, and valid attendance date.' };
  }
  requirePermission_('bulk', center);

  const existingByCnic = getAttendanceByCnicForDate_(attendanceDate);
  const candidates = readParticipants_()
    .filter(row => row.trainingCenter === center && row.batch === batch && row.slot === slot)
    .map(row => {
      const existing = existingByCnic[row.cnic];
      return {
        sn: row.sn,
        cnic: row.cnic,
        teacherName: row.teacherName,
        school: row.school,
        contact: row.contact,
        registeredCenter: row.trainingCenter,
        registeredBatch: row.batch,
        registeredSlot: row.slot,
        attendanceDate,
        attendanceKey: existing ? existing.attendanceKey : row.cnic + '|' + attendanceDate,
        attendanceStatus: existing ? normalizeAttendanceStatus_(existing.attendanceStatus) : 'Present',
        absenceReason: existing ? clean_(existing.absenceReason) : '',
        absenceReasonDetail: existing ? clean_(existing.absenceReasonDetail) : '',
        alreadySaved: Boolean(existing),
        notes: existing ? existing.notes : ''
      };
    });

  return {
    ok: true,
    center,
    batch,
    slot,
    attendanceDate,
    total: candidates.length,
    present: candidates.filter(row => row.attendanceStatus === 'Present').length,
    absent: candidates.filter(row => row.attendanceStatus === 'Absent').length,
    candidates
  };
}

function submitBulkAttendance(payload) {
  payload = payload || {};

  const center = clean_(payload.center);
  const batch = clean_(payload.batch);
  const slot = clean_(payload.slot);
  const attendanceDate = clean_(payload.attendanceDate);
  const records = Array.isArray(payload.records) ? payload.records : [];
  if (!center || !batch || !slot || !isIsoDate_(attendanceDate)) {
    return { ok: false, message: 'Select center, batch, slot, and valid attendance date.' };
  }
  if (!records.length) {
    return { ok: false, message: 'No candidates found to submit.' };
  }
  const user = requirePermission_('bulk', center);

  const lock = getWriteLock_();
  if (!lock.tryLock(20000)) {
    return { ok: false, message: 'Attendance system is busy. Please try again in a few seconds.' };
  }

  const sheet = getAttendanceSheetForWrite_();
  try {
    const expectedByCnic = {};
    readParticipants_()
      .filter(row => row.trainingCenter === center && row.batch === batch && row.slot === slot)
      .forEach(row => expectedByCnic[row.cnic] = row);

    const rowInfoByKey = getAttendanceRowInfoByKey_();
    const preparedRecords = [];
    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const cnic = normalizeCnic_(record.cnic);
      const candidate = expectedByCnic[cnic];
      if (!candidate) continue;

      const status = normalizeAttendanceStatus_(record.attendanceStatus);
      const key = cnic + '|' + attendanceDate;
      const existing = rowInfoByKey[key];
      let absenceInfo;
      try {
        absenceInfo = normalizeAbsenceReasonPayload_(
          status,
          record.absenceReason,
          record.absenceReasonDetail,
          existing && existing.record ? existing.record.absenceReason : ''
        );
      } catch (error) {
        return {
          ok: false,
          message: (candidate.teacherName || cnic) + ': ' + (error && error.message ? error.message : String(error))
        };
      }
      preparedRecords.push({ record, cnic, candidate, status, absenceInfo });
    }

    const rowsToAppend = [];
    let created = 0;
    let updated = 0;
    let present = 0;
    let absent = 0;

    preparedRecords.forEach(item => {
      const record = item.record;
      const cnic = item.cnic;
      const candidate = item.candidate;
      const status = item.status;
      if (status === 'Present') present++;
      if (status === 'Absent') absent++;

      const key = cnic + '|' + attendanceDate;
      const existing = rowInfoByKey[key];
      const row = buildAttendanceRow_(candidate, {
        attendanceDate,
        markedCenter: center,
        markedBatch: batch,
        markedSlot: slot,
        notes: clean_(record.notes),
        attendanceKey: key,
        attendanceStatus: status,
        absenceReason: item.absenceInfo.absenceReason,
        absenceReasonDetail: item.absenceInfo.absenceReasonDetail,
        markedBy: user.email
      });

      if (existing) {
        const before = existing.record;
        sheet.getRange(existing.rowNumber, 1, 1, ATTENDANCE_COLUMNS.length).setValues([row]);
        const after = objectFromHeaders_(ATTENDANCE_COLUMNS, row);
        logAudit_('BULK_UPDATE', key, cnic, attendanceDate, center, before, after);
        updated++;
      } else {
        rowsToAppend.push(row);
        logAudit_('BULK_CREATE', key, cnic, attendanceDate, center, null, objectFromHeaders_(ATTENDANCE_COLUMNS, row));
        created++;
      }
    });

    if (rowsToAppend.length) {
      sheet.getRange(sheet.getLastRow() + 1, 1, rowsToAppend.length, ATTENDANCE_COLUMNS.length).setValues(rowsToAppend);
    }
    refreshDashboardSummaries_();

    return {
      ok: true,
      message: 'Bulk attendance saved.',
      total: created + updated,
      created,
      updated,
      present,
      absent
    };
  } finally {
    lock.releaseLock();
  }
}

function getAttendanceByCnicForDate_(attendanceDate) {
  const out = {};
  readAttendance_().forEach(record => {
    if (record.attendanceDate === attendanceDate) out[record.cnic] = record;
  });
  return out;
}

function getAttendanceRowInfoByKey_() {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ATTENDANCE_SHEET);
  const out = {};
  if (!sheet || sheet.getLastRow() < 2) return out;

  const values = getAttendanceDisplayRows_(sheet);
  values.forEach((row, i) => {
    const key = clean_(row[19]);
    if (!key) return;
    out[key] = {
      rowNumber: i + 2,
      record: objectFromHeaders_(ATTENDANCE_COLUMNS, row)
    };
  });
  return out;
}
