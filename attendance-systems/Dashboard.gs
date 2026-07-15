function getDashboardData(filters) {
  filters = filters || {};
  const user = requirePermission_('view', filters.center || '');
  const hasLiveFilters = Boolean(filters.date || filters.center || filters.batch || filters.slot);

  if (!hasLiveFilters && user.allowedCenters.indexOf('*') !== -1) {
    return getPrecomputedDashboardData_(user);
  }

  const records = filterRecordsForUser_(readAttendance_(), user).filter(record => {
    if (filters.date && record.attendanceDate !== filters.date) return false;
    if (filters.center && record.markedCenter !== filters.center) return false;
    if (filters.batch && record.markedBatch !== filters.batch) return false;
    if (filters.slot && record.markedSlot !== filters.slot) return false;
    return true;
  });

  const participants = readParticipants_();
  const adjustmentData = getParticipantAdjustmentDashboard({
    center: filters.center,
    batch: filters.batch
  });
  const summaryByCenter = summarizeBy_(records, 'markedCenter');
  const summaryByBatch = summarizeBy_(records, 'markedBatch');
  const recentRecords = records.slice(-100).reverse();

  return {
    ok: true,
    generatedAt: Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss'),
    totalCandidates: participants.length,
    totalAttendance: records.length,
    uniqueAttendees: uniqueSorted_(records.map(row => row.cnic)).length,
    centers: filterCentersForUser_(uniqueSorted_(participants.map(row => row.trainingCenter).concat(records.map(row => row.markedCenter))), user),
    batches: uniqueSorted_(participants.map(row => row.batch).concat(records.map(row => row.markedBatch))),
    slots: uniqueSorted_(participants.map(row => row.slot).concat(records.map(row => row.markedSlot))),
    summaryByCenter,
    summaryByBatch,
    recentRecords,
    adjustmentSummaryByBatch: adjustmentData.summaryByBatch,
    adjustmentSummaryByCenter: adjustmentData.summaryByCenter,
    recentAdjustments: adjustmentData.recentAdjustments
  };
}

function getPrecomputedDashboardData_(user) {
  const participants = readParticipants_();
  const summary = readDashboardSummaries_();
  const records = filterRecordsForUser_(getRecentAttendanceRecords_(150), user);
  const adjustments = filterAdjustmentsForUser_(getRecentAdjustmentRecords_(150), user);

  return {
    ok: true,
    generatedAt: summary.generatedAt || Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss'),
    totalCandidates: participants.length,
    totalAttendance: Number(summary.meta.totalAttendance || 0),
    uniqueAttendees: Number(summary.meta.uniqueAttendees || 0),
    centers: filterCentersForUser_(uniqueSorted_(participants.map(row => row.trainingCenter)), user),
    batches: uniqueSorted_(participants.map(row => row.batch)),
    slots: uniqueSorted_(participants.map(row => row.slot)),
    summaryByCenter: filterSummaryRowsForUser_(summary.attendanceCenter, user),
    summaryByBatch: summary.attendanceBatch,
    recentRecords: records.slice(0, 100),
    adjustmentSummaryByBatch: summary.adjustmentBatch,
    adjustmentSummaryByCenter: filterAdjustmentSummaryRowsForUser_(summary.adjustmentCenter, user),
    recentAdjustments: adjustments.slice(0, 150)
  };
}

function searchAttendance(query) {
  const user = requirePermission_('search');
  query = clean_(query).toLowerCase();
  if (!query) return { ok: false, message: 'Enter CNIC, name, school, center, batch, or slot.' };

  const matches = filterRecordsForUser_(readAttendance_(), user).filter(row => {
    return [
      row.cnic,
      row.teacherName,
      row.school,
      row.markedCenter,
      row.markedBatch,
      row.markedSlot,
      row.registeredTrainingCenter
    ].join(' ').toLowerCase().indexOf(query) !== -1;
  }).slice(-200).reverse();

  return { ok: true, matches };
}

function summarizeBy_(records, field) {
  const totals = {};
  records.forEach(record => {
    const key = record[field] || 'Not selected';
    if (!totals[key]) totals[key] = { label: key, attendance: 0, uniqueCnic: {} };
    totals[key].attendance++;
    totals[key].uniqueCnic[record.cnic] = true;
  });

  return Object.keys(totals).sort().map(key => ({
    label: key,
    attendance: totals[key].attendance,
    uniqueAttendees: Object.keys(totals[key].uniqueCnic).length
  }));
}

function refreshDashboardSummaries_() {
  const ss = getSpreadsheet_();
  const sheet = getOrCreateSheet_(ss, CONFIG.SUMMARY_SHEET);
  ensureHeaders_(sheet, DASHBOARD_SUMMARY_COLUMNS, true);

  const now = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss');
  const records = readAttendance_();
  const adjustments = readParticipantAdjustments_();
  const rows = [];

  rows.push(summaryRow_('META', 'Total Candidates', readParticipants_().length, 0, 0, 0, 0, now));
  rows.push(summaryRow_('META', 'Total Attendance', records.length, 0, 0, 0, 0, now));
  rows.push(summaryRow_('META', 'Unique Attendees', uniqueSorted_(records.map(row => row.cnic)).length, 0, 0, 0, 0, now));

  summarizeBy_(records, 'markedCenter').forEach(row => {
    rows.push(summaryRow_('ATTENDANCE_CENTER', row.label, row.attendance, row.uniqueAttendees, 0, 0, 0, now));
  });
  summarizeBy_(records, 'markedBatch').forEach(row => {
    rows.push(summaryRow_('ATTENDANCE_BATCH', row.label, row.attendance, row.uniqueAttendees, 0, 0, 0, now));
  });
  summarizeAdjustmentsByBatch_(adjustments).forEach(row => {
    rows.push(summaryRow_('ADJUSTMENT_BATCH', row.label, 0, 0, row.movedIn, row.movedOut, row.netChange, now));
  });
  summarizeAdjustmentsByCenter_(adjustments).forEach(row => {
    rows.push(summaryRow_('ADJUSTMENT_CENTER', row.label, 0, 0, row.movedIn, row.movedOut, row.netChange, now));
  });

  clearSheetData_(sheet, DASHBOARD_SUMMARY_COLUMNS);
  if (rows.length) {
    sheet.getRange(2, 1, rows.length, DASHBOARD_SUMMARY_COLUMNS.length).setValues(rows);
  }
  cacheRemove_(CACHE_KEYS.DASHBOARD_SUMMARY);
}

function summaryRow_(type, label, records, uniqueCount, movedIn, movedOut, netChange, updatedAt) {
  return [type, label, records, uniqueCount, movedIn, movedOut, netChange, updatedAt];
}

function readDashboardSummaries_() {
  const cacheKey = CACHE_KEYS.DASHBOARD_SUMMARY;
  const cached = cacheGetJson_(cacheKey);
  if (cached) return cached;

  const sheet = getSpreadsheet_().getSheetByName(CONFIG.SUMMARY_SHEET);
  const out = {
    generatedAt: '',
    meta: {},
    attendanceCenter: [],
    attendanceBatch: [],
    adjustmentCenter: [],
    adjustmentBatch: []
  };
  if (!sheet || sheet.getLastRow() < 2) return out;

  const rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, DASHBOARD_SUMMARY_COLUMNS.length).getDisplayValues();
  rows.forEach(row => {
    const type = clean_(row[0]);
    const label = clean_(row[1]);
    const records = Number(row[2] || 0);
    const uniqueCount = Number(row[3] || 0);
    const movedIn = Number(row[4] || 0);
    const movedOut = Number(row[5] || 0);
    const netChange = Number(row[6] || 0);
    const updatedAt = clean_(row[7]);
    if (updatedAt) out.generatedAt = updatedAt;

    if (type === 'META' && label === 'Total Attendance') out.meta.totalAttendance = records;
    if (type === 'META' && label === 'Unique Attendees') out.meta.uniqueAttendees = records;
    if (type === 'ATTENDANCE_CENTER') out.attendanceCenter.push({ label, attendance: records, uniqueAttendees: uniqueCount });
    if (type === 'ATTENDANCE_BATCH') out.attendanceBatch.push({ label, attendance: records, uniqueAttendees: uniqueCount });
    if (type === 'ADJUSTMENT_CENTER') out.adjustmentCenter.push({ label, movedIn, movedOut, netChange });
    if (type === 'ADJUSTMENT_BATCH') out.adjustmentBatch.push({ label, movedIn, movedOut, netChange });
  });

  cachePutJson_(cacheKey, out);
  return out;
}

function filterSummaryRowsForUser_(rows, user) {
  if (user.allowedCenters.indexOf('*') !== -1) return rows;
  return rows.filter(row => user.allowedCenters.indexOf(row.label) !== -1);
}

function filterAdjustmentSummaryRowsForUser_(rows, user) {
  if (user.allowedCenters.indexOf('*') !== -1) return rows;
  return rows.filter(row => user.allowedCenters.indexOf(row.label) !== -1);
}

function getRecentAttendanceRecords_(limit) {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ATTENDANCE_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return [];
  const count = Math.min(limit || 100, sheet.getLastRow() - 1);
  const startRow = sheet.getLastRow() - count + 1;
  const values = sheet.getRange(startRow, 1, count, ATTENDANCE_COLUMNS.length).getDisplayValues();
  return values.reverse().map(row => ({
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
    attendanceStatus: normalizeAttendanceStatus_(row[20] || 'Present')
  }));
}

function getRecentAdjustmentRecords_(limit) {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ADJUSTMENT_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return [];
  const count = Math.min(limit || 100, sheet.getLastRow() - 1);
  const startRow = sheet.getLastRow() - count + 1;
  const values = sheet.getRange(startRow, 1, count, ADJUSTMENT_COLUMNS.length).getDisplayValues();
  return values.reverse()
    .filter(row => clean_(row[2]) && clean_(row[3]))
    .map(row => objectFromHeaders_(ADJUSTMENT_COLUMNS, row))
    .map(row => {
      row.cnic = normalizeCnic_(row.cnic);
      row.adjustmentDate = clean_(row.adjustmentDate);
      return row;
    });
}
