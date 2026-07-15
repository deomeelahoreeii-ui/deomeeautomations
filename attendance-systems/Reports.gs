function getReportsBootstrap() {
  const user = requirePermission_('view');
  const base = getBootstrapBase_();
  return {
    ok: true,
    today: today_(),
    centers: filterCentersForUser_(base.centers, user),
    batches: base.batches,
    slots: base.slots,
    user: userPublic_(user)
  };
}

function previewReport(payload) {
  const report = buildReport_(payload || {}, 200);
  return {
    ok: true,
    title: report.title,
    reportType: report.reportType,
    columns: report.columns,
    rows: report.rows,
    metrics: report.metrics,
    totalRows: report.totalRows,
    previewRows: report.rows.length
  };
}

function exportReport(payload) {
  const report = buildReport_(payload || {}, 0);
  const spreadsheet = SpreadsheetApp.create(report.title + ' - ' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyyMMdd-HHmmss'));
  writeReportSpreadsheet_(spreadsheet, report);
  SpreadsheetApp.flush();

  const exportUrl = 'https://docs.google.com/spreadsheets/d/' + spreadsheet.getId() + '/export?format=xlsx';
  const response = UrlFetchApp.fetch(exportUrl, {
    headers: { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() },
    muteHttpExceptions: true
  });
  if (response.getResponseCode() >= 400) {
    throw new Error('Excel export failed with HTTP ' + response.getResponseCode() + '.');
  }

  const fileName = report.title + '.xlsx';
  const file = DriveApp.createFile(response.getBlob().setName(fileName));
  DriveApp.getFileById(spreadsheet.getId()).setTrashed(true);
  return {
    ok: true,
    fileName,
    url: file.getUrl(),
    totalRows: report.totalRows
  };
}

function buildReport_(payload, previewLimit) {
  const filters = normalizeReportFilters_(payload);
  const user = requirePermission_('view', filters.center || '');
  const context = buildReportContext_(user);
  let report;

  if (filters.reportType === 'attendanceSummary') report = buildAttendanceSummaryReport_(filters, context);
  else if (filters.reportType === 'adjustedIn') report = buildAdjustedReport_(filters, context, 'in');
  else if (filters.reportType === 'adjustedOut') report = buildAdjustedReport_(filters, context, 'out');
  else if (filters.reportType === 'movementHistory') report = buildMovementHistoryReport_(filters, context);
  else report = buildAbsenceReport_(filters, context);

  report.totalRows = report.rows.length;
  if (previewLimit && report.rows.length > previewLimit) report.rows = report.rows.slice(0, previewLimit);
  return report;
}

function normalizeReportFilters_(payload) {
  const reportType = clean_(payload.reportType) || 'absence';
  const dateFrom = isIsoDate_(payload.dateFrom) ? clean_(payload.dateFrom) : today_();
  const dateTo = isIsoDate_(payload.dateTo) ? clean_(payload.dateTo) : dateFrom;
  return {
    reportType,
    dateFrom,
    dateTo,
    center: clean_(payload.center),
    batch: clean_(payload.batch),
    slot: clean_(payload.slot),
    status: clean_(payload.status) || 'absent',
    query: clean_(payload.query).toLowerCase()
  };
}

function buildReportContext_(user) {
  const current = readParticipants_();
  const originals = getOriginalAllocationByCnic_();
  const attendance = filterRecordsForUser_(readAttendance_(), user);
  const adjustments = filterAdjustmentsForUser_(readParticipantAdjustments_(), user);
  const attendanceByCnicDate = {};

  attendance.forEach(row => {
    attendanceByCnicDate[row.cnic + '|' + row.attendanceDate] = row;
  });

  return {
    user,
    current,
    originals,
    attendance,
    adjustments,
    attendanceByCnicDate
  };
}

function buildAbsenceReport_(filters, context) {
  const dates = enumerateReportDates_(filters.dateFrom, filters.dateTo);
  const expected = context.current.filter(row => participantMatchesFilters_(row, filters));
  const columns = [
    'Attendance Date', 'Status', 'CNIC', 'Teacher Name', 'School', 'Contact',
    'Original Center', 'Current Center', 'Batch', 'Slot', 'Session Time',
    'Marked Center', 'Marked Batch', 'Marked Slot', 'Marked By',
    'Absence Reason', 'Absence Reason Detail', 'Notes', 'Attended Elsewhere'
  ];
  const rows = [];
  const metrics = { expected: expected.length * dates.length, present: 0, absentMarked: 0, notMarked: 0, totalAbsent: 0 };

  dates.forEach(date => {
    expected.forEach(participant => {
      const original = context.originals[participant.cnic] || participant;
      const attendance = context.attendanceByCnicDate[participant.cnic + '|' + date];
      const status = attendance ? normalizeAttendanceStatus_(attendance.attendanceStatus) : 'Not Marked';
      const attendedElsewhere = attendance && attendance.markedCenter !== participant.trainingCenter ? 'Yes' : '';

      if (status === 'Present') metrics.present++;
      if (status === 'Absent') metrics.absentMarked++;
      if (status === 'Not Marked') metrics.notMarked++;

      const isAbsent = status === 'Absent' || status === 'Not Marked';
      if (isAbsent) metrics.totalAbsent++;
      if (filters.status === 'absent' && !isAbsent) return;
      if (filters.status === 'present' && status !== 'Present') return;
      if (!reportRowMatchesQuery_(participant, filters.query, [original.trainingCenter, participant.trainingCenter, status])) return;

      rows.push([
        date,
        status,
        participant.cnic,
        participant.teacherName,
        participant.school,
        participant.contact,
        original.trainingCenter,
        participant.trainingCenter,
        participant.batch,
        participant.slot,
        participant.sessionTime,
        attendance ? attendance.markedCenter : '',
        attendance ? attendance.markedBatch : '',
        attendance ? attendance.markedSlot : '',
        attendance ? attendance.markedBy : '',
        attendance ? attendance.absenceReason : '',
        attendance ? attendance.absenceReasonDetail : '',
        attendance ? attendance.notes : '',
        attendedElsewhere
      ]);
    });
  });

  return { reportType: 'absence', title: 'Absence Report', columns, rows, metrics };
}

function buildAttendanceSummaryReport_(filters, context) {
  const dates = enumerateReportDates_(filters.dateFrom, filters.dateTo);
  const expected = context.current.filter(row => participantMatchesFilters_(row, filters));
  const adjustments = context.adjustments;
  const columns = [
    'Date', 'Center', 'Batch', 'Slot', 'Session Time', 'Expected Participants',
    'Present', 'Absent Marked', 'Not Marked', 'Total Absent',
    'Adjusted In', 'Adjusted Out', 'Attended Elsewhere', 'Attendance %'
  ];
  const rows = [];

  dates.forEach(date => {
    const groups = {};
    expected.forEach(participant => {
      const key = [participant.trainingCenter, participant.batch, participant.slot, participant.sessionTime].join('|');
      if (!groups[key]) {
        groups[key] = {
          center: participant.trainingCenter,
          batch: participant.batch,
          slot: participant.slot,
          sessionTime: participant.sessionTime,
          expected: 0,
          present: 0,
          absentMarked: 0,
          notMarked: 0,
          attendedElsewhere: 0
        };
      }
      const group = groups[key];
      group.expected++;
      const attendance = context.attendanceByCnicDate[participant.cnic + '|' + date];
      if (!attendance) group.notMarked++;
      else if (normalizeAttendanceStatus_(attendance.attendanceStatus) === 'Absent') group.absentMarked++;
      else group.present++;
      if (attendance && attendance.markedCenter !== participant.trainingCenter) group.attendedElsewhere++;
    });

    Object.keys(groups).sort().forEach(key => {
      const group = groups[key];
      const adjustedIn = adjustments.filter(row => row.adjustmentDate <= date && row.newCenter === group.center && row.newBatch === group.batch && row.newSlot === group.slot).length;
      const adjustedOut = adjustments.filter(row => row.adjustmentDate <= date && row.oldCenter === group.center && row.oldBatch === group.batch && row.oldSlot === group.slot).length;
      const totalAbsent = group.absentMarked + group.notMarked;
      rows.push([
        date,
        group.center,
        group.batch,
        group.slot,
        group.sessionTime,
        group.expected,
        group.present,
        group.absentMarked,
        group.notMarked,
        totalAbsent,
        adjustedIn,
        adjustedOut,
        group.attendedElsewhere,
        group.expected ? Math.round((group.present / group.expected) * 10000) / 100 + '%' : '0%'
      ]);
    });
  });

  return { reportType: 'attendanceSummary', title: 'Attendance Summary Report', columns, rows, metrics: { groups: rows.length } };
}

function buildAdjustedReport_(filters, context, direction) {
  const isIn = direction === 'in';
  const columns = [
    'Adjustment Date', 'CNIC', 'Teacher Name', 'School', 'Contact',
    'Old Center', 'Old Batch', 'Old Slot', 'Old Group Code',
    'New Center', 'New Batch', 'New Slot', 'New Group Code',
    'Reason', 'Changed By', 'User Role'
  ];
  const rows = context.adjustments.filter(row => {
    if (!dateInRange_(row.adjustmentDate, filters.dateFrom, filters.dateTo)) return false;
    const center = isIn ? row.newCenter : row.oldCenter;
    const batch = isIn ? row.newBatch : row.oldBatch;
    const slot = isIn ? row.newSlot : row.oldSlot;
    if (filters.center && center !== filters.center) return false;
    if (filters.batch && batch !== filters.batch) return false;
    if (filters.slot && slot !== filters.slot) return false;
    if (filters.query && [row.cnic, row.teacherName, row.school, row.oldCenter, row.newCenter, row.changedBy].join(' ').toLowerCase().indexOf(filters.query) === -1) return false;
    return true;
  }).map(row => [
    row.adjustmentDate,
    row.cnic,
    row.teacherName,
    row.school,
    row.contact,
    row.oldCenter,
    row.oldBatch,
    row.oldSlot,
    row.oldGroupCode,
    row.newCenter,
    row.newBatch,
    row.newSlot,
    row.newGroupCode,
    row.reason,
    row.changedBy,
    row.userRole
  ]);

  return {
    reportType: isIn ? 'adjustedIn' : 'adjustedOut',
    title: isIn ? 'Adjusted In Participants Report' : 'Adjusted Out Participants Report',
    columns,
    rows,
    metrics: { adjustments: rows.length }
  };
}

function buildMovementHistoryReport_(filters, context) {
  const query = filters.query;
  const cnics = {};
  context.current.forEach(row => {
    if (!query || reportRowMatchesQuery_(row, query, [])) cnics[row.cnic] = true;
  });
  context.adjustments.forEach(row => {
    if (!query || [row.cnic, row.teacherName, row.school].join(' ').toLowerCase().indexOf(query) !== -1) cnics[row.cnic] = true;
  });
  context.attendance.forEach(row => {
    if (!query || [row.cnic, row.teacherName, row.school].join(' ').toLowerCase().indexOf(query) !== -1) cnics[row.cnic] = true;
  });

  const columns = ['Event Date', 'Event Type', 'CNIC', 'Teacher Name', 'Center', 'Batch', 'Slot', 'Details', 'Changed/Marked By'];
  const rows = [];
  Object.keys(cnics).sort().forEach(cnic => {
    const original = context.originals[cnic];
    if (original) {
      rows.push(['Original', 'Original Allocation', cnic, original.teacherName, original.trainingCenter, original.batch, original.slot, original.school, '']);
    }
    context.adjustments.filter(row => row.cnic === cnic).forEach(row => {
      rows.push([
        row.adjustmentDate,
        'Adjustment',
        cnic,
        row.teacherName,
        row.newCenter,
        row.newBatch,
        row.newSlot,
        'From ' + row.oldCenter + ' / ' + row.oldBatch + ' / ' + row.oldSlot + ' to ' + row.newCenter + ' / ' + row.newBatch + ' / ' + row.newSlot + '. Reason: ' + row.reason,
        row.changedBy
      ]);
    });
    context.attendance.filter(row => row.cnic === cnic).forEach(row => {
      rows.push([
        row.attendanceDate,
        'Attendance ' + normalizeAttendanceStatus_(row.attendanceStatus),
        cnic,
        row.teacherName,
        row.markedCenter,
        row.markedBatch,
        row.markedSlot,
        row.school + (row.absenceReason ? ' | Absence reason: ' + row.absenceReason + (row.absenceReasonDetail ? ' - ' + row.absenceReasonDetail : '') : ''),
        row.markedBy
      ]);
    });
    const current = context.current.find(row => row.cnic === cnic);
    if (current) {
      rows.push(['Current', 'Current Allocation', cnic, current.teacherName, current.trainingCenter, current.batch, current.slot, current.school, '']);
    }
  });

  return { reportType: 'movementHistory', title: 'Candidate Movement History Report', columns, rows, metrics: { candidates: Object.keys(cnics).length } };
}

function writeReportSpreadsheet_(spreadsheet, report) {
  const sheet = spreadsheet.getSheets()[0];
  sheet.setName(report.title.slice(0, 90));
  const meta = [
    [report.title, ''],
    ['Generated At', Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss')],
    ['Total Rows', report.totalRows]
  ];
  sheet.getRange(1, 1, meta.length, 2).setValues(meta);
  sheet.getRange(1, 1).setFontWeight('bold').setFontSize(14);
  const columnCount = report.columns.length;
  sheet.getRange(5, 1, 1, columnCount).setValues([normalizeSheetRow_(report.columns, columnCount)]).setFontWeight('bold').setBackground('#e8f0fe');
  if (report.rows.length) {
    const rows = report.rows.map(row => normalizeSheetRow_(row, columnCount));
    sheet.getRange(6, 1, rows.length, columnCount).setValues(rows);
  }
  sheet.setFrozenRows(5);
  sheet.autoResizeColumns(1, columnCount);
}

function normalizeSheetRow_(row, columnCount) {
  row = Array.isArray(row) ? row.slice(0, columnCount) : [row];
  while (row.length < columnCount) row.push('');
  return row;
}

function participantMatchesFilters_(participant, filters) {
  if (filters.center && participant.trainingCenter !== filters.center) return false;
  if (filters.batch && participant.batch !== filters.batch) return false;
  if (filters.slot && participant.slot !== filters.slot) return false;
  return true;
}

function reportRowMatchesQuery_(participant, query, extras) {
  if (!query) return true;
  return [
    participant.cnic,
    participant.teacherName,
    participant.school,
    participant.contact,
    participant.trainingCenter,
    participant.batch,
    participant.slot
  ].concat(extras || []).join(' ').toLowerCase().indexOf(query) !== -1;
}

function enumerateReportDates_(fromDate, toDate) {
  const out = [];
  const start = new Date(fromDate + 'T00:00:00');
  const end = new Date(toDate + 'T00:00:00');
  const maxDays = 45;
  for (let d = new Date(start), i = 0; d <= end && i < maxDays; d.setDate(d.getDate() + 1), i++) {
    out.push(Utilities.formatDate(d, CONFIG.TIMEZONE, 'yyyy-MM-dd'));
  }
  return out.length ? out : [today_()];
}

function dateInRange_(date, fromDate, toDate) {
  if (!date) return false;
  return date >= fromDate && date <= toDate;
}
