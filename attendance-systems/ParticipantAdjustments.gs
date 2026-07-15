function searchParticipantForAdjustment(cnic) {
  const user = requirePermission_('adjust');

  const normalized = normalizeCnic_(cnic);
  if (!normalized) {
    return { ok: false, message: 'Enter a CNIC number.' };
  }

  const candidate = findParticipantByCnic_(normalized);
  if (!candidate) {
    return { ok: false, message: 'No participant found for CNIC ' + normalized + '.' };
  }
  assertCenterAccess_(user, candidate.trainingCenter);

  return {
    ok: true,
    candidate,
    previousAdjustments: getParticipantAdjustments_(normalized, 25),
    previousAttendance: getCandidateAttendance_(normalized, 10)
  };
}

function adjustParticipantAllocation(payload) {
  payload = payload || {};

  const cnic = normalizeCnic_(payload.cnic);
  const newCenter = clean_(payload.newCenter);
  const newBatch = clean_(payload.newBatch);
  const newSlot = clean_(payload.newSlot);
  const reason = clean_(payload.reason);

  if (!cnic) return { ok: false, message: 'CNIC is required.' };
  if (!newCenter || !newBatch || !newSlot) {
    return { ok: false, message: 'Select the new center, batch, and slot.' };
  }
  if (!reason) {
    return { ok: false, message: 'Adjustment reason is required.' };
  }

  const lock = getWriteLock_();
  if (!lock.tryLock(15000)) {
    return { ok: false, message: 'Participant adjustment system is busy. Please try again in a few seconds.' };
  }

  try {
    const rowInfo = findParticipantRowByCnic_(cnic);
    if (!rowInfo) {
      return { ok: false, message: 'Participant was not found. Refresh and try again.' };
    }

    const before = rowInfo.participant;
    const user = requirePermission_('adjust', before.trainingCenter);
    assertCenterAccess_(user, newCenter);

    const target = findAllocationTemplate_(newCenter, newBatch, newSlot);
    if (!target) {
      return {
        ok: false,
        message: 'No existing allocation template found for ' + [newCenter, newBatch, newSlot].join(' | ') + '. Select a valid center, batch, and slot combination.'
      };
    }

    if (
      before.trainingCenter === target.center &&
      before.batch === target.batch &&
      before.slot === target.slot
    ) {
      return { ok: false, message: 'Participant is already assigned to this center, batch, and slot.' };
    }

    const after = {
      sn: before.sn,
      batch: target.batch,
      groupCode: target.groupCode,
      slot: target.slot,
      sessionTime: target.sessionTime,
      startDate: target.startDate,
      endDate: target.endDate,
      teacherName: before.teacherName,
      cnic: before.cnic,
      school: before.school,
      tehsil: before.tehsil,
      contact: before.contact,
      trainingCenter: target.center
    };

    updateParticipantAllocationRow_(rowInfo, after);
    clearParticipantCaches_();
    const adjustment = appendParticipantAdjustment_(before, after, reason, user);
    refreshDashboardSummaries_();

    return {
      ok: true,
      message: 'Participant adjusted from ' + before.trainingCenter + ' / ' + before.batch + ' / ' + before.slot +
        ' to ' + after.trainingCenter + ' / ' + after.batch + ' / ' + after.slot + '.',
      candidate: after,
      adjustmentKey: adjustment.adjustmentKey,
      previousAdjustments: getParticipantAdjustments_(cnic, 25),
      previousAttendance: getCandidateAttendance_(cnic, 10)
    };
  } finally {
    lock.releaseLock();
  }
}

function getParticipantAdjustmentDashboard(filters) {
  filters = filters || {};
  const user = requirePermission_('view', filters.center || '');
  const rows = filterAdjustmentsForUser_(readParticipantAdjustments_(), user).filter(row => {
    if (filters.fromDate && row.adjustmentDate < filters.fromDate) return false;
    if (filters.toDate && row.adjustmentDate > filters.toDate) return false;
    if (filters.center && row.newCenter !== filters.center && row.oldCenter !== filters.center) return false;
    if (filters.batch && row.newBatch !== filters.batch && row.oldBatch !== filters.batch) return false;
    return true;
  });

  return {
    ok: true,
    totalAdjustments: rows.length,
    summaryByBatch: summarizeAdjustmentsByBatch_(rows),
    summaryByCenter: summarizeAdjustmentsByCenter_(rows),
    recentAdjustments: rows.slice(-150).reverse()
  };
}

function searchParticipantAdjustments(query) {
  const user = requirePermission_('view');
  query = clean_(query).toLowerCase();
  if (!query) return { ok: false, message: 'Enter CNIC, name, school, center, batch, or slot.' };

  const matches = filterAdjustmentsForUser_(readParticipantAdjustments_(), user).filter(row => {
    return [
      row.cnic,
      row.teacherName,
      row.school,
      row.oldCenter,
      row.oldBatch,
      row.oldSlot,
      row.newCenter,
      row.newBatch,
      row.newSlot,
      row.changedBy
    ].join(' ').toLowerCase().indexOf(query) !== -1;
  }).slice(-200).reverse();

  return { ok: true, matches };
}

function updateParticipantAllocationRow_(rowInfo, after) {
  const index = rowInfo.index;
  const updates = {};
  updates.Batch = after.batch;
  updates['Group Code'] = after.groupCode;
  updates.Slot = after.slot;
  updates['Session Time'] = after.sessionTime;
  updates['Start Date'] = after.startDate;
  updates['End Date'] = after.endDate;
  updates['Training Center'] = after.trainingCenter;

  Object.keys(updates).forEach(header => {
    rowInfo.sheet.getRange(rowInfo.rowNumber, index[header] + 1).setValue(updates[header]);
  });
}

function appendParticipantAdjustment_(before, after, reason, user) {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ADJUSTMENT_SHEET);
  const now = new Date();
  const key = before.cnic + '|' + Utilities.formatDate(now, CONFIG.TIMEZONE, 'yyyyMMddHHmmssSSS');
  const row = [
    now,
    Utilities.formatDate(now, CONFIG.TIMEZONE, 'yyyy-MM-dd'),
    key,
    before.cnic,
    before.teacherName,
    before.school,
    before.contact,
    before.trainingCenter,
    before.batch,
    before.groupCode,
    before.slot,
    before.sessionTime,
    before.startDate,
    before.endDate,
    after.trainingCenter,
    after.batch,
    after.groupCode,
    after.slot,
    after.sessionTime,
    after.startDate,
    after.endDate,
    reason,
    user.email,
    user.role,
    JSON.stringify(before),
    JSON.stringify(after)
  ];
  sheet.appendRow(row);
  return {
    adjustmentKey: key,
    adjustmentDate: row[1],
    cnic: before.cnic
  };
}

function readParticipantAdjustments_() {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ADJUSTMENT_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return [];

  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, ADJUSTMENT_COLUMNS.length).getDisplayValues();
  return values
    .filter(row => clean_(row[1]) && clean_(row[2]))
    .map(row => objectFromHeaders_(ADJUSTMENT_COLUMNS, row))
    .map(row => {
      row.cnic = normalizeCnic_(row.cnic);
      row.adjustmentDate = clean_(row.adjustmentDate);
      return row;
    });
}

function getParticipantAdjustments_(cnic, limit) {
  const normalized = normalizeCnic_(cnic);
  const rows = readParticipantAdjustments_().filter(row => row.cnic === normalized);
  const start = Math.max(0, rows.length - (limit || 25));
  return rows.slice(start).reverse();
}

function filterAdjustmentsForUser_(rows, user) {
  if (user.allowedCenters.indexOf('*') !== -1) return rows;
  return rows.filter(row => {
    return user.allowedCenters.indexOf(row.oldCenter) !== -1 || user.allowedCenters.indexOf(row.newCenter) !== -1;
  });
}

function summarizeAdjustmentsByBatch_(rows) {
  const totals = {};
  rows.forEach(row => {
    addAdjustmentSummary_(totals, row.oldBatch || 'Not selected', 0, 1);
    addAdjustmentSummary_(totals, row.newBatch || 'Not selected', 1, 0);
  });
  return adjustmentSummaryRows_(totals);
}

function summarizeAdjustmentsByCenter_(rows) {
  const totals = {};
  rows.forEach(row => {
    addAdjustmentSummary_(totals, row.oldCenter || 'Not selected', 0, 1);
    addAdjustmentSummary_(totals, row.newCenter || 'Not selected', 1, 0);
  });
  return adjustmentSummaryRows_(totals);
}

function addAdjustmentSummary_(totals, label, movedIn, movedOut) {
  if (!totals[label]) totals[label] = { label, movedIn: 0, movedOut: 0 };
  totals[label].movedIn += movedIn;
  totals[label].movedOut += movedOut;
}

function adjustmentSummaryRows_(totals) {
  return Object.keys(totals).sort().map(key => ({
    label: key,
    movedIn: totals[key].movedIn,
    movedOut: totals[key].movedOut,
    netChange: totals[key].movedIn - totals[key].movedOut
  }));
}

function restoreParticipantsFromAdjustmentLog_() {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ADJUSTMENT_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return { restored: 0 };

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0].map(clean_);
  const cnicIndex = headers.indexOf('CNIC');
  const beforeIndex = headers.indexOf('Before');
  if (cnicIndex === -1 || beforeIndex === -1) return { restored: 0 };

  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getDisplayValues();
  const originalByCnic = {};
  values.forEach(row => {
    const cnic = normalizeCnic_(row[cnicIndex]);
    if (!cnic || originalByCnic[cnic]) return;
    const beforeJson = clean_(row[beforeIndex]);
    if (!beforeJson) return;
    try {
      const before = JSON.parse(beforeJson);
      if (before && before.cnic) originalByCnic[cnic] = before;
    } catch (error) {
      // Ignore malformed historical test rows; reset should continue for valid rows.
    }
  });

  let restored = 0;
  Object.keys(originalByCnic).forEach(cnic => {
    const rowInfo = findParticipantRowByCnic_(cnic);
    if (!rowInfo) return;
    updateParticipantAllocationRow_(rowInfo, originalByCnic[cnic]);
    restored++;
  });

  return { restored };
}
