function searchCandidate(cnic) {
  requirePermission_('search');
  const normalized = normalizeCnic_(cnic);
  if (!normalized) {
    return { ok: false, message: 'Enter a CNIC number.' };
  }

  const candidate = findParticipantByCnic_(normalized);
  if (!candidate) {
    return { ok: false, message: 'No candidate found for CNIC ' + normalized + '.' };
  }

  return {
    ok: true,
    candidate,
    previousAttendance: getCandidateAttendance_(normalized, 25)
  };
}

function readParticipants_() {
  const cached = cacheGetJson_(CACHE_KEYS.PARTICIPANTS);
  if (cached) return cached;

  const participants = readParticipantsFromSheet_();
  cachePutJson_(CACHE_KEYS.PARTICIPANTS, participants);
  return participants;
}

function readParticipantsFromSheet_() {
  const sheet = getParticipantsSheet_();
  const values = sheet.getDataRange().getDisplayValues();
  if (values.length < 2) return [];

  const headers = values[0].map(clean_);
  const index = headerIndex_(headers);

  return values.slice(1)
    .filter(row => clean_(row[index.CNIC]))
    .map(row => participantFromRow_(row, index));
}

function findParticipantByCnic_(cnic) {
  const rowInfo = findParticipantRowByCnic_(cnic);
  return rowInfo ? rowInfo.participant : null;
}

function findParticipantRowByCnic_(cnic) {
  const normalized = normalizeCnic_(cnic);
  const cached = getParticipantIndex_()[normalized];
  if (cached) {
    const sheet = getParticipantsSheet_();
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0].map(clean_);
    const index = headerIndex_(headers);
    return {
      sheet,
      headers,
      index,
      rowNumber: cached.rowNumber,
      participant: cached.participant
    };
  }

  const sheet = getParticipantsSheet_();
  if (sheet.getLastRow() < 2) return null;

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0].map(clean_);
  const index = headerIndex_(headers);
  const cnicColumn = index.CNIC + 1;
  const range = sheet.getRange(2, cnicColumn, sheet.getLastRow() - 1, 1);
  const match = range.createTextFinder(normalized).matchEntireCell(true).findNext();
  if (!match) return null;

  const row = sheet.getRange(match.getRow(), 1, 1, sheet.getLastColumn()).getDisplayValues()[0];
  return {
    sheet,
    headers,
    index,
    rowNumber: match.getRow(),
    participant: participantFromRow_(row, index)
  };
}

function getParticipantIndex_() {
  const cached = cacheGetJson_(CACHE_KEYS.PARTICIPANT_INDEX);
  if (cached) return cached;

  const sheet = getParticipantsSheet_();
  const out = {};
  if (sheet.getLastRow() < 2) {
    cachePutJson_(CACHE_KEYS.PARTICIPANT_INDEX, out);
    return out;
  }

  const values = sheet.getDataRange().getDisplayValues();
  const headers = values[0].map(clean_);
  const index = headerIndex_(headers);
  values.slice(1).forEach((row, i) => {
    const participant = participantFromRow_(row, index);
    if (!participant.cnic) return;
    out[participant.cnic] = {
      rowNumber: i + 2,
      participant
    };
  });
  cachePutJson_(CACHE_KEYS.PARTICIPANT_INDEX, out);
  return out;
}

function participantFromRow_(row, index) {
  return {
    sn: clean_(row[index.SN]),
    batch: clean_(row[index.Batch]),
    groupCode: clean_(row[index['Group Code']]),
    slot: clean_(row[index.Slot]),
    sessionTime: clean_(row[index['Session Time']]),
    startDate: clean_(row[index['Start Date']]),
    endDate: clean_(row[index['End Date']]),
    teacherName: clean_(row[index['Teacher Name']]),
    cnic: normalizeCnic_(row[index.CNIC]),
    school: clean_(row[index.School]),
    tehsil: clean_(row[index.Tehsil]),
    contact: normalizePhone_(row[index.Contact]),
    trainingCenter: clean_(row[index['Training Center']])
  };
}

function buildAllocationOptions_(participants) {
  const byKey = {};
  participants.forEach(row => {
    const center = clean_(row.trainingCenter);
    const batch = clean_(row.batch);
    const slot = clean_(row.slot);
    if (!center || !batch || !slot) return;
    const key = [center, batch, slot].join('|');
    const option = {
      key,
      center,
      batch,
      slot,
      groupCode: clean_(row.groupCode),
      sessionTime: clean_(row.sessionTime),
      startDate: clean_(row.startDate),
      endDate: clean_(row.endDate),
      label: [center, batch, slot].join(' | ')
    };
    if (!byKey[key] || allocationTemplateScore_(option) > allocationTemplateScore_(byKey[key])) {
      byKey[key] = option;
    }
  });
  return Object.keys(byKey).sort().map(key => byKey[key]);
}

function buildValidAllocationOptions_(currentParticipants) {
  const rows = [];
  appendAllocationTemplateRows_(rows, readOriginalAllocations_());
  appendAllocationTemplateRows_(rows, currentParticipants || readParticipants_());
  appendAdjustmentAllocationTemplateRows_(rows, readParticipantAdjustments_());
  return buildAllocationOptions_(rows);
}

function appendAllocationTemplateRows_(target, rows) {
  (rows || []).forEach(row => target.push(row));
}

function appendAdjustmentAllocationTemplateRows_(target, adjustments) {
  (adjustments || []).forEach(row => {
    const oldTemplate = adjustmentAllocationTemplate_(row, 'old');
    const newTemplate = adjustmentAllocationTemplate_(row, 'new');
    if (oldTemplate) target.push(oldTemplate);
    if (newTemplate) target.push(newTemplate);
  });
}

function adjustmentAllocationTemplate_(row, prefix) {
  const center = clean_(row[prefix + 'Center']);
  const batch = clean_(row[prefix + 'Batch']);
  const slot = clean_(row[prefix + 'Slot']);
  if (!center || !batch || !slot) return null;

  return {
    trainingCenter: center,
    batch,
    groupCode: clean_(row[prefix + 'GroupCode']),
    slot,
    sessionTime: clean_(row[prefix + 'SessionTime']),
    startDate: clean_(row[prefix + 'StartDate']),
    endDate: clean_(row[prefix + 'EndDate'])
  };
}

function allocationTemplateScore_(option) {
  return [
    option.groupCode,
    option.sessionTime,
    option.startDate,
    option.endDate
  ].filter(Boolean).length;
}

function filterAllocationsForUser_(allocations, user) {
  if (user.allowedCenters.indexOf('*') !== -1) return allocations;
  return allocations.filter(item => user.allowedCenters.indexOf(item.center) !== -1);
}

function findAllocationTemplate_(center, batch, slot) {
  center = clean_(center);
  batch = clean_(batch);
  slot = clean_(slot);
  return buildValidAllocationOptions_().find(item => {
    return item.center === center && item.batch === batch && item.slot === slot;
  }) || null;
}
