function setupOriginalAllocationSheet_(ss) {
  const sheet = getOrCreateSheet_(ss, CONFIG.ORIGINAL_ALLOCATION_SHEET);
  ensureHeaders_(sheet, ORIGINAL_ALLOCATION_COLUMNS, true);
  if (sheet.getLastRow() >= 2) return sheet;

  const originals = buildOriginalAllocationRows_();
  if (originals.length) {
    sheet.getRange(2, 1, originals.length, ORIGINAL_ALLOCATION_COLUMNS.length).setValues(originals);
  }
  sheet.autoResizeColumns(1, ORIGINAL_ALLOCATION_COLUMNS.length);
  return sheet;
}

function buildOriginalAllocationRows_() {
  const current = readParticipantsFromSheet_();
  const earliestBefore = getEarliestAdjustmentBeforeByCnic_();
  const now = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss');

  return current.map(participant => {
    const original = earliestBefore[participant.cnic] || participant;
    return participantToOriginalRow_(original, now);
  });
}

function participantToOriginalRow_(participant, snapshotAt) {
  return [
    participant.sn,
    participant.batch,
    participant.groupCode,
    participant.slot,
    participant.sessionTime,
    participant.startDate,
    participant.endDate,
    participant.teacherName,
    participant.cnic,
    participant.school,
    participant.tehsil,
    participant.contact,
    participant.trainingCenter,
    snapshotAt
  ];
}

function getEarliestAdjustmentBeforeByCnic_() {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ADJUSTMENT_SHEET);
  const out = {};
  if (!sheet || sheet.getLastRow() < 2) return out;

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0].map(clean_);
  const cnicIndex = headers.indexOf('CNIC');
  const beforeIndex = headers.indexOf('Before');
  if (cnicIndex === -1 || beforeIndex === -1) return out;

  const rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getDisplayValues();
  rows.forEach(row => {
    const cnic = normalizeCnic_(row[cnicIndex]);
    if (!cnic || out[cnic]) return;
    try {
      const before = JSON.parse(clean_(row[beforeIndex]));
      if (before && before.cnic) out[cnic] = before;
    } catch (error) {
      // Ignore malformed historical rows.
    }
  });
  return out;
}

function readOriginalAllocations_() {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.ORIGINAL_ALLOCATION_SHEET);
  if (!sheet || sheet.getLastRow() < 2) return [];

  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, ORIGINAL_ALLOCATION_COLUMNS.length).getDisplayValues();
  const index = headerIndex_(ORIGINAL_ALLOCATION_COLUMNS);
  return values
    .filter(row => clean_(row[index.CNIC]))
    .map(row => participantFromRow_(row, index));
}

function getOriginalAllocationByCnic_() {
  const out = {};
  readOriginalAllocations_().forEach(row => out[row.cnic] = row);
  return out;
}
