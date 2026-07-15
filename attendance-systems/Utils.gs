function headerIndex_(headers) {
  const index = {};
  headers.forEach((header, i) => index[header] = i);
  PARTICIPANT_COLUMNS.forEach(header => {
    if (index[header] === undefined) {
      throw new Error('Participant sheet missing required column: ' + header);
    }
  });
  return index;
}

function batchDateLabel_(row) {
  const parts = [row.batch, row.startDate, row.endDate].filter(Boolean);
  return parts.length ? parts.join(' | ') : '';
}

function uniqueSorted_(values) {
  const seen = {};
  values.forEach(value => {
    value = clean_(value);
    if (value) seen[value] = true;
  });
  return Object.keys(seen).sort((a, b) => a.localeCompare(b));
}

function objectFromHeaders_(headers, row) {
  const out = {};
  headers.forEach((header, i) => {
    const key = toCamel_(header);
    out[key] = header === 'Attendance Status' ? normalizeAttendanceStatus_(row[i] || 'Present') : row[i];
  });
  return out;
}

function toCamel_(text) {
  text = String(text);
  if (/^[A-Z0-9]+$/.test(text)) return text.toLowerCase();
  return text.replace(/[^a-zA-Z0-9]+(.)/g, (_, chr) => chr.toUpperCase()).replace(/^[A-Z]/, chr => chr.toLowerCase());
}

function normalizeCnic_(value) {
  return String(value || '').replace(/\D/g, '').trim();
}

function normalizePhone_(value) {
  return String(value || '').replace(/[^\d+]/g, '').trim();
}

function normalizeAttendanceStatus_(value) {
  value = clean_(value).toLowerCase();
  return value === 'absent' ? 'Absent' : 'Present';
}

function clean_(value) {
  return String(value == null ? '' : value).replace(/\s+/g, ' ').trim();
}

function today_() {
  return Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd');
}

function isIsoDate_(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function isAuthorizationRequiredError_(error) {
  const message = error && error.message ? error.message : String(error || '');
  return message.indexOf('Required permissions:') !== -1 ||
    message.indexOf('permission to call') !== -1 ||
    message.indexOf('Authorization is required') !== -1 ||
    message.indexOf('You do not have permission to call') !== -1;
}

function getWriteLock_() {
  return LockService.getDocumentLock() || LockService.getScriptLock();
}
