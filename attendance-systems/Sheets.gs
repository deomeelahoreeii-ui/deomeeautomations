function getSpreadsheet_() {
  try {
    if (CONFIG.SPREADSHEET_ID) {
      return SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
    }
    return SpreadsheetApp.getActiveSpreadsheet();
  } catch (error) {
    if (isAuthorizationRequiredError_(error)) {
      throw error;
    }
    const message = error && error.message ? error.message : String(error);
    throw new Error(
      'Google Drive access is missing for this spreadsheet. Ask an admin to confirm your email is active in the Users sheet, run syncUsersToSpreadsheetPermissions(), then reopen the portal. Original error: ' + message
    );
  }
}

function assertSpreadsheetAccess_() {
  const ss = getSpreadsheet_();
  const probeSheet = ss.getSheetByName(CONFIG.USERS_SHEET) || ss.getSheets()[0];
  if (!probeSheet) {
    throw new Error('Spreadsheet has no sheets.');
  }
  probeSheet.getRange(1, 1).getDisplayValue();
  return true;
}

function getParticipantsSheet_() {
  const sheet = getSpreadsheet_().getSheetByName(CONFIG.PARTICIPANTS_SHEET);
  if (!sheet) {
    throw new Error('Participant sheet not found: ' + CONFIG.PARTICIPANTS_SHEET);
  }
  return sheet;
}

function getOrCreateSheet_(ss, name) {
  return ss.getSheetByName(name) || ss.insertSheet(name);
}

function ensureHeaders_(sheet, headers, createMissing) {
  if (sheet.getMaxColumns() < headers.length) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), headers.length - sheet.getMaxColumns());
  }

  const existingWidth = Math.max(1, sheet.getLastColumn(), headers.length);
  const existing = sheet.getRange(1, 1, 1, existingWidth).getValues()[0].map(clean_);
  const empty = existing.every(value => !value);
  if (empty && createMissing) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }

  let current = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), headers.length)).getValues()[0].map(clean_);
  let missing = headers.filter(header => current.indexOf(header) === -1);
  if (missing.length && createMissing) {
    const startColumn = sheet.getLastColumn() + 1;
    if (sheet.getMaxColumns() < startColumn + missing.length - 1) {
      sheet.insertColumnsAfter(sheet.getMaxColumns(), startColumn + missing.length - 1 - sheet.getMaxColumns());
    }
    sheet.getRange(1, startColumn, 1, missing.length).setValues([missing]);
    current = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(clean_);
    missing = headers.filter(header => current.indexOf(header) === -1);
  }
  if (missing.length) {
    throw new Error('Sheet "' + sheet.getName() + '" is missing columns: ' + missing.join(', '));
  }

  sheet.getRange(1, 1, 1, Math.max(headers.length, sheet.getLastColumn()))
    .setFontWeight('bold')
    .setBackground('#e8f0fe')
    .setBorder(true, true, true, true, true, true);
  sheet.setFrozenRows(1);
  sheet.autoResizeColumns(1, Math.min(Math.max(headers.length, sheet.getLastColumn()), sheet.getMaxColumns()));
}
