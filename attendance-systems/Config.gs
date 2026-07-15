const CONFIG = {
  // If this script is bound to the Google Sheet, this can be left blank.
  // If this is a standalone Apps Script project, keep the spreadsheet ID below.
  SPREADSHEET_ID: '1I2qNPMJVnG2Bp3_pvx7r7kycXYw7_g41efcBTuzVnOM',
  PARTICIPANTS_SHEET: 'Final List of Participants',
  ORIGINAL_ALLOCATION_SHEET: 'Participant Original Allocation',
  ATTENDANCE_SHEET: 'Attendance Log',
  AUDIT_SHEET: 'Attendance Audit Log',
  ADJUSTMENT_SHEET: 'Participant Adjustment Log',
  SUMMARY_SHEET: 'Attendance Dashboard Summary',
  USERS_SHEET: 'Users',
  ABSENCE_REASONS_SHEET: 'Absence Reasons',
  SETTINGS_SHEET: 'Attendance Settings',
  TIMEZONE: 'Asia/Karachi',
  CITY_LABEL: 'Lahore',
  PORTAL_TITLE: 'PECTAA Attendance Portal',
  CACHE_TTL_SECONDS: 600
};

const PARTICIPANT_COLUMNS = [
  'SN',
  'Batch',
  'Group Code',
  'Slot',
  'Session Time',
  'Start Date',
  'End Date',
  'Teacher Name',
  'CNIC',
  'School',
  'Tehsil',
  'Contact',
  'Training Center'
];

const ORIGINAL_ALLOCATION_COLUMNS = PARTICIPANT_COLUMNS.concat(['Snapshot At']);

const ATTENDANCE_COLUMNS = [
  'Timestamp',
  'Attendance Date',
  'Marked Center',
  'Marked Batch',
  'Marked Slot',
  'CNIC',
  'Teacher Name',
  'Registered Batch',
  'Registered Group Code',
  'Registered Slot',
  'Registered Session Time',
  'Registered Start Date',
  'Registered End Date',
  'School',
  'Tehsil',
  'Contact',
  'Registered Training Center',
  'Marked By',
  'Notes',
  'Attendance Key',
  'Attendance Status',
  'Absence Reason',
  'Absence Reason Detail'
];

const ABSENCE_REASON_COLUMNS = [
  'Reason',
  'Active',
  'Created By',
  'Created At',
  'Updated At',
  'Updated By',
  'Archived At',
  'Archived By',
  'Reason Key'
];

const DEFAULT_ABSENCE_REASONS = [
  { key: 'will_full_absent', reason: 'Will Full Absent', protected: false },
  { key: 'other', reason: 'Other', protected: true }
];

const DASHBOARD_SUMMARY_COLUMNS = [
  'Summary Type',
  'Label',
  'Records',
  'Unique Count',
  'Moved In',
  'Moved Out',
  'Net Change',
  'Updated At'
];

const AUDIT_COLUMNS = [
  'Timestamp',
  'Action',
  'Attendance Key',
  'CNIC',
  'Attendance Date',
  'Changed By',
  'Before',
  'After',
  'User Role',
  'Center'
];

const ADJUSTMENT_COLUMNS = [
  'Timestamp',
  'Adjustment Date',
  'Adjustment Key',
  'CNIC',
  'Teacher Name',
  'School',
  'Contact',
  'Old Center',
  'Old Batch',
  'Old Group Code',
  'Old Slot',
  'Old Session Time',
  'Old Start Date',
  'Old End Date',
  'New Center',
  'New Batch',
  'New Group Code',
  'New Slot',
  'New Session Time',
  'New Start Date',
  'New End Date',
  'Reason',
  'Changed By',
  'User Role',
  'Before',
  'After'
];

const USER_COLUMNS = [
  'Email',
  'Name',
  'Role',
  'Allowed Centers',
  'Active',
  'Notes',
  'Created At',
  'Updated At'
];

const ROLES = {
  VIEWER: 'viewer',
  EDITOR: 'editor',
  ADMIN: 'admin'
};
