CREATE TABLE IF NOT EXISTS teachers (
    cnic TEXT PRIMARY KEY,
    teacher_name TEXT,
    mobile_no TEXT,
    gender TEXT,
    designation TEXT,
    subject TEXT,
    grade INTEGER,
    cadre TEXT,
    employment_type TEXT,
    degree_level TEXT,
    degree_subject TEXT,
    date_joining_service TEXT,
    date_joining_school TEXT,
    date_joining_post TEXT,
    status TEXT,
    district TEXT,
    tehsil TEXT,
    markaz TEXT,
    emis INTEGER,
    school TEXT,
    school_level TEXT,
    personal_no TEXT,
    age TEXT
);

DROP TABLE IF EXISTS pectaa_marking_2026;
CREATE TABLE pectaa_marking_2026 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cnic TEXT UNIQUE,
    applied_subject TEXT,
    custom_passing_year TEXT,
    custom_percentage TEXT,
    degree_image_url TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(cnic) REFERENCES teachers(cnic)
);
