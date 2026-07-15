ALTER TABLE private_institutes_survey_2026 ADD COLUMN institute_type TEXT NOT NULL DEFAULT 'School';
ALTER TABLE private_institutes_survey_2026 ADD COLUMN location_link_pin TEXT;
ALTER TABLE private_institutes_survey_2026 ADD COLUMN hidden_location_coordinates TEXT;
ALTER TABLE private_institutes_survey_2026 ADD COLUMN wing TEXT NOT NULL DEFAULT 'MEE';
