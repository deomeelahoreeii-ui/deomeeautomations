wrangler d1 create private_institutes_survey_production_db

wrangler d1 execute private_institutes_survey_production_db --local --file schema.sql --yes

wrangler d1 execute private_institutes_survey_production_db --remote --file schema.sql --yes

wrangler d1 execute private_institutes_survey_production_db --remote --file migrations/20260701_add_location_wing_fields.sql --yes

wrangler d1 execute private_institutes_survey_production_db --local --command="SELECT COUNT(*) AS count FROM private_institutes_survey_2026;" --yes
