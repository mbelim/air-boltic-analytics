
-- Used ChatGPT to understand the sql logic to flatten the nested json structure for databricks SQL
-- Using this prompt "Help me flatten the nested json files to read it as table in databricks, example json: { "Boeing": etc... }"

-- Assumption:
-- The airplane model specifications were provided as a static reference dataset rather than a frequently changing operational table.
-- Since this data changes rarely and is small in size, it is stored locally under /seeds as a JSON file for simplicity and version control.
--In production, this reference data would likely live in an S3 bucket or Delta table under a reference or lookup schema and be refreshed periodically.

WITH src AS (
  SELECT from_json(
    _corrupt_record,
    'MAP<STRING, MAP<STRING, STRUCT<max_seats INT, max_weight INT, max_distance INT, engine_type STRING>>>'
  ) AS data
  FROM ref {{ 'aeroplane_model' }}
),
mfr AS (
  SELECT explode(data) AS (manufacturer, models)
  FROM src
),
mdl AS (
  SELECT manufacturer, explode(models) AS (airplane_model, specs)
  FROM mfr
)
SELECT
  manufacturer,
  airplane_model,
  specs.max_seats      AS max_seats,
  specs.max_weight     AS max_weight,
  specs.max_distance   AS max_distance,
  specs.engine_type    AS engine_type
FROM mdl;
