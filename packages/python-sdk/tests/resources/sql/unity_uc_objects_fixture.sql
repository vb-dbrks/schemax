-- Live E2E fixture: Unity Catalog schema-level objects (volume, function, materialized view).
-- Tokens replaced in test:
--   test_uc_objects   -> unique catalog name
--   __MANAGED_ROOT__ -> unique managed location path
--
-- Creates: catalog, schema, base table (for MV dependency), managed volume,
--          SQL function, materialized view. Used to verify import and discovery
--          see volumes/functions/MVs, and to run validate/sql/diff/apply flows.

CREATE CATALOG IF NOT EXISTS test_uc_objects
MANAGED LOCATION '__MANAGED_ROOT__/catalog';

CREATE SCHEMA IF NOT EXISTS test_uc_objects.uc_core
MANAGED LOCATION '__MANAGED_ROOT__/uc_core';

CREATE OR REPLACE TABLE test_uc_objects.uc_core.base_fact (
  id BIGINT NOT NULL,
  label STRING,
  CONSTRAINT pk_base_fact PRIMARY KEY (id)
)
USING DELTA
COMMENT 'Base table for MV dependency';

CREATE VOLUME IF NOT EXISTS test_uc_objects.uc_core.managed_vol
COMMENT 'Managed volume for E2E';

CREATE OR REPLACE FUNCTION test_uc_objects.uc_core.live_e2e_func()
RETURNS INT
LANGUAGE SQL
RETURN (42);

CREATE MATERIALIZED VIEW IF NOT EXISTS test_uc_objects.uc_core.live_e2e_mv
COMMENT 'E2E materialized view'
AS SELECT id, label FROM test_uc_objects.uc_core.base_fact;
