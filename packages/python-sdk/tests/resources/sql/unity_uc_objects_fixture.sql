-- Live E2E fixture: Unity Catalog schema-level objects (volume, function, materialized view).
-- Tokens replaced in test:
--   test_uc_objects   -> unique catalog name
--   __MANAGED_ROOT__ -> unique managed location path
--   __EXTERNAL_LOCATION__ -> (optional) external location path for external volume
--
-- Creates: catalog, schema, base table (for MV dependency), second table with
--          PARTITIONED BY/CLUSTER BY, managed volume, external volume (if path set),
--          SQL function, Python UDF, permanent view, materialized view with schedule,
--          MV with PARTITIONED BY. Used to verify import and discovery of all object types.

CREATE CATALOG IF NOT EXISTS test_uc_objects
MANAGED LOCATION '__MANAGED_ROOT__/catalog';

CREATE SCHEMA IF NOT EXISTS test_uc_objects.uc_core
MANAGED LOCATION '__MANAGED_ROOT__/uc_core';

CREATE OR REPLACE TABLE test_uc_objects.uc_core.base_fact (
  id BIGINT NOT NULL,
  label STRING,
  dt DATE,
  amount DECIMAL(10,2),
  payload VARIANT,
  CONSTRAINT pk_base_fact PRIMARY KEY (id)
)
USING DELTA
COMMENT 'Base table for MV dependency';

-- Delta: use PARTITIONED BY or CLUSTER BY, not both (Databricks restriction)
CREATE OR REPLACE TABLE test_uc_objects.uc_core.events_partitioned (
  id BIGINT NOT NULL,
  dt DATE NOT NULL,
  name STRING
)
USING DELTA
PARTITIONED BY (dt)
COMMENT 'Partitioned table for E2E';

CREATE VIEW test_uc_objects.uc_core.base_fact_view
COMMENT 'Permanent view over base_fact'
AS SELECT id, label, dt FROM test_uc_objects.uc_core.base_fact;

CREATE VOLUME IF NOT EXISTS test_uc_objects.uc_core.managed_vol
COMMENT 'Managed volume for E2E';

-- External volume: only created when __EXTERNAL_LOCATION__ is replaced with a real path
-- CREATE EXTERNAL VOLUME IF NOT EXISTS test_uc_objects.uc_core.external_vol
-- LOCATION '__EXTERNAL_LOCATION__/external_vol'
-- COMMENT 'External volume for E2E';

CREATE OR REPLACE FUNCTION test_uc_objects.uc_core.live_e2e_func()
RETURNS INT
LANGUAGE SQL
RETURN (42);

CREATE OR REPLACE FUNCTION test_uc_objects.uc_core.live_e2e_py_udf(x DOUBLE, y DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
AS $$
def weight(x, y):
    return x * y
return weight(x, y)
$$;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_uc_objects.uc_core.live_e2e_mv
COMMENT 'E2E materialized view'
AS SELECT id, label FROM test_uc_objects.uc_core.base_fact;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_uc_objects.uc_core.live_e2e_mv_scheduled
COMMENT 'E2E MV with refresh schedule'
REFRESH EVERY 1 DAY
AS SELECT id, dt, label FROM test_uc_objects.uc_core.base_fact;
