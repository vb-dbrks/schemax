-- Unity Catalog import fixture for Schematic integration testing.
-- Purpose: create realistic metadata-only objects (no significant data load)
-- so `schematic import` can validate catalog/schema/table/view/metadata import.
--
-- Usage example:
--   databricks sql statement execute \
--     --profile dev \
--     --warehouse-id <WAREHOUSE_ID> \
--     --file packages/python-sdk/tests/resources/sql/unity_import_fixture.sql
--
-- Notes:
-- - This script is idempotent for repeated test runs.
-- - It intentionally focuses on objects currently supported by Schematic import.
-- - `__MANAGED_ROOT__` is replaced by the live integration test runner.

-- Optional reset (uncomment if you want a clean slate before re-running):
-- DROP CATALOG IF EXISTS test_import_fixture CASCADE;
-- DROP CATALOG IF EXISTS test_import_aux CASCADE;

CREATE CATALOG IF NOT EXISTS test_import_fixture
MANAGED LOCATION '__MANAGED_ROOT__/catalogs/test_import_fixture';
CREATE CATALOG IF NOT EXISTS test_import_aux
MANAGED LOCATION '__MANAGED_ROOT__/catalogs/test_import_aux';
COMMENT ON CATALOG test_import_fixture IS 'Schematic live import fixture catalog';
COMMENT ON CATALOG test_import_aux IS 'Schematic live import auxiliary catalog';

ALTER CATALOG test_import_fixture SET TAGS (
  'sf_fixture' = 'schematic-import',
  'sf_owner' = 'qa'
);

ALTER CATALOG test_import_aux SET TAGS (
  'sf_fixture' = 'schematic-import',
  'sf_purpose' = 'multi-catalog'
);

CREATE SCHEMA IF NOT EXISTS test_import_fixture.core
MANAGED LOCATION '__MANAGED_ROOT__/schemas/test_import_fixture/core';
CREATE SCHEMA IF NOT EXISTS test_import_fixture.analytics
MANAGED LOCATION '__MANAGED_ROOT__/schemas/test_import_fixture/analytics';
CREATE SCHEMA IF NOT EXISTS test_import_fixture.staging
MANAGED LOCATION '__MANAGED_ROOT__/schemas/test_import_fixture/staging';
CREATE SCHEMA IF NOT EXISTS test_import_aux.aux
MANAGED LOCATION '__MANAGED_ROOT__/schemas/test_import_aux/aux';
COMMENT ON SCHEMA test_import_fixture.core IS 'Core schema for fixture entities';
COMMENT ON SCHEMA test_import_fixture.analytics IS 'Analytics schema for fixture views';
COMMENT ON SCHEMA test_import_fixture.staging IS 'Staging schema for fixture landing objects';

ALTER SCHEMA test_import_fixture.core SET TAGS (
  'sf_domain' = 'commerce',
  'sf_tier' = 'gold'
);

ALTER SCHEMA test_import_fixture.analytics SET TAGS (
  'sf_domain' = 'reporting',
  'sf_tier' = 'silver'
);

ALTER SCHEMA test_import_aux.aux SET TAGS (
  'sf_domain' = 'shared',
  'sf_tier' = 'bronze'
);

CREATE OR REPLACE TABLE test_import_fixture.core.users (
  user_id BIGINT NOT NULL,
  email STRING NOT NULL COMMENT 'Primary login email',
  full_name STRING COMMENT 'Display name',
  country_code STRING COMMENT 'ISO country code',
  created_at TIMESTAMP COMMENT 'Creation timestamp',
  PRIMARY KEY (user_id)
)
USING DELTA
COMMENT 'User dimension table'
TBLPROPERTIES (
  'quality' = 'gold',
  'retention' = '365d',
  'pii' = 'true'
);

ALTER TABLE test_import_fixture.core.users SET TAGS (
  'sf_entity' = 'user',
  'sf_source' = 'fixture'
);

ALTER TABLE test_import_fixture.core.users ALTER COLUMN email SET TAGS (
  'sf_classification' = 'restricted',
  'sf_contains_pii' = 'true'
);

ALTER TABLE test_import_fixture.core.users ALTER COLUMN country_code SET TAGS (
  'sf_classification' = 'public'
);

CREATE OR REPLACE TABLE test_import_fixture.core.orders (
  order_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  order_date DATE COMMENT 'Order creation date',
  order_amount DECIMAL(12,2) COMMENT 'Gross order amount',
  status STRING COMMENT 'Order status',
  PRIMARY KEY (order_id),
  CONSTRAINT fk_orders_user FOREIGN KEY (user_id)
    REFERENCES test_import_fixture.core.users(user_id)
)
USING DELTA
PARTITIONED BY (order_date)
COMMENT 'Order fact table'
TBLPROPERTIES (
  'quality' = 'silver',
  'sensitivity' = 'medium',
  'ingestion_mode' = 'batch'
);
ALTER TABLE test_import_fixture.core.orders
ADD CONSTRAINT chk_order_amount_non_negative CHECK (order_amount >= 0);

ALTER TABLE test_import_fixture.core.orders SET TAGS (
  'sf_entity' = 'order',
  'sf_source' = 'fixture'
);

ALTER TABLE test_import_fixture.core.orders ALTER COLUMN order_amount SET TAGS (
  'sf_metric' = 'revenue'
);

CREATE OR REPLACE TABLE test_import_fixture.core.orders_clustered (
  order_id BIGINT NOT NULL,
  status STRING,
  order_amount DECIMAL(12,2),
  PRIMARY KEY (order_id)
)
USING DELTA
CLUSTER BY (status)
COMMENT 'Orders table with liquid clustering for import metadata coverage'
TBLPROPERTIES (
  'quality' = 'silver',
  'fixture' = 'clustered'
);

CREATE OR REPLACE TABLE test_import_fixture.staging.events_raw (
  event_id STRING NOT NULL,
  user_id BIGINT,
  event_type STRING,
  event_ts TIMESTAMP,
  payload STRING
)
USING DELTA
COMMENT 'Landing table for raw events'
TBLPROPERTIES (
  'quality' = 'bronze',
  'ingestion_mode' = 'streaming-like'
);

ALTER TABLE test_import_fixture.staging.events_raw SET TAGS (
  'sf_entity' = 'event',
  'sf_layer' = 'staging'
);

CREATE OR REPLACE TABLE test_import_fixture.staging.events_external (
  event_id STRING NOT NULL,
  event_type STRING,
  source STRING,
  payload STRING
)
USING DELTA
LOCATION '__MANAGED_ROOT__/external/events_external'
COMMENT 'External table for import fixture coverage'
TBLPROPERTIES (
  'quality' = 'bronze',
  'table_class' = 'external'
);

ALTER TABLE test_import_fixture.staging.events_external SET TAGS (
  'sf_entity' = 'event',
  'sf_storage' = 'external'
);

CREATE OR REPLACE TABLE test_import_aux.aux.lookup_country (
  country_code STRING NOT NULL,
  country_name STRING,
  region STRING,
  PRIMARY KEY (country_code)
)
USING DELTA
COMMENT 'Country lookup table for joins'
TBLPROPERTIES (
  'quality' = 'reference'
);

ALTER TABLE test_import_aux.aux.lookup_country SET TAGS (
  'sf_entity' = 'lookup',
  'sf_source' = 'fixture'
);

CREATE OR REPLACE VIEW test_import_fixture.analytics.v_orders_enriched AS
SELECT
  o.order_id,
  o.user_id,
  u.email,
  u.country_code,
  o.order_date,
  o.order_amount,
  o.status
FROM test_import_fixture.core.orders o
JOIN test_import_fixture.core.users u
  ON o.user_id = u.user_id;

ALTER VIEW test_import_fixture.analytics.v_orders_enriched SET TBLPROPERTIES (
  'comment' = 'Orders enriched with user attributes'
);
ALTER VIEW test_import_fixture.analytics.v_orders_enriched SET TAGS (
  'sf_consumer' = 'bi',
  'sf_purpose' = 'semantic'
);

CREATE OR REPLACE VIEW test_import_fixture.analytics.v_orders_by_country AS
SELECT
  u.country_code,
  COUNT(*) AS order_count,
  SUM(o.order_amount) AS total_amount
FROM test_import_fixture.core.orders o
JOIN test_import_fixture.core.users u
  ON o.user_id = u.user_id
GROUP BY u.country_code;

ALTER VIEW test_import_fixture.analytics.v_orders_by_country SET TBLPROPERTIES (
  'comment' = 'Country-level aggregate of orders'
);
ALTER VIEW test_import_fixture.analytics.v_orders_by_country SET TAGS (
  'sf_consumer' = 'finance',
  'sf_purpose' = 'aggregation'
);

-- Minimal seed rows (optional for smoke checks; not required for import metadata).
INSERT INTO test_import_fixture.core.users (user_id, email, full_name, country_code, created_at)
VALUES
  (1, 'alice@example.com', 'Alice Example', 'US', current_timestamp()),
  (2, 'bob@example.com', 'Bob Example', 'CA', current_timestamp());

INSERT INTO test_import_fixture.core.orders (order_id, user_id, order_date, order_amount, status)
VALUES
  (1001, 1, current_date(), 42.50, 'completed'),
  (1002, 2, current_date(), 99.99, 'pending');
