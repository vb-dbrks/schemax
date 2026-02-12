-- Live command matrix fixture
-- Tokens replaced in test:
--   test_cmd_fixture -> unique catalog
--   __MANAGED_ROOT__ -> unique managed location path

CREATE CATALOG IF NOT EXISTS test_cmd_fixture
MANAGED LOCATION '__MANAGED_ROOT__/catalog';

CREATE SCHEMA IF NOT EXISTS test_cmd_fixture.core
MANAGED LOCATION '__MANAGED_ROOT__/core';

CREATE SCHEMA IF NOT EXISTS test_cmd_fixture.analytics;

CREATE OR REPLACE TABLE test_cmd_fixture.core.customers (
  customer_id BIGINT NOT NULL,
  email STRING NOT NULL,
  created_at TIMESTAMP,
  CONSTRAINT pk_customers PRIMARY KEY (customer_id)
)
USING DELTA
COMMENT 'Customer master table'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain' = 'crm'
);

CREATE OR REPLACE TABLE test_cmd_fixture.core.orders (
  order_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  order_ts TIMESTAMP,
  amount DECIMAL(12,2),
  CONSTRAINT pk_orders PRIMARY KEY (order_id),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
    REFERENCES test_cmd_fixture.core.customers(customer_id)
)
USING DELTA
PARTITIONED BY (order_ts)
COMMENT 'Orders fact table'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain' = 'sales'
);

ALTER TABLE test_cmd_fixture.core.customers SET TAGS ('sf_entity' = 'customer', 'sf_owner' = 'analytics');
ALTER TABLE test_cmd_fixture.core.orders SET TAGS ('sf_entity' = 'order', 'sf_owner' = 'analytics');
ALTER TABLE test_cmd_fixture.core.customers ALTER COLUMN email SET TAGS ('sf_classification' = 'restricted');

CREATE OR REPLACE VIEW test_cmd_fixture.analytics.v_orders AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_ts,
  o.amount,
  c.email
FROM test_cmd_fixture.core.orders o
JOIN test_cmd_fixture.core.customers c ON o.customer_id = c.customer_id;

ALTER VIEW test_cmd_fixture.analytics.v_orders
SET TAGS ('sf_exposure' = 'internal');
