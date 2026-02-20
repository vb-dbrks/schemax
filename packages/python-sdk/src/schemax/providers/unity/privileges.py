"""
Unity Catalog privilege constants per securable object type.

Use for UI dropdowns, validation, and documentation.
Reference: https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges.html
"""

# Catalog-level privileges
CATALOG_PRIVILEGES = [
    "ALL PRIVILEGES",
    "USE CATALOG",
    "USE SCHEMA",
    "CREATE SCHEMA",
    "CREATE TABLE",
    "CREATE VIEW",
    "CREATE FUNCTION",
    "CREATE VOLUME",
    "CREATE EXTERNAL LOCATION",
    "CREATE STORAGE CREDENTIAL",
    "CREATE CONNECTION",
    "CREATE SHARING",
    "CREATE RECIPIENT",
    "CREATE PROVIDER",
    "CREATE CATALOG",
    "CREATE MODEL",
    "CREATE MATERIALIZED VIEW",
    "CREATE PIPELINE",
    "EXECUTE",
    "MODIFY",
    "SELECT",
    "READ VOLUME",
    "WRITE VOLUME",
    "APPLY TAG",
    "USE MARKETPLACE ASSETS",
]

# Schema-level privileges
SCHEMA_PRIVILEGES = [
    "ALL PRIVILEGES",
    "USE SCHEMA",
    "CREATE TABLE",
    "CREATE VIEW",
    "CREATE FUNCTION",
    "CREATE VOLUME",
    "CREATE MODEL",
    "CREATE MATERIALIZED VIEW",
    "CREATE PIPELINE",
    "EXECUTE",
    "MODIFY",
    "SELECT",
    "READ VOLUME",
    "WRITE VOLUME",
    "APPLY TAG",
]

# Table/View-level privileges (shared)
TABLE_VIEW_PRIVILEGES = [
    "ALL PRIVILEGES",
    "SELECT",
    "MODIFY",
    "READ VOLUME",
    "WRITE VOLUME",
    "APPLY TAG",
]

# Volume-level privileges (object-level grants on volumes)
VOLUME_PRIVILEGES = [
    "ALL PRIVILEGES",
    "READ VOLUME",
    "WRITE VOLUME",
    "MANAGE",
    "APPLY TAG",
]

# Function-level privileges (object-level grants on functions)
FUNCTION_PRIVILEGES = [
    "ALL PRIVILEGES",
    "EXECUTE",
    "APPLY TAG",
    "MANAGE",
]

# Materialized view privileges (SELECT to query, REFRESH to refresh)
MATERIALIZED_VIEW_PRIVILEGES = [
    "ALL PRIVILEGES",
    "SELECT",
    "REFRESH",
    "APPLY TAG",
]
