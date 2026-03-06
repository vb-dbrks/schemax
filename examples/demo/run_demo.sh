#!/usr/bin/env bash
set -e

###############################################################################
# SchemaX Interactive Demo
#
# A guided walkthrough of SchemaX features including project initialization,
# SQL import, schema evolution, snapshot management, diffing, and DAB bundle
# generation.
#
# Usage:
#   ./run_demo.sh                  # Interactive mode (pauses between steps)
#   ./run_demo.sh --non-interactive  # Runs all steps without pausing
#
# Prerequisites:
#   - schemax CLI installed and on PATH
###############################################################################

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INTERACTIVE=true
if [[ "${1:-}" == "--non-interactive" ]]; then
    INTERACTIVE=false
fi

DEMO_DIR=""  # set in setup; cleaned up on exit

# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

print_color() {
    local color="$1"
    shift
    printf "${color}%s${RESET}\n" "$*"
}

print_header() {
    local step="$1"
    local title="$2"
    echo ""
    printf "${MAGENTA}${BOLD}%s${RESET}\n" "============================================================================"
    printf "${MAGENTA}${BOLD}  Step %s: %s${RESET}\n" "$step" "$title"
    printf "${MAGENTA}${BOLD}%s${RESET}\n" "============================================================================"
    echo ""
}

print_info() {
    printf "${CYAN}  [info]${RESET} %s\n" "$*"
}

print_success() {
    printf "${GREEN}  [ok]${RESET}   %s\n" "$*"
}

print_warn() {
    printf "${YELLOW}  [warn]${RESET} %s\n" "$*"
}

print_cmd() {
    printf "\n${BLUE}${BOLD}  \$ %s${RESET}\n\n" "$*"
}

print_file_content() {
    local label="$1"
    local file="$2"
    printf "\n${DIM}  --- %s ---${RESET}\n" "$label"
    sed 's/^/    /' "$file"
    printf "${DIM}  --- end ---${RESET}\n\n"
}

pause() {
    if $INTERACTIVE; then
        printf "${YELLOW}  Press Enter to continue...${RESET}"
        read -r
    fi
}

run_cmd() {
    # Print the command, execute it, and show output indented
    print_cmd "$*"
    local output
    if output=$("$@" 2>&1); then
        if [[ -n "$output" ]]; then
            echo "$output" | sed 's/^/    /'
        fi
        echo ""
        return 0
    else
        local rc=$?
        if [[ -n "$output" ]]; then
            echo "$output" | sed 's/^/    /'
        fi
        echo ""
        return $rc
    fi
}

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
cleanup() {
    if [[ -n "$DEMO_DIR" && -d "$DEMO_DIR" ]]; then
        print_info "Cleaning up temporary directory: $DEMO_DIR"
        rm -rf "$DEMO_DIR"
        print_success "Cleanup complete."
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
preflight() {
    printf "${BOLD}${CYAN}"
    cat << 'BANNER'

   ____       _                          __  __
  / ___|  ___| |__   ___ _ __ ___   __ _\ \/ /
  \___ \ / __| '_ \ / _ \ '_ ` _ \ / _` |\  /
   ___) | (__| | | |  __/ | | | | | (_| |/  \
  |____/ \___|_| |_|\___|_| |_| |_|\__,_/_/\_\

  Interactive Demo
BANNER
    printf "${RESET}\n"

    print_info "Checking prerequisites..."

    if ! command -v schemax &>/dev/null; then
        print_warn "schemax CLI not found on PATH."
        print_warn "This demo will show the commands that WOULD be run."
        print_warn "Install schemax first for a live demo:  pip install schemax"
        echo ""
        SCHEMAX_AVAILABLE=false
    else
        local version
        version=$(schemax --version 2>/dev/null || echo "unknown")
        print_success "schemax CLI found: $version"
        SCHEMAX_AVAILABLE=true
    fi

    # Create a temp working directory
    DEMO_DIR=$(mktemp -d "${TMPDIR:-/tmp}/schemax-demo.XXXXXX")
    print_success "Working directory: $DEMO_DIR"

    pause
}

# ---------------------------------------------------------------------------
# Helper: run schemax or show what would run
# ---------------------------------------------------------------------------
run_schemax() {
    if $SCHEMAX_AVAILABLE; then
        run_cmd schemax "$@"
    else
        print_cmd "schemax $*"
        printf "${DIM}    (skipped -- schemax not installed)${RESET}\n\n"
    fi
}

# ---------------------------------------------------------------------------
# Step 1: Initialize project
# ---------------------------------------------------------------------------
step_1_init() {
    print_header "1" "Initialize a SchemaX Project"

    print_info "Every SchemaX project starts with 'schemax init'."
    print_info "This creates the .schemax/ directory with project.json,"
    print_info "changelog, environments, and snapshots scaffolding."
    echo ""

    cd "$DEMO_DIR"

    run_schemax init --provider unity

    # Add catalog mappings so SQL generation knows logical -> physical names
    if [[ -f "$DEMO_DIR/.schemax/project.json" ]]; then
        python3 -c "
import json, pathlib
p = pathlib.Path('$DEMO_DIR/.schemax/project.json')
proj = json.loads(p.read_text())
for env_name, env_cfg in proj['provider']['environments'].items():
    env_cfg['catalogMappings'] = {'analytics': f'{env_name}_analytics'}
p.write_text(json.dumps(proj, indent=2))
"
    fi

    if [[ -f "$DEMO_DIR/.schemax/project.json" ]]; then
        print_success "Project initialized. Let's look at the project file:"
        print_file_content "project.json" "$DEMO_DIR/.schemax/project.json"
    else
        print_info "Expected file: .schemax/project.json"
    fi

    if [[ -d "$DEMO_DIR/.schemax" ]]; then
        print_info "Project structure:"
        find "$DEMO_DIR/.schemax" -type f | sort | sed "s|$DEMO_DIR/||" | sed 's/^/    /'
        echo ""
    fi

    pause
}

# ---------------------------------------------------------------------------
# Step 2: Import from SQL DDL
# ---------------------------------------------------------------------------
step_2_import_sql() {
    print_header "2" "Import Schema from SQL DDL"

    print_info "SchemaX can import existing schemas from SQL DDL files."
    print_info "This is useful for onboarding existing databases."
    print_info "Let's create a sample DDL that uses rich Unity Catalog features."
    echo ""

    # Create the sample DDL file
    cat > "$DEMO_DIR/demo_schema.sql" << 'SQL'
-- ==========================================================================
-- SchemaX Demo: Unity Catalog DDL
-- ==========================================================================

-- Create a catalog for our analytics platform
CREATE CATALOG IF NOT EXISTS analytics;

-- Create schemas for different domains
CREATE SCHEMA IF NOT EXISTS analytics.core
COMMENT 'Core business entities and reference data';

CREATE SCHEMA IF NOT EXISTS analytics.reporting
COMMENT 'Reporting views and materialized aggregations';

-- -------------------------------------------------------------------------
-- Core tables with Delta format, constraints, comments, tags, properties
-- -------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.core.customers (
    customer_id     BIGINT        NOT NULL  COMMENT 'Unique customer identifier',
    email           STRING        NOT NULL  COMMENT 'Customer email address',
    full_name       STRING        NOT NULL  COMMENT 'Full legal name',
    tier            STRING        DEFAULT 'standard' COMMENT 'Customer tier: standard, premium, enterprise',
    region          STRING                  COMMENT 'Geographic region code',
    created_at      TIMESTAMP     NOT NULL  COMMENT 'Account creation timestamp',
    updated_at      TIMESTAMP              COMMENT 'Last profile update timestamp',
    is_active       BOOLEAN       DEFAULT true COMMENT 'Whether the account is active',

    CONSTRAINT pk_customers PRIMARY KEY (customer_id),
    CONSTRAINT uq_email UNIQUE (email),
    CONSTRAINT ck_tier CHECK (tier IN ('standard', 'premium', 'enterprise'))
)
USING DELTA
COMMENT 'Master customer records for the analytics platform'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'quality'                          = 'gold'
);

ALTER TABLE analytics.core.customers SET TAGS ('pii' = 'true', 'domain' = 'customer');
ALTER TABLE analytics.core.customers ALTER COLUMN email SET TAGS ('pii_type' = 'email');
ALTER TABLE analytics.core.customers ALTER COLUMN full_name SET TAGS ('pii_type' = 'name');


CREATE TABLE IF NOT EXISTS analytics.core.orders (
    order_id        BIGINT        NOT NULL  COMMENT 'Unique order identifier',
    customer_id     BIGINT        NOT NULL  COMMENT 'FK to customers table',
    order_date      DATE          NOT NULL  COMMENT 'Date the order was placed',
    status          STRING        NOT NULL  COMMENT 'Order status: pending, confirmed, shipped, delivered, cancelled',
    total_amount    DECIMAL(18,2) NOT NULL  COMMENT 'Total order amount in USD',
    currency        STRING        DEFAULT 'USD' COMMENT 'Currency code (ISO 4217)',
    item_count      INT                     COMMENT 'Number of distinct items',
    shipped_at      TIMESTAMP              COMMENT 'Shipment timestamp',

    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES analytics.core.customers (customer_id),
    CONSTRAINT ck_status CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
    CONSTRAINT ck_amount CHECK (total_amount >= 0)
)
USING DELTA
COMMENT 'Customer order records with line-item summary'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality'                          = 'gold'
);

ALTER TABLE analytics.core.orders SET TAGS ('domain' = 'sales');


-- -------------------------------------------------------------------------
-- Reporting views with dependencies on core tables
-- -------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS analytics.reporting.customer_order_summary
COMMENT 'Aggregated order metrics per customer for dashboards'
AS
SELECT
    c.customer_id,
    c.full_name,
    c.tier,
    c.region,
    COUNT(o.order_id)        AS total_orders,
    SUM(o.total_amount)      AS lifetime_value,
    AVG(o.total_amount)      AS avg_order_value,
    MIN(o.order_date)        AS first_order_date,
    MAX(o.order_date)        AS last_order_date
FROM analytics.core.customers c
LEFT JOIN analytics.core.orders o ON c.customer_id = o.customer_id
WHERE c.is_active = true
GROUP BY c.customer_id, c.full_name, c.tier, c.region;
SQL

    print_success "Created demo_schema.sql with:"
    print_info "  - 1 catalog (analytics)"
    print_info "  - 2 schemas (core, reporting)"
    print_info "  - 2 tables with Delta format, constraints, comments, tags, properties"
    print_info "  - 1 view with JOIN across tables"
    echo ""

    print_file_content "demo_schema.sql" "$DEMO_DIR/demo_schema.sql"

    pause

    print_info "Now importing the DDL into SchemaX..."
    echo ""

    cd "$DEMO_DIR"
    run_schemax import --from-sql demo_schema.sql --mode replace

    print_info "Validating the imported schema..."
    run_schemax validate

    pause
}

# ---------------------------------------------------------------------------
# Step 3: Generate SQL and create baseline snapshot
# ---------------------------------------------------------------------------
step_3_sql_and_snapshot() {
    print_header "3" "Generate Migration SQL and Create a Snapshot"

    print_info "SchemaX generates SQL migration scripts from the changelog."
    print_info "The --target flag maps logical catalog names to physical ones."
    echo ""

    cd "$DEMO_DIR"
    run_schemax sql --target dev

    pause

    print_info "Now let's freeze this state as a versioned snapshot."
    print_info "Snapshots are immutable points in time for your schema."
    echo ""

    run_schemax snapshot create --name "Initial schema" --comment "Baseline from DDL import"

    if [[ -d "$DEMO_DIR/.schemax/snapshots" ]]; then
        print_info "Snapshot files:"
        find "$DEMO_DIR/.schemax/snapshots" -type f | sort | sed "s|$DEMO_DIR/||" | sed 's/^/    /'
        echo ""
    fi

    pause
}

# ---------------------------------------------------------------------------
# Step 4: Evolve the schema (ALTER operations)
# ---------------------------------------------------------------------------
step_4_alter() {
    print_header "4" "Evolve the Schema with ALTER Operations"

    print_info "Real-world schemas evolve over time. Let's simulate adding"
    print_info "new columns, updating properties, and setting tags."
    echo ""

    cat > "$DEMO_DIR/alter_schema.sql" << 'SQL'
-- ==========================================================================
-- SchemaX Demo: Schema Evolution (v2)
-- ==========================================================================

-- Add a loyalty_points column to customers
ALTER TABLE analytics.core.customers
ADD COLUMN loyalty_points INT COMMENT 'Accumulated loyalty points balance';

-- Add a discount_pct column to orders
ALTER TABLE analytics.core.orders
ADD COLUMN discount_pct DECIMAL(5,2) COMMENT 'Discount percentage applied to the order';

-- Add a promo_code column to orders
ALTER TABLE analytics.core.orders
ADD COLUMN promo_code STRING COMMENT 'Promotional code used for the order';

-- Update table properties for optimized reads
ALTER TABLE analytics.core.orders
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '8');

-- Add data classification tags
ALTER TABLE analytics.core.customers ALTER COLUMN loyalty_points
SET TAGS ('sensitivity' = 'internal');

-- Update table-level comment with richer description
COMMENT ON TABLE analytics.core.orders IS
'Customer order records with line-item summary. Includes discount and promo tracking (v2).';
SQL

    print_success "Created alter_schema.sql with:"
    print_info "  - 3 new columns (loyalty_points, discount_pct, promo_code)"
    print_info "  - Updated table properties (dataSkippingNumIndexedCols)"
    print_info "  - New column-level tags (sensitivity)"
    print_info "  - Updated table comment"
    echo ""

    print_file_content "alter_schema.sql" "$DEMO_DIR/alter_schema.sql"

    pause

    print_info "Importing the ALTER statements..."
    echo ""

    cd "$DEMO_DIR"
    run_schemax import --from-sql alter_schema.sql

    print_info "Generating incremental migration SQL (changes only)..."
    echo ""
    run_schemax sql --target dev

    pause
}

# ---------------------------------------------------------------------------
# Step 5: Snapshot v2 and diff
# ---------------------------------------------------------------------------
step_5_snapshot_and_diff() {
    print_header "5" "Create v2 Snapshot and Diff Between Versions"

    print_info "Let's snapshot the updated schema and compare versions."
    echo ""

    cd "$DEMO_DIR"
    run_schemax snapshot create --name "Added columns and tags"

    pause

    print_info "Now diffing between the two snapshots..."
    print_info "This shows exactly what changed between v0.1.0 and v0.2.0."
    echo ""

    run_schemax diff --from v0.1.0 --to v0.2.0 --show-sql --target dev

    pause
}

# ---------------------------------------------------------------------------
# Step 6: Generate DAB resources
# ---------------------------------------------------------------------------
step_6_bundle() {
    print_header "6" "Generate Databricks Asset Bundle Resources"

    print_info "SchemaX can generate DAB (Databricks Asset Bundle) resources"
    print_info "so you can deploy schema changes via CI/CD pipelines."
    echo ""

    cd "$DEMO_DIR"
    run_schemax bundle

    if [[ -d "$DEMO_DIR/resources" ]]; then
        print_info "Generated bundle files:"
        find "$DEMO_DIR/resources" -type f | sort | sed "s|$DEMO_DIR/||" | sed 's/^/    /'
        echo ""

        for f in "$DEMO_DIR/resources"/*; do
            if [[ -f "$f" ]]; then
                local basename
                basename=$(basename "$f")
                print_file_content "$basename" "$f"
            fi
        done
    fi

    pause
}

# ---------------------------------------------------------------------------
# Step 7: Summary and cleanup
# ---------------------------------------------------------------------------
step_7_summary() {
    print_header "7" "Demo Complete"

    printf "${GREEN}${BOLD}"
    cat << 'BANNER'

    Demo walkthrough finished successfully!

BANNER
    printf "${RESET}"

    print_info "Here's what we covered:"
    echo ""
    printf "${BOLD}    1.${RESET} ${CYAN}schemax init${RESET}             - Initialize a project with Unity Catalog provider\n"
    printf "${BOLD}    2.${RESET} ${CYAN}schemax import --from-sql${RESET} - Import rich DDL (catalogs, schemas, tables, views)\n"
    printf "${BOLD}    3.${RESET} ${CYAN}schemax sql${RESET}              - Generate migration SQL for a target environment\n"
    printf "${BOLD}    4.${RESET} ${CYAN}schemax snapshot create${RESET}  - Freeze schema state as an immutable snapshot\n"
    printf "${BOLD}    5.${RESET} ${CYAN}schemax diff${RESET}             - Compare snapshots and show SQL diff\n"
    printf "${BOLD}    6.${RESET} ${CYAN}schemax bundle${RESET}           - Generate Databricks Asset Bundle resources\n"
    printf "${BOLD}    7.${RESET} ${CYAN}schemax validate${RESET}         - Validate project integrity at any time\n"
    echo ""
    print_info "Other commands not shown in this demo:"
    printf "    ${DIM}schemax apply${RESET}           - Execute migrations against a live Databricks workspace\n"
    printf "    ${DIM}schemax rollback${RESET}        - Roll back a deployment (partial or complete)\n"
    printf "    ${DIM}schemax changelog undo${RESET}  - Undo specific pending changelog operations\n"
    printf "    ${DIM}schemax workspace-state${RESET} - Inspect the current workspace state\n"
    echo ""
    print_info "Working directory was: $DEMO_DIR"
    print_info "It will be cleaned up automatically on exit."
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    preflight
    step_1_init
    step_2_import_sql
    step_3_sql_and_snapshot
    step_4_alter
    step_5_snapshot_and_diff
    step_6_bundle
    step_7_summary
}

main "$@"
