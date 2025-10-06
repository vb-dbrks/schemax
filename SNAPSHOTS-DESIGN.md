# SchemaX Snapshots & Migration Design

## Overview

SchemaX will support a **hybrid state+change-based migration system** inspired by SSDT, Liquibase, and Flyway.

## Core Concepts

### 1. Operations (Already Implemented)
- **Append-only log** of all user actions
- **Immutable** - never edited or deleted
- **Granular** - one op per atomic change
- **Traceable** - includes timestamp, target, payload

### 2. Snapshots (New)
- **Point-in-time** complete state
- **Named & versioned** (semantic versioning)
- **Cryptographically hashed** for integrity
- **References ops** that led to this state

### 3. Deployments (New)
- **Per-environment** tracking (dev, test, prod)
- **Records what was applied** and when
- **Tracks versions** deployed to each environment
- **Detects drift** from expected state

## Data Model

### Enhanced Project File

```typescript
interface ProjectFile {
  version: 1;
  name: string;
  environments: Environment[];
  
  // Current working state
  state: {
    catalogs: Catalog[];
  };
  
  // All operations (append-only)
  ops: Op[];
  
  // Snapshots (new)
  snapshots: Snapshot[];
  
  // Deployment history (new)
  deployments: Deployment[];
  
  // Settings (new)
  settings: ProjectSettings;
}

interface Snapshot {
  id: string;              // snap_uuid
  version: string;         // semantic version (1.0.0)
  name: string;            // user-provided description
  ts: string;              // ISO timestamp
  createdBy: string;       // user email/name
  
  // Full state at this point
  state: {
    catalogs: Catalog[];
  };
  
  // Operations included in this snapshot
  opsIncluded: string[];   // op IDs
  
  // Previous snapshot (for delta calculation)
  previousSnapshot: string | null;
  
  // Integrity
  hash: string;            // SHA-256 of state + ops
  
  // Metadata
  tags: string[];          // ["production", "milestone"]
  comment?: string;
}

interface Deployment {
  id: string;
  environment: string;     // "dev" | "test" | "prod"
  ts: string;
  deployedBy: string;
  
  // What was deployed
  snapshotId: string | null;      // Full deployment
  opsApplied: string[];           // Incremental deployment
  
  // Tracking
  schemaVersion: string;          // snapshot version or "incremental"
  sqlGenerated: string;           // SQL script that was run
  
  // Result
  status: "success" | "failed" | "rolled_back";
  error?: string;
  
  // Drift detection
  driftDetected: boolean;
  driftDetails?: DriftInfo[];
}

interface DriftInfo {
  objectType: "catalog" | "schema" | "table" | "column";
  objectName: string;
  expectedVersion: string;
  actualVersion: string | null;
  issue: "missing" | "modified" | "extra";
}

interface ProjectSettings {
  // Versioning
  autoIncrementVersion: boolean;  // auto-bump minor on snapshot
  versionPrefix: string;          // e.g., "v" for v1.0.0
  
  // Deployment
  requireSnapshotForProd: boolean;
  allowDrift: boolean;
  
  // Safety
  requireComments: boolean;
  warnOnBreakingChanges: boolean;
}
```

## User Workflows

### Workflow 1: Initial Development

```
1. User creates catalogs, schemas, tables (ops accumulate)
2. User clicks "Create Snapshot" → v0.1.0
3. User continues editing (more ops)
4. User clicks "Create Snapshot" → v0.2.0
5. User clicks "Deploy to Dev"
   → Generates full CREATE script from v0.2.0
   → Applies to Databricks dev workspace
   → Records deployment
   → Tags all objects with schemax.version=0.2.0
```

### Workflow 2: Incremental Changes

```
1. User in prod (current: v1.0.0)
2. User adds column, renames table (ops accumulate)
3. User clicks "Create Snapshot" → v1.1.0
4. User clicks "Deploy to Prod"
   → Calculates diff: v1.0.0 → v1.1.0
   → Generates ALTER scripts only
   → Checks drift: compares UC properties
   → If safe, applies migration
   → Updates schemax.version to 1.1.0
```

### Workflow 3: Drift Detection

```
1. Someone manually ALTERs a table in UC
2. User tries to deploy v1.2.0
3. SchemaX checks schemax.version property
4. Mismatch detected: UC says v1.0.0, project says v1.1.0
5. Warning shown: "Drift detected. Schema was modified outside SchemaX."
6. Options:
   a) Abort deployment
   b) Force overwrite (dangerous)
   c) Import changes from UC
```

### Workflow 4: Environment Promotion

```
Dev (v1.2.0) → Test (v1.1.0) → Prod (v1.0.0)

1. Deploy v1.2.0 to dev → succeeds
2. Test in dev environment
3. Click "Promote to Test"
   → Generates migration: v1.1.0 → v1.2.0
   → Reviews SQL
   → Deploys to test
4. Validate in test
5. Click "Promote to Prod"
   → Requires snapshot (safety)
   → Generates migration: v1.0.0 → v1.2.0
   → Reviews SQL (shows all changes)
   → Requires confirmation
   → Deploys to prod
```

## Migration Generation

### Algorithm: State Diff

```typescript
function generateMigration(
  fromSnapshot: Snapshot,
  toSnapshot: Snapshot,
  ops: Op[]
): SQLScript {
  // 1. Filter ops between snapshots
  const relevantOps = ops.filter(op => 
    fromSnapshot.opsIncluded.includes(op.id) === false &&
    toSnapshot.opsIncluded.includes(op.id) === true
  );
  
  // 2. Group by object
  const byTable = groupByTarget(relevantOps);
  
  // 3. Analyze for breaking changes
  const breakingChanges = detectBreaking(relevantOps);
  
  // 4. Generate SQL
  const sql = relevantOps.map(op => opToSQL(op));
  
  // 5. Add version tracking
  sql.push(updateVersionProperties(toSnapshot.version));
  
  return { sql, breakingChanges };
}
```

### SQL Generation Examples

**Op: add_column**
```sql
ALTER TABLE catalog.schema.table 
ADD COLUMN email STRING COMMENT 'User email';

-- Update version tracking
ALTER TABLE catalog.schema.table 
SET TBLPROPERTIES (
  'schemax.version' = '1.1.0',
  'schemax.deployed_at' = '2025-10-06T10:00:00Z'
);
```

**Op: rename_column** (Breaking!)
```sql
-- Requires columnMapping=name
ALTER TABLE catalog.schema.table 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE catalog.schema.table 
RENAME COLUMN old_name TO new_name;

-- Update version
ALTER TABLE catalog.schema.table 
SET TBLPROPERTIES ('schemax.version' = '1.1.0');
```

**Op: drop_column** (Breaking!)
```sql
-- WARNING: This is a breaking change!
-- Data in this column will be lost.
-- Consider backing up data first.

ALTER TABLE catalog.schema.table 
DROP COLUMN column_name;
```

## Breaking Change Detection

### Rules

| Operation | Breaking? | Reason |
|-----------|-----------|---------|
| add_column (nullable) | ❌ No | Safe, existing queries unaffected |
| add_column (not null) | ⚠️ Maybe | Depends if table has data |
| drop_column | ✅ Yes | Queries referencing column will fail |
| rename_column | ✅ Yes | Queries using old name will fail |
| rename_table | ✅ Yes | All queries will fail |
| change_column_type | ⚠️ Maybe | Depends on type compatibility |
| reorder_columns | ❌ No | Only affects SELECT * |

### Warning UI

```
⚠️ Breaking Changes Detected

This migration contains 2 breaking changes:

1. RENAME COLUMN orders.status → orders.order_status
   Impact: Existing queries using 'status' will fail
   
2. DROP COLUMN orders.legacy_id
   Impact: Data will be permanently lost

Recommendation:
- Deploy during maintenance window
- Update dependent queries first
- Consider gradual rollout

[Review SQL] [Cancel] [Deploy Anyway]
```

## Version Tracking in Unity Catalog

### Table Properties

Every SchemaX-managed table will have:

```sql
TBLPROPERTIES (
  -- Version tracking
  'schemax.version' = '1.2.0',
  'schemax.snapshot' = 'snap_abc123',
  
  -- Deployment info
  'schemax.deployed_at' = '2025-10-06T10:00:00Z',
  'schemax.deployed_by' = 'user@company.com',
  'schemax.environment' = 'prod',
  
  -- Change tracking
  'schemax.last_op' = 'op_xyz789',
  'schemax.ops_applied' = 'op_1,op_2,op_3',
  
  -- Integrity
  'schemax.hash' = 'sha256:abc...'
)
```

### Drift Detection Query

```sql
-- Check if table was modified outside SchemaX
SHOW TBLPROPERTIES catalog.schema.table;

-- Compare:
-- - Expected: schemax.version from project file
-- - Actual: schemax.version from UC
-- - If mismatch → drift detected
```

## UI Enhancements

### New Commands

1. **SchemaX: Create Snapshot**
   - Prompts for version (auto-suggests next)
   - Prompts for name/description
   - Creates snapshot
   - Shows summary

2. **SchemaX: Deploy to Environment**
   - Select environment (dev/test/prod)
   - Shows migration preview (SQL)
   - Highlights breaking changes
   - Requires confirmation
   - Executes deployment

3. **SchemaX: Check Drift**
   - Connects to UC
   - Compares versions
   - Reports discrepancies
   - Offers to import changes

4. **SchemaX: View Snapshots**
   - Lists all snapshots
   - Shows version timeline
   - Allows comparison
   - Can revert to snapshot

### New UI Components

**Snapshot Panel:**
```
Snapshots (3)
├── v1.0.0 - "Production Release" (2025-09-01) [DEPLOYED: prod]
├── v0.2.0 - "Added customer tables" (2025-08-15) [DEPLOYED: test]
└── v0.1.0 - "Initial schema" (2025-08-01) [DEPLOYED: dev]

Current: 12 uncommitted operations

[Create Snapshot] [View Timeline] [Deploy]
```

**Deployment Panel:**
```
Deploy to: [Prod ▼]

Migration: v1.0.0 → v1.1.0

Changes (5):
✓ ADD COLUMN orders.email
✓ ADD COLUMN orders.phone
⚠️ RENAME COLUMN orders.status → order_status (BREAKING)
✓ ADD INDEX on customer_id
✓ SET COMMENT on orders

[Review SQL] [Check Drift] [Deploy] [Cancel]
```

## Implementation Phases

### Phase 1: Snapshots (Foundation)
- [ ] Add Snapshot data model
- [ ] "Create Snapshot" command
- [ ] Snapshot viewer UI
- [ ] Version auto-increment
- [ ] Snapshot integrity (hashing)

### Phase 2: Migration Generation
- [ ] Diff algorithm (snapshot → snapshot)
- [ ] Op → SQL translation
- [ ] Breaking change detection
- [ ] SQL preview UI
- [ ] Migration testing (dry-run)

### Phase 3: Deployment
- [ ] Databricks connector
- [ ] SQL execution engine
- [ ] Deployment tracking
- [ ] Rollback support
- [ ] Error handling

### Phase 4: Drift Detection
- [ ] Read UC properties
- [ ] Version comparison
- [ ] Drift reporting
- [ ] Import from UC
- [ ] Conflict resolution

### Phase 5: Advanced
- [ ] Multi-environment sync
- [ ] Approval workflows
- [ ] Rollback automation
- [ ] Migration history visualization
- [ ] Performance impact analysis

## Benefits of This Approach

✅ **Best of both worlds**
- State-based: Easy to reason about desired end state
- Change-based: Precise control over migrations

✅ **Safe deployments**
- Preview SQL before execution
- Breaking change warnings
- Drift detection

✅ **Audit trail**
- Complete operation history
- Deployment tracking per environment
- Who deployed what, when

✅ **Flexible workflows**
- Quick iterations in dev
- Controlled promotions to prod
- Support for hotfixes

✅ **Databricks-native**
- Uses Unity Catalog features
- Leverages table properties
- No external infrastructure needed

## Next Steps

1. **Review & refine** this design
2. **Prioritize features** (MVP vs nice-to-have)
3. **Design UI mockups** for snapshot/deployment panels
4. **Implement Phase 1** (snapshots foundation)
5. **Test with real UC workspace**

---

**Questions to consider:**

1. Should snapshots be compressed (they can get large)?
2. How to handle schema evolution (new project file versions)?
3. Should we support branching (like git)?
4. How to handle manual fixes in prod (emergency patches)?
5. Integration with CI/CD pipelines?

