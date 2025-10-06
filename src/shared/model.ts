import { z } from 'zod';

// Column definition
export const Column = z.object({
  id: z.string(),
  name: z.string(),
  type: z.string(),
  nullable: z.boolean(),
  comment: z.string().optional(),
});
export type Column = z.infer<typeof Column>;

// Constraint definition (MVP: check constraints)
export const Constraint = z.object({
  type: z.literal('check'),
  expr: z.string(),
});
export type Constraint = z.infer<typeof Constraint>;

// Grant definition
export const Grant = z.object({
  principal: z.string(),
  privileges: z.array(z.string()),
});
export type Grant = z.infer<typeof Grant>;

// Table definition
export const Table = z.object({
  id: z.string(),
  name: z.string(),
  format: z.enum(['delta', 'iceberg']),
  columnMapping: z.enum(['name', 'id']).optional(),
  columns: z.array(Column),
  properties: z.record(z.string()),
  constraints: z.array(Constraint),
  grants: z.array(Grant),
  comment: z.string().optional(),
});
export type Table = z.infer<typeof Table>;

// Schema definition
export const Schema = z.object({
  id: z.string(),
  name: z.string(),
  tables: z.array(Table),
});
export type Schema = z.infer<typeof Schema>;

// Catalog definition
export const Catalog = z.object({
  id: z.string(),
  name: z.string(),
  schemas: z.array(Schema),
});
export type Catalog = z.infer<typeof Catalog>;

// Snapshot metadata (stored in project.json)
export const SnapshotMetadata = z.object({
  id: z.string(),
  version: z.string(), // semantic version
  name: z.string(),
  ts: z.string(),
  createdBy: z.string().optional(),
  file: z.string(), // relative path to snapshot file (e.g., ".schemax/snapshots/v0.1.0.json")
  previousSnapshot: z.string().nullable(), // version of previous snapshot
  opsCount: z.number(), // number of ops included in this snapshot
  hash: z.string(),
  tags: z.array(z.string()).default([]),
  comment: z.string().optional(),
});
export type SnapshotMetadata = z.infer<typeof SnapshotMetadata>;

// Snapshot file content (stored in .schemax/snapshots/vX.Y.Z.json)
export const SnapshotFile = z.object({
  id: z.string(),
  version: z.string(),
  name: z.string(),
  ts: z.string(),
  createdBy: z.string().optional(),
  state: z.object({
    catalogs: z.array(Catalog),
  }),
  opsIncluded: z.array(z.string()), // op IDs that led to this state
  previousSnapshot: z.string().nullable(), // version
  hash: z.string(),
  tags: z.array(z.string()).default([]),
  comment: z.string().optional(),
});
export type SnapshotFile = z.infer<typeof SnapshotFile>;

// Changelog file (schemax.changelog.json)
export const ChangelogFile = z.object({
  version: z.literal(1),
  sinceSnapshot: z.string().nullable(), // version of snapshot this changelog is based on
  ops: z.array(z.any()), // Op[]
  lastModified: z.string(), // ISO timestamp
});
export type ChangelogFile = z.infer<typeof ChangelogFile>;

// Drift info
export const DriftInfo = z.object({
  objectType: z.enum(['catalog', 'schema', 'table', 'column']),
  objectName: z.string(),
  expectedVersion: z.string(),
  actualVersion: z.string().nullable(),
  issue: z.enum(['missing', 'modified', 'extra']),
});
export type DriftInfo = z.infer<typeof DriftInfo>;

// Deployment definition
export const Deployment = z.object({
  id: z.string(),
  environment: z.string(),
  ts: z.string(),
  deployedBy: z.string().optional(),
  snapshotId: z.string().nullable(),
  opsApplied: z.array(z.string()),
  schemaVersion: z.string(),
  sqlGenerated: z.string().optional(),
  status: z.enum(['success', 'failed', 'rolled_back']),
  error: z.string().optional(),
  driftDetected: z.boolean().default(false),
  driftDetails: z.array(DriftInfo).optional(),
});
export type Deployment = z.infer<typeof Deployment>;

// Project settings
export const ProjectSettings = z.object({
  autoIncrementVersion: z.boolean().default(true),
  versionPrefix: z.string().default('v'),
  requireSnapshotForProd: z.boolean().default(true),
  allowDrift: z.boolean().default(false),
  requireComments: z.boolean().default(false),
  warnOnBreakingChanges: z.boolean().default(true),
});
export type ProjectSettings = z.infer<typeof ProjectSettings>;

// Project file definition (v2 - simplified, references external files)
export const ProjectFile = z.object({
  version: z.literal(2), // Bumped to v2
  name: z.string(),
  environments: z.array(z.enum(['dev', 'test', 'prod'])),
  snapshots: z.array(SnapshotMetadata).default([]), // Metadata only, full snapshots in separate files
  deployments: z.array(Deployment).default([]),
  settings: ProjectSettings.default({}),
  latestSnapshot: z.string().nullable(), // version string of latest snapshot
});
export type ProjectFile = z.infer<typeof ProjectFile>;

