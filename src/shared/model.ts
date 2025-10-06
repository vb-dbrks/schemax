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

// Project file definition
export const ProjectFile = z.object({
  version: z.literal(1),
  name: z.string(),
  environments: z.array(z.enum(['dev', 'test', 'prod'])),
  state: z.object({
    catalogs: z.array(Catalog),
  }),
  ops: z.array(z.any()), // Will be Op[] from ops.ts
  lastSnapshotHash: z.string().nullable(),
});
export type ProjectFile = z.infer<typeof ProjectFile>;

