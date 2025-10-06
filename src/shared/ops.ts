import { z } from 'zod';

// Operation types
export const OpType = z.enum([
  // Catalog operations
  'add_catalog',
  'rename_catalog',
  'drop_catalog',
  
  // Schema operations
  'add_schema',
  'rename_schema',
  'drop_schema',
  
  // Table operations
  'add_table',
  'rename_table',
  'drop_table',
  'set_table_comment',
  'set_table_property',
  'unset_table_property',
  
  // Column operations
  'add_column',
  'rename_column',
  'reorder_columns',
  'drop_column',
  'change_column_type',
  'set_nullable',
  'set_column_comment',
  
  // Column tag operations (NEW)
  'set_column_tag',
  'unset_column_tag',
  
  // Constraint operations (NEW)
  'add_constraint',
  'drop_constraint',
  
  // Row filter operations (NEW)
  'add_row_filter',
  'update_row_filter',
  'remove_row_filter',
  
  // Column mask operations (NEW)
  'add_column_mask',
  'update_column_mask',
  'remove_column_mask',
]);
export type OpType = z.infer<typeof OpType>;

// Operation definition
export const Op = z.object({
  id: z.string().optional(), // unique identifier (backwards compatible)
  ts: z.string(), // ISO timestamp
  op: OpType,
  target: z.string(), // id of the object the op applies to
  payload: z.record(z.any()),
});
export type Op = z.infer<typeof Op>;

