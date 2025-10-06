import { z } from 'zod';

// Operation types
export const OpType = z.enum([
  'add_catalog',
  'rename_catalog',
  'drop_catalog',
  'add_schema',
  'rename_schema',
  'drop_schema',
  'add_table',
  'rename_table',
  'drop_table',
  'add_column',
  'rename_column',
  'reorder_columns',
  'drop_column',
  'change_column_type',
  'set_nullable',
  'set_table_property',
  'unset_table_property',
  'set_table_comment',
  'set_column_comment',
]);
export type OpType = z.infer<typeof OpType>;

// Operation definition
export const Op = z.object({
  ts: z.string(), // ISO timestamp
  op: OpType,
  target: z.string(), // id of the object the op applies to
  payload: z.record(z.any()),
});
export type Op = z.infer<typeof Op>;

