import { UnityState } from '../../src/providers/unity/models';

export const sampleEmptyState: UnityState = {
  catalogs: [],
};

export const sampleStateWithCatalog: UnityState = {
  catalogs: [
    {
      id: 'cat_001',
      name: '__implicit__',
      schemas: [],
    },
  ],
};

export const sampleStateWithSchema: UnityState = {
  catalogs: [
    {
      id: 'cat_001',
      name: '__implicit__',
      schemas: [
        {
          id: 'schema_001',
          name: 'test_schema',
          catalogId: 'cat_001',
          tables: [],
        },
      ],
    },
  ],
};

export const sampleStateWithTable: UnityState = {
  catalogs: [
    {
      id: 'cat_001',
      name: '__implicit__',
      schemas: [
        {
          id: 'schema_001',
          name: 'test_schema',
          catalogId: 'cat_001',
          tables: [
            {
              id: 'table_001',
              name: 'test_table',
              schemaId: 'schema_001',
              format: 'delta',
              columns: [
                {
                  id: 'col_001',
                  name: 'id',
                  type: 'BIGINT',
                  nullable: false,
                  comment: 'Primary key',
                },
                {
                  id: 'col_002',
                  name: 'name',
                  type: 'STRING',
                  nullable: true,
                },
              ],
              properties: {},
              constraints: [],
              grants: [],
            },
          ],
        },
      ],
    },
  ],
};

export const sampleStateWithConstraints: UnityState = {
  catalogs: [
    {
      id: 'cat_001',
      name: '__implicit__',
      schemas: [
        {
          id: 'schema_001',
          name: 'test_schema',
          catalogId: 'cat_001',
          tables: [
            {
              id: 'table_001',
              name: 'users',
              schemaId: 'schema_001',
              format: 'delta',
              columns: [
                {
                  id: 'col_001',
                  name: 'id',
                  type: 'BIGINT',
                  nullable: false,
                },
                {
                  id: 'col_002',
                  name: 'email',
                  type: 'STRING',
                  nullable: false,
                },
              ],
              properties: {},
              constraints: [
                {
                  id: 'constraint_001',
                  type: 'primary_key',
                  name: 'users_pk',
                  columns: ['col_001'],
                },
              ],
              grants: [],
            },
          ],
        },
      ],
    },
  ],
};

