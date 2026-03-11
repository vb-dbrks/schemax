/**
 * Unit tests for SchemaDetails - Schema properties, tags, and grants
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { SchemaDetails } from '../../../src/webview/components/SchemaDetails';

jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));
jest.mock('../../../src/webview/components/BulkOperationsPanel', () => ({
  BulkOperationsPanel: () => React.createElement('div', null, 'BulkPanel'),
}));

const mockUpdateSchema = jest.fn();
const mockRenameSchema = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

const defaultSchemaInfo = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: {
    id: 'sch_1',
    name: 'my_schema',
    comment: 'A test schema',
    tags: { team: 'analytics' },
    grants: [{ principal: 'data_engineers', privileges: ['USE SCHEMA', 'CREATE TABLE'] }],
  },
};

const mockFindSchema = jest.fn(() => defaultSchemaInfo);

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    project: { managedLocations: {} },
    findSchema: mockFindSchema,
    updateSchema: mockUpdateSchema,
    renameSchema: mockRenameSchema,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

describe('SchemaDetails Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFindSchema.mockReturnValue(defaultSchemaInfo);
  });

  test('renders schema header with full name', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('my_catalog.my_schema')).toBeInTheDocument();
  });

  test('renders SCHEMA badge', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('SCHEMA')).toBeInTheDocument();
  });

  test('shows empty state when schema not found', () => {
    mockFindSchema.mockReturnValueOnce(null);
    render(<SchemaDetails schemaId="sch_999" />);
    expect(screen.getByText('Schema not found')).toBeInTheDocument();
  });

  test('renders comment text', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('A test schema')).toBeInTheDocument();
  });

  test('shows Comment recommended when no comment', () => {
    mockFindSchema.mockReturnValueOnce({
      catalog: { id: 'cat_1', name: 'c' },
      schema: { id: 'sch_1', name: 's', tags: {}, grants: [], tables: [], views: [] },
    });
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText(/Comment recommended/)).toBeInTheDocument();
  });

  test('renders Schema Tags section header', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('Schema Tags (Unity Catalog)')).toBeInTheDocument();
  });

  test('renders Grants section with count', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('+ Add Grant')).toBeInTheDocument();
  });

  test('has Add Tag button', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('+ Add Tag')).toBeInTheDocument();
  });

  test('has Bulk operations button', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('Bulk operations')).toBeInTheDocument();
  });

  test('renders grant principal and privileges', () => {
    render(<SchemaDetails schemaId="sch_1" />);
    expect(screen.getByText('data_engineers')).toBeInTheDocument();
    expect(screen.getByText('USE SCHEMA, CREATE TABLE')).toBeInTheDocument();
  });
});
