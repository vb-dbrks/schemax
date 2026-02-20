/**
 * Unit tests for FunctionDetails - Function properties, body, and grants
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { FunctionDetails } from '../../../src/webview/components/FunctionDetails';

const mockUpdateFunction = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

const defaultFuncInfo = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: { id: 'sch_1', name: 'my_schema' },
  func: {
    id: 'func_1',
    name: 'my_func',
    language: 'SQL',
    returnType: 'INT',
    body: 'SELECT 1',
    comment: 'A test function',
    grants: [{ principal: 'users', privileges: ['EXECUTE'] }],
  },
};
const mockFindFunction = jest.fn(() => defaultFuncInfo);

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    findFunction: mockFindFunction,
    updateFunction: mockUpdateFunction,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

describe('FunctionDetails Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFindFunction.mockReturnValue(defaultFuncInfo);
  });

  test('renders function header with full name', () => {
    render(<FunctionDetails functionId="func_1" />);
    expect(screen.getByText('my_catalog.my_schema.my_func')).toBeInTheDocument();
    expect(screen.getByText('FUNCTION')).toBeInTheDocument();
  });

  test('renders Properties with Language, Return type, Comment', () => {
    render(<FunctionDetails functionId="func_1" />);
    expect(screen.getByText('Properties')).toBeInTheDocument();
    expect(screen.getByText('SQL')).toBeInTheDocument();
    expect(screen.getByText('INT')).toBeInTheDocument();
    expect(screen.getByText('A test function')).toBeInTheDocument();
  });

  test('renders Body section', () => {
    render(<FunctionDetails functionId="func_1" />);
    expect(screen.getByText('Body')).toBeInTheDocument();
    expect(screen.getByText('SELECT 1')).toBeInTheDocument();
  });

  test('renders Grants section', () => {
    render(<FunctionDetails functionId="func_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
    expect(screen.getByText('users')).toBeInTheDocument();
    expect(screen.getByText('EXECUTE')).toBeInTheDocument();
  });

  test('shows empty state when function not found', () => {
    mockFindFunction.mockReturnValueOnce(null);
    render(<FunctionDetails functionId="func_999" />);
    expect(screen.getByText('Function not found')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<FunctionDetails functionId="func_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });
});
