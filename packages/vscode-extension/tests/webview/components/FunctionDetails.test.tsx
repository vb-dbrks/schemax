/**
 * Unit tests for FunctionDetails - Function properties, body, parameters, and grants
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { FunctionDetails } from '../../../src/webview/components/FunctionDetails';

jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));

const mockFindFunction = jest.fn();
const mockUpdateFunction = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

const defaultFnInfo = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: { id: 'sch_1', name: 'my_schema' },
  func: {
    id: 'fn_1',
    name: 'my_function',
    language: 'SQL',
    returnType: 'INT',
    body: 'RETURN x + y',
    comment: 'Adds two numbers',
    parameters: [
      { name: 'x', dataType: 'INT' },
      { name: 'y', dataType: 'INT' },
    ],
    grants: [{ principal: 'analysts', privileges: ['EXECUTE'] }],
  },
};

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
    mockFindFunction.mockReturnValue(defaultFnInfo);
  });

  test('renders function header with full qualified name', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('my_catalog.my_schema.my_function')).toBeInTheDocument();
  });

  test('renders FUNCTION badge', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('FUNCTION')).toBeInTheDocument();
  });

  test('shows empty state when function not found', () => {
    mockFindFunction.mockReturnValueOnce(null);
    render(<FunctionDetails functionId="fn_999" />);
    expect(screen.getByText('Function not found')).toBeInTheDocument();
  });

  test('displays language property', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('Language')).toBeInTheDocument();
    expect(screen.getByText('SQL')).toBeInTheDocument();
  });

  test('displays return type property', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('Return type')).toBeInTheDocument();
    expect(screen.getByText('INT')).toBeInTheDocument();
  });

  test('displays parameters', () => {
    render(<FunctionDetails functionId="fn_1" />);
    // Parameters should be visible in the component if rendered
    // The component renders parameters as part of the function info
  });

  test('displays function body', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('Body')).toBeInTheDocument();
    expect(screen.getByText('RETURN x + y')).toBeInTheDocument();
  });

  test('displays comment when present', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('Comment')).toBeInTheDocument();
    expect(screen.getByText('Adds two numbers')).toBeInTheDocument();
  });

  test('shows Comment recommended when comment is absent', () => {
    mockFindFunction.mockReturnValueOnce({
      ...defaultFnInfo,
      func: { ...defaultFnInfo.func, comment: undefined },
    });
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText(/Comment recommended/)).toBeInTheDocument();
  });

  test('renders grants section with principal and privileges', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
    expect(screen.getByText('analysts')).toBeInTheDocument();
    expect(screen.getByText('EXECUTE')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });

  test('opens body modal when Edit body is clicked', () => {
    render(<FunctionDetails functionId="fn_1" />);
    const bodyEdit = screen.getByTitle('Edit body');
    fireEvent.click(bodyEdit);
    expect(screen.getByText('Set Function Body')).toBeInTheDocument();
    expect(screen.getByDisplayValue('RETURN x + y')).toBeInTheDocument();
  });

  test('opens return type modal when Edit return type is clicked', () => {
    render(<FunctionDetails functionId="fn_1" />);
    const returnTypeEdit = screen.getByTitle('Edit return type');
    fireEvent.click(returnTypeEdit);
    expect(screen.getByText('Set Return Type')).toBeInTheDocument();
    expect(screen.getByDisplayValue('INT')).toBeInTheDocument();
  });

  test('opens comment modal when Edit comment is clicked', () => {
    render(<FunctionDetails functionId="fn_1" />);
    const commentEdit = screen.getByTitle('Edit comment');
    fireEvent.click(commentEdit);
    expect(screen.getByText('Set Function Comment')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Adds two numbers')).toBeInTheDocument();
  });

  test('shows empty grants message when no grants defined', () => {
    mockFindFunction.mockReturnValueOnce({
      ...defaultFnInfo,
      func: { ...defaultFnInfo.func, grants: [] },
    });
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText(/No grants defined/)).toBeInTheDocument();
  });

  test('defaults language to SQL when not specified', () => {
    mockFindFunction.mockReturnValueOnce({
      ...defaultFnInfo,
      func: { ...defaultFnInfo.func, language: undefined },
    });
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('SQL')).toBeInTheDocument();
  });

  test('defaults return type to STRING when not specified', () => {
    mockFindFunction.mockReturnValueOnce({
      ...defaultFnInfo,
      func: { ...defaultFnInfo.func, returnType: undefined },
    });
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('STRING')).toBeInTheDocument();
  });

  test('defaults body to NULL when not specified', () => {
    mockFindFunction.mockReturnValueOnce({
      ...defaultFnInfo,
      func: { ...defaultFnInfo.func, body: undefined },
    });
    render(<FunctionDetails functionId="fn_1" />);
    expect(screen.getByText('NULL')).toBeInTheDocument();
  });
});
