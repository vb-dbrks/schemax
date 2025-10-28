/**
 * Tests for Sidebar component - Tree navigation
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { Sidebar } from '../../../src/webview/components/Sidebar';
import { UnityState } from '../../../src/providers/unity/models';

// Mock the Zustand store
const mockSetSelectedCatalog = jest.fn();
const mockSetSelectedSchema = jest.fn();
const mockSetSelectedTable = jest.fn();

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    state: {
      catalogs: [],
    } as UnityState,
    selectedCatalogId: null,
    selectedSchemaId: null,
    selectedTableId: null,
    setSelectedCatalog: mockSetSelectedCatalog,
    setSelectedSchema: mockSetSelectedSchema,
    setSelectedTable: mockSetSelectedTable,
    addCatalog: jest.fn(),
    addSchema: jest.fn(),
    addTable: jest.fn(),
  }),
}));

describe('Sidebar Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should render sidebar component', () => {
    const { container } = render(<Sidebar />);
    // Sidebar container should be present
    const sidebarElement = container.querySelector('.sidebar');
    expect(sidebarElement).not.toBeNull();
  });

  test('should render without crashing with empty state', () => {
    const { container } = render(<Sidebar />);
    expect(container).toBeTruthy();
  });
});

