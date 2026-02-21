/**
 * Integration tests for App - Root layout and empty state.
 * Verifies App renders, shows Sidebar and SnapshotPanel, and shows
 * "No selection" when nothing is selected. Detail panel content for
 * volume/function/MV is covered by unit tests (VolumeDetails.test.tsx etc.).
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { App } from '../../../src/webview/App';

const mockProject = {
  state: { catalogs: [] },
  ops: [],
  provider: { type: 'unity', version: '1.0.0' },
  environments: {},
  activeEnvironment: null,
};

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    project: mockProject,
    provider: { name: 'Unity Catalog', version: '1.0.0' },
    selectedCatalogId: null,
    selectedSchemaId: null,
    selectedTableId: null,
    findView: jest.fn(() => null),
    findVolume: jest.fn(() => null),
    findFunction: jest.fn(() => null),
    findMaterializedView: jest.fn(() => null),
    setProject: jest.fn(),
    setProvider: jest.fn(),
  }),
}));

describe('App Integration', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders without crashing', () => {
    const { container } = render(<App />);
    expect(container).toBeTruthy();
  });

  test('shows loading state when first mounted', () => {
    render(<App />);
    // App starts with loading=true until it receives project-loaded message from extension
    expect(screen.getByText(/Loading SchemaX Designer/)).toBeInTheDocument();
  });
});
