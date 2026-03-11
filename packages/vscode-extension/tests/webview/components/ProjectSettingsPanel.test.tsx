/**
 * Unit tests for ProjectSettingsPanel - Project metadata, target tabs, locations, and save/cancel.
 *
 * Coverage:
 *  - Project Settings header
 *  - Project metadata (name, latest snapshot, snapshots count, targets count)
 *  - Target tabs and provider-specific settings
 *  - Managed and external locations sections
 *  - Save Changes / Cancel buttons
 *  - Close button
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { ProjectSettingsPanel } from '../../../src/webview/components/ProjectSettingsPanel';
import type { ProjectFile } from '../../../src/webview/models/unity';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

jest.mock('../../../src/webview/vscode-api', () => ({
  getVsCodeApi: () => ({ postMessage: jest.fn(), getState: jest.fn(), setState: jest.fn() }),
}));

jest.mock('../../../src/webview/components/settings/UnityTargetSettings', () => ({
  UnityTargetSettings: (props: any) => React.createElement('div', { 'data-testid': 'unity-settings' }, 'UnityTargetSettings'),
}));

// ---------------------------------------------------------------------------
// Default project fixture
// ---------------------------------------------------------------------------

const defaultProject: ProjectFile = {
  version: 5,
  name: 'test-project',
  targets: {
    default: {
      type: 'unity',
      version: '1.0.0',
      environments: {
        dev: { topLevelName: 'dev_catalog' },
        prod: { topLevelName: 'prod_catalog' },
      },
    },
  },
  defaultTarget: 'default',
  state: { catalogs: [] },
  ops: [],
  snapshots: [{ id: 's1', version: 'v1', name: 'initial', ts: '2026-01-01T00:00:00Z' }],
  deployments: [],
  settings: { autoIncrementVersion: true, versionPrefix: 'v' },
  latestSnapshot: 'v1',
  managedLocations: {
    warehouse: { description: 'Main warehouse', paths: { dev: 's3://dev/warehouse', prod: 's3://prod/warehouse' } },
  },
  externalLocations: {},
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('ProjectSettingsPanel Component', () => {
  const mockOnClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders Project Settings header', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('Project Settings')).toBeInTheDocument();
  });

  test('renders project name', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('test-project')).toBeInTheDocument();
  });

  test('renders latest snapshot', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('v1')).toBeInTheDocument();
  });

  test('renders snapshots count', () => {
    const { container } = render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('Snapshots')).toBeInTheDocument();
    // Find the info-row containing "Snapshots" and verify its value
    const snapshotsLabel = screen.getByText('Snapshots');
    const snapshotsRow = snapshotsLabel.closest('.info-row');
    expect(snapshotsRow).not.toBeNull();
    expect(snapshotsRow!.querySelector('.info-value')?.textContent).toBe('1');
  });

  test('renders targets count', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('Targets')).toBeInTheDocument();
  });

  test('renders target tab', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('default')).toBeInTheDocument();
  });

  test('renders UnityTargetSettings for unity type target', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByTestId('unity-settings')).toBeInTheDocument();
  });

  test('renders managed locations section', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('Physical Isolation (Managed Tables)')).toBeInTheDocument();
  });

  test('renders managed location name and paths', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('warehouse')).toBeInTheDocument();
    expect(screen.getByText('s3://dev/warehouse')).toBeInTheDocument();
    expect(screen.getByText('s3://prod/warehouse')).toBeInTheDocument();
  });

  test('has Add Managed Location button', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('+ Add Managed Location')).toBeInTheDocument();
  });

  test('has Add External Location button', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('+ Add External Location')).toBeInTheDocument();
  });

  test('has Save Changes button (disabled initially since not dirty)', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    const saveButton = screen.getByText('Save Changes');
    expect(saveButton).toBeInTheDocument();
    expect(saveButton.closest('vscode-button, button')).toHaveAttribute('disabled');
  });

  test('has Cancel button', () => {
    render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    expect(screen.getByText('Cancel')).toBeInTheDocument();
  });

  test('renders close button', () => {
    const { container } = render(<ProjectSettingsPanel project={defaultProject as any} onClose={mockOnClose} />);
    const closeBtn = container.querySelector('.close-btn');
    expect(closeBtn).not.toBeNull();
  });
});
