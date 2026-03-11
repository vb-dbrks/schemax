/**
 * Unit tests for SnapshotPanel - Snapshot timeline, badges, and uncommitted changes
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { SnapshotPanel } from '../../../src/webview/components/SnapshotPanel';

const mockUseDesignerStore = jest.fn();
jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: (...args: any[]) => mockUseDesignerStore(...args),
}));

const defaultProject = {
  snapshots: [
    { id: 'snap_1', version: 'v1.0.0', name: 'Initial release', ts: '2025-01-01T00:00:00Z', comment: 'First snapshot', createdBy: 'admin', opsCount: 5, tags: ['release'] },
    { id: 'snap_2', version: 'v1.1.0', name: 'Feature update', ts: '2025-02-01T00:00:00Z', opsCount: 3 },
  ],
  deployments: [{ snapshotId: 'snap_1', environment: 'prod' }],
  ops: [{ id: 'op_1', op: 'add_table', ts: '2025-03-01' }],
  state: { catalogs: [] },
};

describe('SnapshotPanel Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseDesignerStore.mockReturnValue({ project: defaultProject });
  });

  test('renders null when no project', () => {
    mockUseDesignerStore.mockReturnValue({ project: null });
    const { container } = render(<SnapshotPanel />);
    expect(container.innerHTML).toBe('');
  });

  test('shows snapshot count in header', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText(/Snapshots \(2\)/)).toBeInTheDocument();
  });

  test('renders snapshot versions', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText('v1.0.0')).toBeInTheDocument();
    expect(screen.getByText('v1.1.0')).toBeInTheDocument();
  });

  test('shows "Latest" badge on most recent snapshot', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText('Latest')).toBeInTheDocument();
    // The latest badge should be next to v1.1.0 (last in array = most recent, displayed first after reverse)
    const latestBadge = screen.getByText('Latest');
    expect(latestBadge.className).toBe('latest-badge');
  });

  test('renders snapshot names', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText('Initial release')).toBeInTheDocument();
    expect(screen.getByText('Feature update')).toBeInTheDocument();
  });

  test('shows snapshot comment when present', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText('First snapshot')).toBeInTheDocument();
  });

  test('shows createdBy when present', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText(/admin/)).toBeInTheDocument();
  });

  test('shows ops count per snapshot', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText(/5 ops/)).toBeInTheDocument();
    expect(screen.getByText(/3 ops/)).toBeInTheDocument();
  });

  test('shows tags when present', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText('release')).toBeInTheDocument();
  });

  test('shows deployment info', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText(/Deployed to:.*prod/)).toBeInTheDocument();
  });

  test('shows uncommitted changes section when ops exist', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText('Working Changes')).toBeInTheDocument();
    expect(screen.getByText('1 uncommitted operations')).toBeInTheDocument();
    expect(screen.getByText('Create a snapshot to save these changes')).toBeInTheDocument();
  });

  test('shows uncommitted badge in header', () => {
    render(<SnapshotPanel />);
    expect(screen.getByText('1 uncommitted')).toBeInTheDocument();
  });

  test('does not show uncommitted section when no ops', () => {
    mockUseDesignerStore.mockReturnValue({
      project: { ...defaultProject, ops: [] },
    });
    render(<SnapshotPanel />);
    expect(screen.queryByText('Working Changes')).not.toBeInTheDocument();
    expect(screen.queryByText(/uncommitted/)).not.toBeInTheDocument();
  });

  test('empty state with no snapshots', () => {
    mockUseDesignerStore.mockReturnValue({
      project: { ...defaultProject, snapshots: [], ops: [] },
    });
    render(<SnapshotPanel />);
    expect(screen.getByText('No snapshots yet')).toBeInTheDocument();
    expect(screen.getByText(/Create Snapshot/)).toBeInTheDocument();
  });

  test('shows Snapshots (0) when no snapshots', () => {
    mockUseDesignerStore.mockReturnValue({
      project: { ...defaultProject, snapshots: [], ops: [] },
    });
    render(<SnapshotPanel />);
    expect(screen.getByText(/Snapshots \(0\)/)).toBeInTheDocument();
  });

  test('handles project with undefined snapshots gracefully', () => {
    mockUseDesignerStore.mockReturnValue({
      project: { state: { catalogs: [] } },
    });
    render(<SnapshotPanel />);
    expect(screen.getByText(/Snapshots \(0\)/)).toBeInTheDocument();
    expect(screen.getByText('No snapshots yet')).toBeInTheDocument();
  });
});
