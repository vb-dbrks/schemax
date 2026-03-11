/**
 * Unit tests for VolumeDetails - Volume properties, location, and grants
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { VolumeDetails } from '../../../src/webview/components/VolumeDetails';

jest.mock('../../../src/webview/components/RichComment', () => ({
  RichComment: ({ text }: { text: string }) => React.createElement('span', null, text),
}));

const mockFindVolume = jest.fn();
const mockUpdateVolume = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

const defaultVolumeInfo = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: { id: 'sch_1', name: 'my_schema' },
  volume: {
    id: 'vol_1',
    name: 'my_volume',
    volumeType: 'external',
    comment: 'Raw data volume',
    location: 's3://bucket/path',
    grants: [{ principal: 'data_engineers', privileges: ['READ VOLUME', 'WRITE VOLUME'] }],
  },
};

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => ({
    findVolume: mockFindVolume,
    updateVolume: mockUpdateVolume,
    addGrant: mockAddGrant,
    revokeGrant: mockRevokeGrant,
  }),
}));

describe('VolumeDetails Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFindVolume.mockReturnValue(defaultVolumeInfo);
  });

  test('renders volume header with full qualified name', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('my_catalog.my_schema.my_volume')).toBeInTheDocument();
  });

  test('renders VOLUME badge', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('VOLUME')).toBeInTheDocument();
  });

  test('shows empty state when volume not found', () => {
    mockFindVolume.mockReturnValueOnce(null);
    render(<VolumeDetails volumeId="vol_999" />);
    expect(screen.getByText('Volume not found')).toBeInTheDocument();
  });

  test('displays volume type as External for external volumes', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('Type')).toBeInTheDocument();
    expect(screen.getByText('External')).toBeInTheDocument();
  });

  test('displays volume type as Managed for managed volumes', () => {
    mockFindVolume.mockReturnValueOnce({
      ...defaultVolumeInfo,
      volume: { ...defaultVolumeInfo.volume, volumeType: 'managed', location: undefined },
    });
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('Managed')).toBeInTheDocument();
  });

  test('displays location for external volumes', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('Location')).toBeInTheDocument();
    expect(screen.getByText('s3://bucket/path')).toBeInTheDocument();
  });

  test('does not display location row for managed volumes', () => {
    mockFindVolume.mockReturnValueOnce({
      ...defaultVolumeInfo,
      volume: { ...defaultVolumeInfo.volume, volumeType: 'managed' },
    });
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.queryByText('Location')).not.toBeInTheDocument();
  });

  test('displays comment when present', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('Comment')).toBeInTheDocument();
    expect(screen.getByText('Raw data volume')).toBeInTheDocument();
  });

  test('shows Comment recommended when comment is absent', () => {
    mockFindVolume.mockReturnValueOnce({
      ...defaultVolumeInfo,
      volume: { ...defaultVolumeInfo.volume, comment: undefined },
    });
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText(/Comment recommended/)).toBeInTheDocument();
  });

  test('renders grants section with principal and privileges', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
    expect(screen.getByText('data_engineers')).toBeInTheDocument();
    expect(screen.getByText('READ VOLUME, WRITE VOLUME')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });

  test('opens comment modal when Edit comment is clicked', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    const commentEdit = screen.getByTitle('Edit comment');
    fireEvent.click(commentEdit);
    expect(screen.getByText('Set Volume Comment')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Raw data volume')).toBeInTheDocument();
  });

  test('opens location modal when Edit location is clicked', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    const locationEdit = screen.getByTitle('Edit location');
    fireEvent.click(locationEdit);
    expect(screen.getByText('Set Volume Location')).toBeInTheDocument();
    expect(screen.getByDisplayValue('s3://bucket/path')).toBeInTheDocument();
  });

  test('shows empty grants message when no grants defined', () => {
    mockFindVolume.mockReturnValueOnce({
      ...defaultVolumeInfo,
      volume: { ...defaultVolumeInfo.volume, grants: [] },
    });
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText(/No grants defined/)).toBeInTheDocument();
  });

  test('renders managed volume without location row', () => {
    const managedVolumeInfo = {
      catalog: { id: 'cat_1', name: 'my_catalog' },
      schema: { id: 'sch_1', name: 'my_schema' },
      volume: {
        id: 'vol_2',
        name: 'managed_volume',
        volumeType: 'managed',
        comment: 'Managed storage',
        grants: [],
      },
    };
    mockFindVolume.mockReturnValueOnce(managedVolumeInfo);
    render(<VolumeDetails volumeId="vol_2" />);
    expect(screen.getByText('Managed')).toBeInTheDocument();
    expect(screen.queryByText('Location')).not.toBeInTheDocument();
  });

  test('renders external volume with location row', () => {
    const externalVolumeInfo = {
      catalog: { id: 'cat_1', name: 'my_catalog' },
      schema: { id: 'sch_1', name: 'my_schema' },
      volume: {
        id: 'vol_3',
        name: 'external_volume',
        volumeType: 'external',
        location: 'abfss://container@account.dfs.core.windows.net/path',
        comment: 'Azure external volume',
        grants: [],
      },
    };
    mockFindVolume.mockReturnValueOnce(externalVolumeInfo);
    render(<VolumeDetails volumeId="vol_3" />);
    expect(screen.getByText('External')).toBeInTheDocument();
    expect(screen.getByText('Location')).toBeInTheDocument();
    expect(screen.getByText('abfss://container@account.dfs.core.windows.net/path')).toBeInTheDocument();
  });
});
