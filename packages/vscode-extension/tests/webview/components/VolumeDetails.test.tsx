/**
 * Unit tests for VolumeDetails - Volume properties and grants
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { VolumeDetails } from '../../../src/webview/components/VolumeDetails';

const mockUpdateVolume = jest.fn();
const mockAddGrant = jest.fn();
const mockRevokeGrant = jest.fn();

const defaultVolumeInfo = {
  catalog: { id: 'cat_1', name: 'my_catalog' },
  schema: { id: 'sch_1', name: 'my_schema' },
  volume: {
    id: 'vol_1',
    name: 'my_volume',
    volumeType: 'managed',
    comment: 'A test volume',
    grants: [{ principal: 'data_engineers', privileges: ['READ VOLUME'] }],
  },
};

const mockFindVolume = jest.fn(() => defaultVolumeInfo);

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

  test('renders volume header with full name', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('my_catalog.my_schema.my_volume')).toBeInTheDocument();
    expect(screen.getByText('VOLUME')).toBeInTheDocument();
  });

  test('renders Properties section with Type and Comment', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('Properties')).toBeInTheDocument();
    expect(screen.getByText('Managed')).toBeInTheDocument();
    expect(screen.getByText('A test volume')).toBeInTheDocument();
  });

  test('renders Grants section with principal and privileges', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText(/Grants \(1\)/)).toBeInTheDocument();
    expect(screen.getByText('data_engineers')).toBeInTheDocument();
    expect(screen.getByText('READ VOLUME')).toBeInTheDocument();
  });

  test('shows Comment recommended when volume has no comment', () => {
    mockFindVolume.mockReturnValueOnce({
      catalog: { id: 'cat_1', name: 'c' },
      schema: { id: 'sch_1', name: 's' },
      volume: { id: 'vol_1', name: 'v', volumeType: 'managed', grants: [] },
    });
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText(/Comment recommended/)).toBeInTheDocument();
  });

  test('opens comment modal when Edit comment is clicked', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    const editButtons = screen.getAllByTitle('Edit comment');
    expect(editButtons.length).toBeGreaterThan(0);
    fireEvent.click(editButtons[0]);
    expect(screen.getByText('Set Volume Comment')).toBeInTheDocument();
    expect(screen.getByDisplayValue('A test volume')).toBeInTheDocument();
  });

  test('shows empty state when volume not found', () => {
    mockFindVolume.mockReturnValueOnce(null);
    render(<VolumeDetails volumeId="vol_999" />);
    expect(screen.getByText('Volume not found')).toBeInTheDocument();
  });

  test('renders Location row for external volume', () => {
    mockFindVolume.mockReturnValueOnce({
      catalog: { id: 'cat_1', name: 'c' },
      schema: { id: 'sch_1', name: 's' },
      volume: {
        id: 'vol_1',
        name: 'ext_vol',
        volumeType: 'external',
        location: 's3://bucket/path',
        grants: [],
      },
    });
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByText('Location')).toBeInTheDocument();
    expect(screen.getByText('s3://bucket/path')).toBeInTheDocument();
  });

  test('has Add Grant button', () => {
    render(<VolumeDetails volumeId="vol_1" />);
    expect(screen.getByRole('button', { name: /Add Grant/i })).toBeInTheDocument();
  });
});
