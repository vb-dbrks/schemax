/**
 * Unit tests for EnvironmentSummary - Environment catalog mapping overview
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { EnvironmentSummary } from '../../../src/webview/components/EnvironmentSummary';

jest.mock('../../../src/webview/vscode-api', () => ({
  getVsCodeApi: () => ({ postMessage: jest.fn(), getState: jest.fn(), setState: jest.fn() }),
}));

const mockProject = {
  name: 'test-project',
  defaultTarget: 'default',
  targets: {
    default: {
      type: 'unity',
      version: '1.0.0',
      environments: {
        dev: {
          topLevelName: 'dev_catalog',
          catalogMappings: { bronze: 'dev_bronze', silver: 'dev_silver' },
          allowDrift: true,
          requireSnapshot: false,
          autoCreateSchemaxSchema: true,
        },
        prod: {
          topLevelName: 'prod_catalog',
          catalogMappings: {},
          allowDrift: false,
          requireSnapshot: true,
          requireApproval: true,
          autoCreateSchemaxSchema: true,
        },
      },
    },
  },
};

const mockUseDesignerStore = jest.fn();

jest.mock('../../../src/webview/state/useDesignerStore', () => ({
  useDesignerStore: () => mockUseDesignerStore(),
}));

describe('EnvironmentSummary Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseDesignerStore.mockReturnValue({ project: mockProject });
  });

  test('renders environment summary section', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByRole('region', { name: 'Environment catalog mapping overview' })).toBeInTheDocument();
  });

  test('renders environment names', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText('dev')).toBeInTheDocument();
    expect(screen.getByText('prod')).toBeInTheDocument();
  });

  test('renders tracking catalog names', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText('dev_catalog')).toBeInTheDocument();
    expect(screen.getByText('prod_catalog')).toBeInTheDocument();
  });

  test('renders catalog mappings for dev', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText(/bronze→dev_bronze/)).toBeInTheDocument();
    expect(screen.getByText(/silver→dev_silver/)).toBeInTheDocument();
  });

  test('shows "No catalog mappings configured" for prod', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText('No catalog mappings configured')).toBeInTheDocument();
  });

  test('renders drift flags correctly', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText('Drift allowed')).toBeInTheDocument();
    expect(screen.getByText('Drift locked')).toBeInTheDocument();
  });

  test('renders snapshot required flag for prod', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText('Snapshot required')).toBeInTheDocument();
  });

  test('renders approval required flag for prod', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText('Approval required')).toBeInTheDocument();
  });

  test('has Docs button', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByRole('button', { name: /Docs/i })).toBeInTheDocument();
  });

  test('renders eyebrow and title text', () => {
    render(<EnvironmentSummary />);
    expect(screen.getByText('Environment mapping')).toBeInTheDocument();
    expect(screen.getByText('Logical → Physical catalogs')).toBeInTheDocument();
  });

  describe('when no environments exist', () => {
    test('renders null when project has no environments', () => {
      mockUseDesignerStore.mockReturnValue({
        project: {
          name: 'empty-project',
          defaultTarget: 'default',
          targets: {
            default: {
              type: 'unity',
              version: '1.0.0',
              environments: {},
            },
          },
        },
      });
      const { container } = render(<EnvironmentSummary />);
      expect(container.innerHTML).toBe('');
    });

    test('renders null when project is undefined', () => {
      mockUseDesignerStore.mockReturnValue({ project: undefined });
      const { container } = render(<EnvironmentSummary />);
      expect(container.innerHTML).toBe('');
    });
  });
});
