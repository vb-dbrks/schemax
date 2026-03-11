/**
 * Unit tests for UnityTargetSettings - Environment-level target configuration
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import { UnityTargetSettings } from '../../../src/webview/components/settings/UnityTargetSettings';

jest.mock('@vscode/webview-ui-toolkit/react', () => ({
  VSCodeButton: (props: any) => <button {...props}>{props.children}</button>,
  VSCodeTextField: (props: any) => <input {...props} />,
}));

const defaultTargetConfig = {
  type: 'unity',
  version: '1.0.0',
  environments: {
    dev: {
      topLevelName: 'dev_catalog',
      catalogMappings: { bronze: 'dev_bronze' },
      allowDrift: true,
      requireSnapshot: false,
      autoCreateTopLevel: true,
      autoCreateSchemaxSchema: true,
    },
    prod: {
      topLevelName: 'prod_catalog',
      catalogMappings: {},
      allowDrift: false,
      requireSnapshot: true,
      autoCreateTopLevel: false,
      autoCreateSchemaxSchema: true,
    },
  },
};

const mockOnTargetConfigChange = jest.fn();
const mockOnMappingErrorChange = jest.fn();

function renderComponent(overrides: Record<string, any> = {}) {
  return render(
    <UnityTargetSettings
      targetConfig={overrides.targetConfig ?? defaultTargetConfig}
      activeEnv={overrides.activeEnv ?? 'dev'}
      logicalCatalogs={overrides.logicalCatalogs ?? []}
      onTargetConfigChange={overrides.onTargetConfigChange ?? mockOnTargetConfigChange}
      onMappingErrorChange={overrides.onMappingErrorChange ?? mockOnMappingErrorChange}
    />
  );
}

describe('UnityTargetSettings Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders Environment Configuration heading', () => {
    renderComponent();
    expect(screen.getByText('Environment Configuration')).toBeDefined();
  });

  test('renders environment names (dev, prod)', () => {
    renderComponent();
    expect(screen.getByText('dev')).toBeDefined();
    expect(screen.getByText('prod')).toBeDefined();
  });

  test('shows Active indicator for active env', () => {
    renderComponent({ activeEnv: 'dev' });
    expect(screen.getByText('● Active')).toBeDefined();
    // The active indicator should appear next to "dev", not "prod"
    const activeIndicator = screen.getByText('● Active');
    const parent = activeIndicator.parentElement!;
    expect(parent.textContent).toContain('dev');
  });

  test('renders Logical Isolation section header for expanded env', () => {
    renderComponent();
    // "dev" is expanded by default (activeEnv)
    expect(screen.getByText('Logical Isolation')).toBeDefined();
  });

  test('renders tracking catalog name for expanded env', () => {
    renderComponent();
    expect(screen.getByText('Tracking Catalog:')).toBeDefined();
    expect(screen.getByText('dev_catalog')).toBeDefined();
  });

  test('renders Settings section with Allow drift and Require snapshot', () => {
    renderComponent();
    expect(screen.getByText('Settings')).toBeDefined();
    expect(screen.getByText('Allow drift')).toBeDefined();
    expect(screen.getByText('Require snapshot')).toBeDefined();
    // dev has allowDrift=true, requireSnapshot=false
    const allowDriftRow = screen.getByText('Allow drift').closest('.info-row')!;
    expect(allowDriftRow.textContent).toContain('Yes');
    const requireSnapshotRow = screen.getByText('Require snapshot').closest('.info-row')!;
    expect(requireSnapshotRow.textContent).toContain('No');
  });

  test('renders Deployment scope section', () => {
    renderComponent();
    expect(screen.getByText('Deployment scope')).toBeDefined();
    // Check some managed category checkboxes are present
    expect(screen.getByText('Catalog structure')).toBeDefined();
    expect(screen.getByText('Governance')).toBeDefined();
  });

  test('renders Existing objects section', () => {
    renderComponent();
    expect(screen.getByText('Existing objects')).toBeDefined();
  });

  test('renders catalog mappings textarea content', () => {
    renderComponent();
    // dev has catalogMappings: { bronze: 'dev_bronze' } -> formatted as "bronze=dev_bronze"
    const textareas = document.querySelectorAll('textarea');
    const mappingTextarea = Array.from(textareas).find(
      (ta) => ta.value === 'bronze=dev_bronze'
    );
    expect(mappingTextarea).toBeDefined();
  });

  test('toggles environment section on click', () => {
    renderComponent();
    // "prod" is collapsed by default; "dev" is expanded
    // Verify prod content is not visible
    expect(screen.queryByText('prod_catalog')).toBeNull();

    // Click "prod" header to expand it
    const prodHeader = screen.getByText('prod').closest('.env-header')!;
    fireEvent.click(prodHeader);

    // Now prod content should be visible
    expect(screen.getByText('prod_catalog')).toBeDefined();
  });
});
