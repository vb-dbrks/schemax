import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, test, expect, jest } from '@jest/globals';
import { ImportAssetsPanel, ImportRunRequest } from '../../../src/webview/components/ImportAssetsPanel';
import { ProjectFile } from '../../../src/providers/unity/models';

const makeProject = (overrides?: Partial<ProjectFile>): ProjectFile => ({
  version: 4,
  name: 'demo',
  provider: {
    type: 'unity',
    version: '1.0.0',
    environments: {
      dev: {
        topLevelName: 'dev_demo',
      },
      prod: {
        topLevelName: 'prod_demo',
      },
    },
  },
  state: {
    catalogs: [
      {
        id: 'cat_1',
        name: 'schemax_demo',
        schemas: [],
      },
    ],
  },
  ops: [],
  snapshots: [],
  deployments: [],
  settings: {
    autoIncrementVersion: true,
    versionPrefix: 'v',
  },
  latestSnapshot: null,
  ...overrides,
});

describe('ImportAssetsPanel', () => {
  test('requires warehouse id before submit', () => {
    const onRun = jest.fn<(request: ImportRunRequest) => void>();

    render(
      <ImportAssetsPanel
        project={makeProject()}
        isRunning={false}
        result={null}
        onClose={jest.fn()}
        onCancel={jest.fn()}
        onRun={onRun}
      />
    );

    fireEvent.click(screen.getByText('Run'));

    expect(onRun).not.toHaveBeenCalled();
    expect(screen.getByText('Warehouse ID is required')).toBeTruthy();
  });

  test('submits import request with trimmed values and defaults', () => {
    const onRun = jest.fn<(request: ImportRunRequest) => void>();

    render(
      <ImportAssetsPanel
        project={makeProject()}
        isRunning={false}
        result={null}
        onClose={jest.fn()}
        onCancel={jest.fn()}
        onRun={onRun}
      />
    );

    const profileInput = screen.getByLabelText('Databricks Profile') as HTMLInputElement;
    const warehouseInput = screen.getByLabelText('Warehouse ID') as HTMLInputElement;
    const catalogInput = screen.getByLabelText('Catalog (optional)') as HTMLInputElement;
    const schemaInput = screen.getByLabelText('Schema (optional)') as HTMLInputElement;
    const tableInput = screen.getByLabelText('Table (optional)') as HTMLInputElement;
    const bindingsInput = screen.getByLabelText('Catalog mappings (optional)') as HTMLTextAreaElement;
    const importRunTypeRadio = screen.getByRole('radio', {
      name: 'Import and write operations',
    }) as HTMLInputElement;
    const adoptBaselineCheckbox = screen.getByRole('checkbox', {
      name: 'Adopt imported snapshot as deployment baseline',
    }) as HTMLInputElement;

    fireEvent.input(profileInput, { target: { value: '  TEAM  ' } });
    fireEvent.input(warehouseInput, { target: { value: '  abc123  ' } });
    fireEvent.input(catalogInput, { target: { value: '  main  ' } });
    fireEvent.input(schemaInput, { target: { value: '  analytics  ' } });
    fireEvent.input(tableInput, { target: { value: '  users  ' } });
    fireEvent.change(bindingsInput, {
      target: { value: 'schemax_demo=dev_schemax_demo\ncore=dev_core' },
    });

    fireEvent.click(importRunTypeRadio);
    fireEvent.click(adoptBaselineCheckbox);
    fireEvent.click(screen.getByText('Run'));

    expect(onRun).toHaveBeenCalledTimes(1);
    expect(onRun).toHaveBeenCalledWith({
      target: 'dev',
      profile: 'TEAM',
      warehouseId: 'abc123',
      catalog: 'main',
      schema: 'analytics',
      table: 'users',
      catalogMappings: {
        schemax_demo: 'dev_schemax_demo',
        core: 'dev_core',
      },
      dryRun: false,
      adoptBaseline: true,
    });
  });

  test('shows validation error for invalid catalog mappings format', () => {
    const onRun = jest.fn<(request: ImportRunRequest) => void>();

    render(
      <ImportAssetsPanel
        project={makeProject()}
        isRunning={false}
        result={null}
        onClose={jest.fn()}
        onCancel={jest.fn()}
        onRun={onRun}
      />
    );

    const warehouseInput = screen.getByLabelText('Warehouse ID') as HTMLInputElement;
    const bindingsInput = screen.getByLabelText('Catalog mappings (optional)') as HTMLTextAreaElement;

    fireEvent.input(warehouseInput, { target: { value: 'abc123' } });
    fireEvent.change(bindingsInput, { target: { value: 'bad-format' } });
    fireEvent.click(screen.getByText('Run'));

    expect(onRun).not.toHaveBeenCalled();
    expect(screen.getByText("Error: Invalid catalog mapping 'bad-format'. Expected logical=physical")).toBeTruthy();
  });

  test('prefills bindings from environment catalogMappings when available', () => {
    const project = makeProject({
      provider: {
        type: 'unity',
        version: '1.0.0',
        environments: {
          dev: {
            topLevelName: 'dev_demo',
            catalogMappings: {
              schemax_demo: 'dev_schemax_demo',
              core: 'dev_core',
            },
          },
        },
      },
    });

    render(
      <ImportAssetsPanel
        project={project}
        isRunning={false}
        result={null}
        onClose={jest.fn()}
        onCancel={jest.fn()}
        onRun={jest.fn()}
      />
    );

    const bindingsInput = screen.getByLabelText('Catalog mappings (optional)') as HTMLTextAreaElement;
    expect(bindingsInput.value).toBe('core=dev_core\nschemax_demo=dev_schemax_demo');
  });

  test('prefills single-catalog mapping from topLevelName when no explicit bindings exist', () => {
    render(
      <ImportAssetsPanel
        project={makeProject()}
        isRunning={false}
        result={null}
        onClose={jest.fn()}
        onCancel={jest.fn()}
        onRun={jest.fn()}
      />
    );

    const bindingsInput = screen.getByLabelText('Catalog mappings (optional)') as HTMLTextAreaElement;
    expect(bindingsInput.value).toBe('schemax_demo=dev_demo');
  });

  test('does not overwrite manual binding edits when environment changes', () => {
    const project = makeProject({
      provider: {
        type: 'unity',
        version: '1.0.0',
        environments: {
          dev: { topLevelName: 'dev_demo' },
          prod: { topLevelName: 'prod_demo' },
        },
      },
    });

    render(
      <ImportAssetsPanel
        project={project}
        isRunning={false}
        result={null}
        onClose={jest.fn()}
        onCancel={jest.fn()}
        onRun={jest.fn()}
      />
    );

    const bindingsInput = screen.getByLabelText('Catalog mappings (optional)') as HTMLTextAreaElement;
    const envSelect = screen.getByLabelText('Target Environment') as HTMLSelectElement;

    fireEvent.change(bindingsInput, { target: { value: 'custom=catalog_name' } });
    fireEvent.input(envSelect, { target: { value: 'prod' } });

    expect(bindingsInput.value).toBe('custom=catalog_name');
  });

  test('shows cancel action while import is running', () => {
    const onCancel = jest.fn();

    render(
      <ImportAssetsPanel
        project={makeProject()}
        isRunning={true}
        result={null}
        progress={{ phase: 'running', message: 'Running import...', percent: 45, level: 'info' }}
        onClose={jest.fn()}
        onCancel={onCancel}
        onRun={jest.fn()}
      />
    );

    fireEvent.click(screen.getByText('Cancel import'));
    expect(onCancel).toHaveBeenCalledTimes(1);
  });
});
