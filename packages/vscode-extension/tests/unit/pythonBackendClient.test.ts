import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, jest, test, beforeEach } from '@jest/globals';

import { PythonBackendClient } from '../../src/backend/pythonBackendClient';

const CONTRACT_FIXTURES_DIR = path.resolve(
  __dirname,
  '../../../../contracts/cli-envelopes'
);

function readFixture(name: string): unknown {
  const fixturePath = path.join(CONTRACT_FIXTURES_DIR, name);
  return JSON.parse(fs.readFileSync(fixturePath, 'utf8')) as unknown;
}

// Mock the @vscode/python-extension module
const mockResolveEnvironment = jest.fn<(...args: unknown[]) => Promise<unknown>>();
const mockGetActiveEnvironmentPath = jest.fn<(...args: unknown[]) => unknown>();

jest.mock('@vscode/python-extension', () => ({
  PythonExtension: {
    api: jest.fn(() =>
      Promise.resolve({
        environments: {
          getActiveEnvironmentPath: mockGetActiveEnvironmentPath,
          resolveEnvironment: mockResolveEnvironment,
        },
      })
    ),
  },
}));

describe('PythonBackendClient.run', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Default: Python extension returns no interpreter
    mockGetActiveEnvironmentPath.mockReturnValue({ id: '', path: '' });
    mockResolveEnvironment.mockResolvedValue(undefined);
  });

  test('uses Python extension API interpreter as first candidate', async () => {
    mockGetActiveEnvironmentPath.mockReturnValue({
      id: 'conda-myenv',
      path: '/opt/micromamba/envs/myenv',
    });
    mockResolveEnvironment.mockResolvedValue({
      executable: {
        uri: { fsPath: '/opt/micromamba/envs/myenv/bin/python' },
      },
    });

    const client = new PythonBackendClient();
    const runSingleMock: jest.Mock = jest.fn();
    runSingleMock.mockImplementationOnce(
      (_cmd: unknown, _args: unknown, _cwd: unknown, rendered: unknown, _useShell: unknown) =>
        Promise.resolve({
          success: true,
          command: rendered,
          stdout: '{}',
          stderr: '',
          exitCode: 0,
        })
    );
    (client as unknown as { runSingle: (...args: unknown[]) => unknown }).runSingle = runSingleMock;

    const result = await client.run(['validate', '--json'], '/tmp');

    expect(result.success).toBe(true);
    expect(result.command).toContain('/opt/micromamba/envs/myenv/bin/python');
    expect(result.command).toContain('-m schemax.cli');
    expect(runSingleMock).toHaveBeenCalledTimes(1);
    // Absolute paths use shell: false
    expect(runSingleMock.mock.calls[0][4]).toBe(false);
  });

  test('falls back to PATH candidates when Python extension has no resolved env', async () => {
    const client = new PythonBackendClient();
    const runSingleMock: jest.Mock = jest.fn();
    // schemax (first PATH candidate) succeeds
    runSingleMock.mockImplementationOnce(
      (_cmd: unknown, _args: unknown, _cwd: unknown, rendered: unknown) =>
        Promise.resolve({
          success: true,
          command: rendered,
          stdout: '{}',
          stderr: '',
          exitCode: 0,
        })
    );
    (client as unknown as { runSingle: (...args: unknown[]) => unknown }).runSingle = runSingleMock;

    const result = await client.run(['validate', '--json'], '/tmp');

    expect(result.success).toBe(true);
    expect(result.command).toContain('schemax');
    expect(runSingleMock).toHaveBeenCalledTimes(1);
    // PATH candidates use shell: true
    expect(runSingleMock.mock.calls[0][4]).toBe(true);
  });

  test('falls back to python3 when schemax candidate fails', async () => {
    const client = new PythonBackendClient();
    const runSingleMock: jest.Mock = jest.fn();
    runSingleMock.mockImplementationOnce(() =>
      Promise.resolve({
        success: false,
        command: 'schemax validate --json',
        stdout: '',
        stderr: 'not found',
        exitCode: 127,
      })
    );
    runSingleMock.mockImplementationOnce(() =>
      Promise.resolve({
        success: true,
        command: 'python3 -m schemax.cli validate --json',
        stdout: '{}',
        stderr: '',
        exitCode: 0,
      })
    );
    (client as unknown as { runSingle: (...args: unknown[]) => unknown }).runSingle = runSingleMock;

    const result = await client.run(['validate', '--json'], '/tmp');

    expect(result.success).toBe(true);
    expect(result.command).toContain('python3 -m schemax.cli');
    expect(runSingleMock).toHaveBeenCalledTimes(2);
  });

  test('still tries PATH candidates after Python extension interpreter fails', async () => {
    mockGetActiveEnvironmentPath.mockReturnValue({
      id: 'venv',
      path: '/home/user/.venv',
    });
    mockResolveEnvironment.mockResolvedValue({
      executable: { uri: { fsPath: '/home/user/.venv/bin/python' } },
    });

    const client = new PythonBackendClient();
    const runSingleMock: jest.Mock = jest.fn();
    // Python extension interpreter fails (schemax not installed in that env)
    runSingleMock.mockImplementationOnce(
      (_cmd: unknown, _args: unknown, _cwd: unknown, rendered: unknown) =>
        Promise.resolve({
          success: false,
          command: rendered,
          stdout: '',
          stderr: 'No module named schemax',
          exitCode: 1,
        })
    );
    // schemax on PATH succeeds
    runSingleMock.mockImplementationOnce(
      (_cmd: unknown, _args: unknown, _cwd: unknown, rendered: unknown) =>
        Promise.resolve({
          success: true,
          command: rendered,
          stdout: '{}',
          stderr: '',
          exitCode: 0,
        })
    );
    (client as unknown as { runSingle: (...args: unknown[]) => unknown }).runSingle = runSingleMock;

    const result = await client.run(['validate', '--json'], '/tmp');

    expect(result.success).toBe(true);
    expect(result.command).toContain('schemax');
    expect(runSingleMock).toHaveBeenCalledTimes(2);
  });

  test('returns cancelled result immediately without trying later candidates', async () => {
    const client = new PythonBackendClient();
    const runSingleMock: jest.Mock = jest.fn();
    runSingleMock.mockImplementationOnce(() =>
      Promise.resolve({
        success: false,
        command: 'schemax validate --json',
        stdout: '',
        stderr: 'Command cancelled',
        exitCode: null,
        cancelled: true,
      })
    );
    (client as unknown as { runSingle: (...args: unknown[]) => unknown }).runSingle = runSingleMock;

    const result = await client.run(['validate', '--json'], '/tmp');

    expect(result.cancelled).toBe(true);
    expect(runSingleMock).toHaveBeenCalledTimes(1);
  });
});

describe('PythonBackendClient.runJson', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockGetActiveEnvironmentPath.mockReturnValue({ id: '', path: '' });
    mockResolveEnvironment.mockResolvedValue(undefined);
  });

  test('returns native command envelope when stdout already contains one', async () => {
    const client = new PythonBackendClient();
    const fixture = readFixture('workspace_state.success.json');
    const mockedRun = jest
      .spyOn(client, 'run')
      .mockResolvedValueOnce({
        success: true,
        command: 'schemax validate --json',
        stdout: JSON.stringify(fixture),
        stderr: '',
        exitCode: 0,
      });

    const result = await client.runJson<{ valid: boolean }>(
      'workspace-state',
      ['workspace-state'],
      '/tmp'
    );

    expect(result.status).toBe('success');
    expect(result.command).toBe('workspace-state');
    expect(mockedRun).toHaveBeenCalled();
  });

  test.each([
    ['sql.success.json', 'sql'],
    ['workspace_state.success.json', 'workspace-state'],
    ['snapshot_validate.success.json', 'snapshot.validate'],
    ['import.success.json', 'import'],
    ['runtime_info.success.json', 'runtime-info'],
  ])('accepts shared envelope fixture %s', async (fixtureName: string, commandName: string) => {
    const client = new PythonBackendClient();
    const fixture = readFixture(fixtureName);
    jest.spyOn(client, 'run').mockResolvedValueOnce({
      success: true,
      command: `schemax ${commandName} --json`,
      stdout: JSON.stringify(fixture),
      stderr: '',
      exitCode: 0,
    });

    const result = await client.runJson(commandName, [commandName], '/tmp');
    expect(result.status).toBe('success');
    expect(result.command).toBe(commandName);
  });

  test('converts malformed native envelope into INVALID_ENVELOPE error', async () => {
    const client = new PythonBackendClient();
    const fixture = readFixture('invalid.missing-meta.json');
    jest.spyOn(client, 'run').mockResolvedValueOnce({
      success: true,
      command: 'schemax validate --json',
      stdout: JSON.stringify(fixture),
      stderr: '',
      exitCode: 0,
    });

    const result = await client.runJson('validate', ['validate'], '/tmp');

    expect(result.status).toBe('error');
    expect(result.errors[0]?.code).toBe('INVALID_ENVELOPE');
  });

  test('preserves native error envelope from CLI output', async () => {
    const client = new PythonBackendClient();
    const fixture = readFixture('rollback.invalid-args.error.json');
    jest.spyOn(client, 'run').mockResolvedValueOnce({
      success: true,
      command: 'schemax rollback --json',
      stdout: JSON.stringify(fixture),
      stderr: '',
      exitCode: 1,
    });

    const result = await client.runJson('rollback', ['rollback'], '/tmp');

    expect(result.status).toBe('error');
    expect(result.errors[0]?.code).toBe('ROLLBACK_INVALID_ARGS');
  });

  test('wraps plain JSON object into schema envelope on success', async () => {
    const client = new PythonBackendClient();
    jest.spyOn(client, 'run').mockResolvedValueOnce({
      success: true,
      command: 'schemax snapshot validate --json',
      stdout: '{"stale":[],"count":0}',
      stderr: '',
      exitCode: 0,
    });

    const result = await client.runJson<{ stale: unknown[]; count: number }>(
      'snapshot.validate',
      ['snapshot', 'validate'],
      '/tmp'
    );

    expect(result.schemaVersion).toBe('1');
    expect(result.status).toBe('success');
    expect(result.data).toEqual({ stale: [], count: 0 });
    expect(result.errors).toEqual([]);
  });

  test('normalizes command failure into error envelope', async () => {
    const client = new PythonBackendClient();
    jest.spyOn(client, 'run').mockResolvedValueOnce({
      success: false,
      command: 'schemax workspace-state --json',
      stdout: '',
      stderr: 'workspace missing',
      exitCode: 1,
    });

    const result = await client.runJson('workspace-state', ['workspace-state'], '/tmp');

    expect(result.status).toBe('error');
    expect(result.data).toBeNull();
    expect(result.errors[0]?.code).toBe('PYTHON_COMMAND_FAILED');
    expect(result.errors[0]?.message).toContain('workspace missing');
  });

  test('treats successful non-JSON output as an error envelope', async () => {
    const client = new PythonBackendClient();
    jest.spyOn(client, 'run').mockResolvedValueOnce({
      success: true,
      command: 'schemax validate --json',
      stdout: 'done',
      stderr: '',
      exitCode: 0,
    });

    const result = await client.runJson('validate', ['validate'], '/tmp');

    expect(result.status).toBe('error');
    expect(result.data).toBeNull();
    expect(result.errors[0]?.code).toBe('PYTHON_COMMAND_FAILED');
    expect(result.errors[0]?.message).toContain('did not return JSON');
  });

  test('parses final JSON line when stdout contains log noise', async () => {
    const client = new PythonBackendClient();
    jest.spyOn(client, 'run').mockResolvedValueOnce({
      success: true,
      command: 'schemax snapshot validate --json',
      stdout: [
        '[SchemaX] starting',
        '[SchemaX] checking',
        '{"stale":[{"version":"v0.2.0"}],"count":1}',
      ].join('\n'),
      stderr: '',
      exitCode: 0,
    });

    const result = await client.runJson<{ stale: Array<{ version: string }>; count: number }>(
      'snapshot.validate',
      ['snapshot', 'validate'],
      '/tmp'
    );

    expect(result.status).toBe('success');
    expect(result.data).not.toBeNull();
    const data = result.data as { stale: Array<{ version: string }>; count: number };
    expect(data.count).toBe(1);
    expect(data.stale[0]?.version).toBe('v0.2.0');
  });
});
