import { describe, expect, test } from '@jest/globals';

import { validateRuntimeInfo } from '../../src/backend/runtimeCompatibility';

describe('validateRuntimeInfo', () => {
  test('accepts compatible runtime info', () => {
    const result = validateRuntimeInfo(
      {
        cliVersion: '0.2.1',
        envelopeSchemaVersion: '1',
        supportedCommands: ['runtime-info'],
        providerIds: ['unity'],
      },
      '0.2.0',
      '1'
    );
    expect(result.ok).toBe(true);
  });

  test('rejects unsupported envelope schema version', () => {
    const result = validateRuntimeInfo(
      {
        cliVersion: '0.2.1',
        envelopeSchemaVersion: '2',
        supportedCommands: ['runtime-info'],
        providerIds: ['unity'],
      },
      '0.2.0',
      '1'
    );
    expect(result.ok).toBe(false);
    expect(result.reason).toContain('Unsupported envelope schema');
  });

  test('rejects too-old cli version', () => {
    const result = validateRuntimeInfo(
      {
        cliVersion: '0.1.9',
        envelopeSchemaVersion: '1',
        supportedCommands: ['runtime-info'],
        providerIds: ['unity'],
      },
      '0.2.0',
      '1'
    );
    expect(result.ok).toBe(false);
    expect(result.reason).toContain('older than required');
  });
});
