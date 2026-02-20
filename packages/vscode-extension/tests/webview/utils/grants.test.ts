/**
 * Unit tests for grant parsing utilities.
 * parsePrivileges splits on comma only so "READ VOLUME" stays one privilege.
 */

import { describe, test, expect } from '@jest/globals';
import { parsePrivileges } from '../../../src/webview/utils/grants';

describe('parsePrivileges', () => {
  test('splits on comma only', () => {
    expect(parsePrivileges('READ VOLUME, WRITE VOLUME')).toEqual(['READ VOLUME', 'WRITE VOLUME']);
  });

  test('keeps multi-word privileges as single items', () => {
    expect(parsePrivileges('READ VOLUME')).toEqual(['READ VOLUME']);
    expect(parsePrivileges('USE CATALOG')).toEqual(['USE CATALOG']);
    expect(parsePrivileges('CREATE SCHEMA')).toEqual(['CREATE SCHEMA']);
  });

  test('trims whitespace around each privilege', () => {
    expect(parsePrivileges('  SELECT  ,  MODIFY  ')).toEqual(['SELECT', 'MODIFY']);
  });

  test('returns empty array for empty or blank string', () => {
    expect(parsePrivileges('')).toEqual([]);
    expect(parsePrivileges('   ')).toEqual([]);
  });

  test('filters out empty segments after split', () => {
    expect(parsePrivileges('SELECT,,MODIFY')).toEqual(['SELECT', 'MODIFY']);
  });

  test('single privilege no comma', () => {
    expect(parsePrivileges('EXECUTE')).toEqual(['EXECUTE']);
  });

  test('table/view privileges comma-separated', () => {
    expect(parsePrivileges('SELECT, MODIFY')).toEqual(['SELECT', 'MODIFY']);
  });
});
