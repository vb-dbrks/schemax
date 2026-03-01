import { describe, expect, test } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';

function readSource(relativePath: string): string {
  const filePath = path.resolve(__dirname, '../../src', relativePath);
  return fs.readFileSync(filePath, 'utf8');
}

function listSourceFiles(relativeDir: string): string[] {
  const root = path.resolve(__dirname, '../../src', relativeDir);
  const files: string[] = [];
  const stack: string[] = [root];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) continue;
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(absPath);
        continue;
      }
      if (entry.isFile() && (entry.name.endsWith('.ts') || entry.name.endsWith('.tsx'))) {
        files.push(path.relative(path.resolve(__dirname, '../../src'), absPath));
      }
    }
  }
  return files;
}

describe('Architecture fitness', () => {
  test('extension runtime entrypoint does not import provider runtime semantics', () => {
    const source = readSource('extension.ts');
    expect(source).not.toMatch(/from ['"]\.\/providers\//);
    expect(source).not.toMatch(/import ['"]\.\/providers['"]/);
  });

  test('storage-v4 does not import provider runtime semantics', () => {
    const source = readSource('storage-v4.ts');
    expect(source).not.toMatch(/from ['"]\.\/providers\//);
  });

  test('ProviderRegistry imports are confined to provider modules', () => {
    const allFiles = listSourceFiles('.');
    const violations = allFiles
      .filter((file) => !file.startsWith(`providers${path.sep}`) && !file.startsWith('providers/'))
      .filter((file) => readSource(file).includes('ProviderRegistry'));
    expect(violations).toEqual([]);
  });

  test('webview layer does not import provider runtime modules', () => {
    const webviewFiles = listSourceFiles('webview');
    const violations = webviewFiles.filter((file) => {
      const source = readSource(file);
      return /from ['"]\.\.\/\.\.\/providers\//.test(source) || /from ['"]\.\.\/providers\//.test(source);
    });
    expect(violations).toEqual([]);
  });

  test('extension runtime does not shell out workflow commands directly', () => {
    const source = readSource('extension.ts');
    expect(source).not.toMatch(/execAsync\(['"`]schemax\s+(?!--version)/);
    expect(source).not.toMatch(/execAsync\(['"`][^'"`]*\s-m\s+schemax\.cli/);
  });

  test('extension runtime execAsync usage stays constrained to install/version checks', () => {
    const source = readSource('extension.ts');
    const execMatches = source.match(/execAsync\(/g) ?? [];
    expect(execMatches.length).toBe(2);
    expect(source).toContain("execAsync('schemax --version'");
    expect(source).toContain('execAsync(`"${py}" -m pip install schemaxpy`');
  });

  test('child process usage is confined to approved runtime adapters', () => {
    const allFiles = listSourceFiles('.');
    const filesUsingChildProcess = allFiles.filter((file) => {
      const source = readSource(file);
      return source.includes("from 'child_process'") || source.includes('from "child_process"');
    });
    expect(filesUsingChildProcess.sort()).toEqual(
      ['backend/pythonBackendClient.ts', 'extension.ts'].sort()
    );
  });

  test('extension workflows route through PythonBackendClient.runJson contracts', () => {
    const extensionSource = readSource('extension.ts');
    const storageSource = readSource('storage-v4.ts');

    expect(extensionSource).toMatch(/runJson<[\s\S]*?>\(\s*'import'/);
    expect(extensionSource).toMatch(/runJson<[\s\S]*?>\(\s*'sql'/);
    expect(extensionSource).toMatch(/runJson<[\s\S]*?>\(\s*'runtime-info'/);
    expect(extensionSource).toMatch(/runJson<[\s\S]*?>\(\s*'snapshot\.validate'/);
    expect(storageSource).toMatch(/runJson<[\s\S]*?>\(\s*'workspace-state'/);
    expect(extensionSource).not.toMatch(/pythonBackend\.run\(/);
    expect(storageSource).not.toMatch(/pythonBackend\.run\(/);
  });

  test('runtime source tree has no direct PythonBackendClient.run workflow usage', () => {
    const allFiles = listSourceFiles('.');
    const violations = allFiles
      .filter((file) => !file.startsWith(`backend${path.sep}`) && !file.startsWith('backend/'))
      .filter((file) => /pythonBackend\.run\(/.test(readSource(file)));
    expect(violations).toEqual([]);
  });
});
