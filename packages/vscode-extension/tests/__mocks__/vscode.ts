// Mock VS Code API for testing
import { jest } from '@jest/globals';

export const window = {
  showInformationMessage: jest.fn(),
  showErrorMessage: jest.fn(),
  showWarningMessage: jest.fn(),
  createOutputChannel: jest.fn(() => ({
    appendLine: jest.fn(),
    show: jest.fn(),
  })),
};

export const workspace = {
  workspaceFolders: [],
  getConfiguration: jest.fn(() => ({
    get: jest.fn(() => undefined),
  })),
  fs: {
    readFile: jest.fn(),
    writeFile: jest.fn(),
    stat: jest.fn(),
    createDirectory: jest.fn(),
  },
};

export const Uri = {
  file: (path: string) => ({ fsPath: path, path }),
  parse: (uri: string) => ({ fsPath: uri, path: uri }),
  joinPath: (base: { fsPath: string }, ...segments: string[]) => {
    const joined = [base.fsPath, ...segments].join('/');
    return { fsPath: joined, path: joined };
  },
};

export const commands = {
  registerCommand: jest.fn(),
};

export enum ViewColumn {
  One = 1,
  Two = 2,
  Three = 3,
}

export enum ProgressLocation {
  Notification = 15,
}

