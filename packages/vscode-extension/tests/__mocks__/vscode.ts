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
  getConfiguration: jest.fn(),
};

export const Uri = {
  file: (path: string) => ({ fsPath: path, path }),
  parse: (uri: string) => ({ fsPath: uri, path: uri }),
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

