export interface CommandEnvelope<T = unknown> {
  schemaVersion: '1';
  command: string;
  status: 'success' | 'error';
  data: T;
  warnings: string[];
  errors: Array<{
    code: string;
    message: string;
    details?: Record<string, unknown>;
  }>;
  meta: {
    durationMs: number;
    executedCommand: string;
    exitCode: number | null;
  };
}

export interface PythonCommandResult {
  success: boolean;
  command: string;
  stdout: string;
  stderr: string;
  exitCode: number | null;
  cancelled?: boolean;
}
