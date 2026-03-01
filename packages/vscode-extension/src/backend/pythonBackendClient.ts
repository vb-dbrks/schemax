import { spawn } from 'child_process';
import type { CommandEnvelope, PythonCommandResult } from './contracts';

type StreamHandler = (chunk: string) => void;

interface RunOptions {
  onStdout?: StreamHandler;
  onStderr?: StreamHandler;
  signal?: AbortSignal;
}

const COMMAND_CANDIDATES = ['schemax', 'python3 -m schemax.cli', 'python -m schemax.cli'] as const;

function parseCommandCandidate(candidate: string, args: string[]): { cmd: string; fullArgs: string[] } {
  if (candidate.includes(' -m ')) {
    const [pythonCmd] = candidate.split(' -m ');
    return { cmd: pythonCmd, fullArgs: ['-m', 'schemax.cli', ...args] };
  }
  return { cmd: candidate, fullArgs: args };
}

function parseJsonLine(output: string): unknown {
  const lines = output
    .trim()
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    try {
      return JSON.parse(lines[index]);
    } catch {
      // Keep scanning older lines.
    }
  }
  return null;
}

function normalizeError(code: string, message: string): CommandEnvelope<null>['errors'][number] {
  return { code, message };
}

export class PythonBackendClient {
  async run(args: string[], cwd: string, options: RunOptions = {}): Promise<PythonCommandResult> {
    let lastFailure: PythonCommandResult = {
      success: false,
      command: '',
      stdout: '',
      stderr: 'No command candidate executed.',
      exitCode: null,
    };

    for (const candidate of COMMAND_CANDIDATES) {
      const { cmd, fullArgs } = parseCommandCandidate(candidate, args);
      const rendered = `${cmd} ${fullArgs.join(' ')}`;
      const result = await this.runSingle(cmd, fullArgs, cwd, rendered, options);
      if (result.cancelled) {
        return result;
      }
      if (result.success) {
        return result;
      }
      lastFailure = result;
    }
    return lastFailure;
  }

  async runJson<T>(
    commandName: string,
    args: string[],
    cwd: string,
    options: RunOptions = {}
  ): Promise<CommandEnvelope<T | null>> {
    const startedAt = Date.now();
    const jsonArgs = [...args, '--json'];
    const result = await this.run(jsonArgs, cwd, options);
    const parsed = parseJsonLine(result.stdout);

    if (parsed && typeof parsed === 'object' && 'schemaVersion' in (parsed as Record<string, unknown>)) {
      return parsed as CommandEnvelope<T>;
    }

    if (result.success && parsed && typeof parsed === 'object') {
      return {
        schemaVersion: '1',
        command: commandName,
        status: 'success',
        data: parsed as T,
        warnings: [],
        errors: [],
        meta: {
          durationMs: Date.now() - startedAt,
          executedCommand: result.command,
          exitCode: result.exitCode,
        },
      };
    }

    return {
      schemaVersion: '1',
      command: commandName,
      status: result.success ? 'success' : 'error',
      data: null,
      warnings: [],
      errors: [
        normalizeError('PYTHON_COMMAND_FAILED', result.stderr || result.stdout || 'Command failed')
      ],
      meta: {
        durationMs: Date.now() - startedAt,
        executedCommand: result.command,
        exitCode: result.exitCode,
      },
    };
  }

  private runSingle(
    cmd: string,
    args: string[],
    cwd: string,
    renderedCommand: string,
    options: RunOptions
  ): Promise<PythonCommandResult> {
    return new Promise((resolve) => {
      if (options.signal?.aborted) {
        resolve({
          success: false,
          command: renderedCommand,
          stdout: '',
          stderr: 'Command cancelled',
          exitCode: null,
          cancelled: true,
        });
        return;
      }

      const child = spawn(cmd, args, { cwd, shell: false });
      let stdout = '';
      let stderr = '';
      let spawnError: string | null = null;
      let cancelled = false;

      const onAbort = () => {
        cancelled = true;
        try {
          child.kill('SIGTERM');
        } catch {
          // Ignore kill race errors.
        }
      };
      options.signal?.addEventListener('abort', onAbort, { once: true });

      child.stdout.on('data', (chunk: Buffer) => {
        const text = chunk.toString();
        stdout += text;
        options.onStdout?.(text);
      });
      child.stderr.on('data', (chunk: Buffer) => {
        const text = chunk.toString();
        stderr += text;
        options.onStderr?.(text);
      });
      child.on('error', (error) => {
        spawnError = error.message;
      });
      child.on('close', (exitCode) => {
        options.signal?.removeEventListener('abort', onAbort);
        const combinedStderr = spawnError ? `${stderr}\n${spawnError}`.trim() : stderr;
        resolve({
          success: exitCode === 0 && !cancelled,
          command: renderedCommand,
          stdout,
          stderr: combinedStderr,
          exitCode,
          cancelled,
        });
      });
    });
  }
}
