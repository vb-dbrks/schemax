import { spawn } from "child_process";
import * as vscode from "vscode";
import type { CommandEnvelope, PythonCommandResult } from "./contracts";

type StreamHandler = (chunk: string) => void;

interface RunOptions {
  onStdout?: StreamHandler;
  onStderr?: StreamHandler;
  signal?: AbortSignal;
}

interface CommandCandidate {
  cmd: string;
  baseArgs: string[];
  /** Use shell: false for absolute interpreter paths (avoids shell metachar issues). */
  useShell: boolean;
}

const BASE_COMMAND_CANDIDATES: CommandCandidate[] = [
  { cmd: "schemax", baseArgs: [], useShell: true },
  { cmd: "python3", baseArgs: ["-m", "schemax.cli"], useShell: true },
  { cmd: "python", baseArgs: ["-m", "schemax.cli"], useShell: true },
];

/**
 * Build the ordered list of command candidates.
 * If the user (or VS Code Python extension) has configured an interpreter path,
 * prepend it so that venv / uv / poetry / conda envs selected in VS Code are
 * tried first — before falling back to PATH-based lookup.
 *
 * Configured interpreter paths are spawned with shell: false to correctly
 * handle paths containing spaces (common on Windows/macOS).
 */
function getCommandCandidates(): CommandCandidate[] {
  const configured = vscode.workspace
    .getConfiguration("python")
    .get<string>("defaultInterpreterPath");
  if (configured && configured.trim()) {
    const pyPath = configured.trim();
    return [
      { cmd: pyPath, baseArgs: ["-m", "schemax.cli"], useShell: false },
      ...BASE_COMMAND_CANDIDATES,
    ];
  }
  return [...BASE_COMMAND_CANDIDATES];
}

function parseJsonLine(output: string): unknown {
  const lines = output
    .trim()
    .split("\n")
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

function isObjectLike(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function normalizeError(code: string, message: string): CommandEnvelope<null>["errors"][number] {
  return { code, message };
}

function isErrorEntry(value: unknown): value is CommandEnvelope<null>["errors"][number] {
  return isObjectLike(value) && typeof value.code === "string" && typeof value.message === "string";
}

function isEnvelopeMeta(value: unknown): value is CommandEnvelope<null>["meta"] {
  return (
    isObjectLike(value) &&
    typeof value.durationMs === "number" &&
    typeof value.executedCommand === "string" &&
    (typeof value.exitCode === "number" || value.exitCode === null)
  );
}

function isCommandEnvelope(value: unknown): value is CommandEnvelope<unknown> {
  return (
    isObjectLike(value) &&
    value.schemaVersion === "1" &&
    typeof value.command === "string" &&
    (value.status === "success" || value.status === "error") &&
    Array.isArray(value.warnings) &&
    value.warnings.every((item) => typeof item === "string") &&
    Array.isArray(value.errors) &&
    value.errors.every((item) => isErrorEntry(item)) &&
    isEnvelopeMeta(value.meta) &&
    "data" in value
  );
}

export class PythonBackendClient {
  async run(args: string[], cwd: string, options: RunOptions = {}): Promise<PythonCommandResult> {
    let lastFailure: PythonCommandResult = {
      success: false,
      command: "",
      stdout: "",
      stderr: "No command candidate executed.",
      exitCode: null,
    };

    for (const candidate of getCommandCandidates()) {
      const fullArgs = [...candidate.baseArgs, ...args];
      const rendered = `${candidate.cmd} ${fullArgs.join(" ")}`;
      const result = await this.runSingle(
        candidate.cmd,
        fullArgs,
        cwd,
        rendered,
        candidate.useShell,
        options,
      );
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
    const jsonArgs = [...args, "--json"];
    const result = await this.run(jsonArgs, cwd, options);
    const parsed = parseJsonLine(result.stdout);

    if (isCommandEnvelope(parsed)) {
      return parsed as CommandEnvelope<T>;
    }

    if (result.success && isObjectLike(parsed) && "schemaVersion" in parsed) {
      return {
        schemaVersion: "1",
        command: commandName,
        status: "error",
        data: null,
        warnings: [],
        errors: [normalizeError("INVALID_ENVELOPE", "Command returned malformed envelope JSON.")],
        meta: {
          durationMs: Date.now() - startedAt,
          executedCommand: result.command,
          exitCode: result.exitCode,
        },
      };
    }

    if (result.success && isObjectLike(parsed)) {
      return {
        schemaVersion: "1",
        command: commandName,
        status: "success",
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

    const nonJsonSuccess = result.success && !isObjectLike(parsed);
    const errorMessage = nonJsonSuccess
      ? "Command succeeded but did not return JSON output."
      : result.stderr || result.stdout || "Command failed";

    return {
      schemaVersion: "1",
      command: commandName,
      status: "error",
      data: null,
      warnings: [],
      errors: [normalizeError("PYTHON_COMMAND_FAILED", errorMessage)],
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
    useShell: boolean,
    options: RunOptions,
  ): Promise<PythonCommandResult> {
    return new Promise((resolve) => {
      if (options.signal?.aborted) {
        resolve({
          success: false,
          command: renderedCommand,
          stdout: "",
          stderr: "Command cancelled",
          exitCode: null,
          cancelled: true,
        });
        return;
      }

      const child = spawn(cmd, args, { cwd, shell: useShell });
      let stdout = "";
      let stderr = "";
      let spawnError: string | null = null;
      let cancelled = false;

      const onAbort = () => {
        cancelled = true;
        try {
          child.kill("SIGTERM");
        } catch {
          // Ignore kill race errors.
        }
      };
      options.signal?.addEventListener("abort", onAbort, { once: true });

      child.stdout.on("data", (chunk: Buffer) => {
        const text = chunk.toString();
        stdout += text;
        options.onStdout?.(text);
      });
      child.stderr.on("data", (chunk: Buffer) => {
        const text = chunk.toString();
        stderr += text;
        options.onStderr?.(text);
      });
      child.on("error", (error) => {
        spawnError = error.message;
      });
      child.on("close", (exitCode) => {
        options.signal?.removeEventListener("abort", onAbort);
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
