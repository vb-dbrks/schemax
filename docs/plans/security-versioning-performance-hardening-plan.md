# SchemaX Security, Version Compatibility, and Performance Hardening Plan

## Objective
Harden the VS Code extension + Python SDK integration for production:
- tighten runtime security controls,
- enforce extension/SDK compatibility contracts,
- improve performance for large project/state files.

## Scope
- `packages/vscode-extension`
- `packages/python-sdk`
- cross-repo CLI JSON command envelope contracts.

## Phase 1: Security Hardening
1. Command execution boundary hardening
- Keep `spawn(..., { shell: false })` as mandatory.
- Add argument allowlists for extension-invoked CLI commands.
- Reject unsafe workspace path traversal and non-workspace execution roots.

2. Input and output contract hardening
- Require strict envelope validation for all extension-consumed commands.
- Treat malformed envelopes as hard errors with explicit codes.
- Add schema-version checks on every response.

3. Trust boundary controls
- Enforce workspace trust before mutating commands (`import`, `apply`, `rollback`).
- Add explicit user confirmation for high-impact operations when not in CI mode.
- Improve redaction of sensitive values in logs/errors (profile, warehouse IDs, paths where needed).

4. Security test gates
- Add negative tests for malformed/hostile output and invalid argument patterns.
- Add architecture fitness checks blocking direct ad-hoc shell workflow invocations.

## Phase 2: Extension/SDK Compatibility
1. Runtime compatibility handshake
- Add CLI command for machine-readable info (SDK version, envelope schema version, capabilities).
- Extension validates SDK version against a supported semver range.

2. UX for mismatch states
- Show actionable errors:
  - update extension,
  - update SDK,
  - re-run install command.
- Include detected versions in error message and output channel.

3. Contract versioning policy
- Pin extension-supported envelope schema range.
- Add forward-compat behavior: unknown fields allowed, missing required fields rejected.

4. Compatibility tests
- Unit tests for older/newer SDK version scenarios.
- Integration tests for unsupported/missing capabilities.

## Phase 3: Performance and Scalability
1. Workspace read path optimization
- Add cache with file mtime/hash invalidation for project/changelog/state loads.
- Avoid repeated full parse/deserialize in same command session.

2. Write path optimization
- Batch/debounce write operations from UI workflows.
- Reduce full-file rewrites where safe (session-based write coalescing).

3. Payload minimization
- Add selective/summary workspace-state mode for extension screens that do not need full state.
- Keep full payload only for editing contexts that require it.

4. Benchmarks and budgets
- Add benchmark fixtures: small/medium/large workspaces.
- Track:
  - workspace-state latency,
  - import planning latency,
  - serialization/deserialization time,
  - peak memory.
- Add thresholds and CI trend checks for non-live workloads.

## Phase 4: Rollout and Guardrails
1. Feature flags
- Roll out compatibility enforcement and caching behind flags, then default on after validation.

2. Observability
- Add timing + failure telemetry for command transport and storage operations.
- Emit reason codes for contract failures and compatibility mismatches.

3. Documentation
- Add operator docs for:
  - supported extension/SDK version matrix,
  - security model,
  - performance tuning knobs.

## Acceptance Criteria
1. No direct non-contract workflow shell calls in extension runtime paths.
2. SDK mismatch is detected and surfaced before workflow execution.
3. Malformed envelopes never reach business logic.
4. Measurable latency improvement on medium/large workspaces vs current baseline.
5. Security and compatibility test suites pass in regular CI gates.
