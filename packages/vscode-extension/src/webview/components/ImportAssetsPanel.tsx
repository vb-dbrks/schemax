import React from 'react';
import { VSCodeButton, VSCodeDropdown, VSCodeOption, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';
import { getVsCodeApi } from '../vscode-api';
import { ProjectFile } from '../../providers/unity/models';

const vscode = getVsCodeApi();

/** Request for import from live Databricks (target, profile, warehouseId, etc.). */
export interface ImportRunRequestLive {
  target: string;
  profile: string;
  warehouseId: string;
  catalog?: string;
  schema?: string;
  table?: string;
  catalogMappings?: Record<string, string>;
  dryRun: boolean;
  adoptBaseline: boolean;
}

/** Request for import from a SQL DDL file. */
export interface ImportRunRequestFromSql {
  fromSql: {
    sqlPath: string;
    mode: 'diff' | 'replace';
    dryRun: boolean;
    target?: string;
  };
}

/** Union: either live import or from-SQL import. */
export type ImportRunRequest = ImportRunRequestLive | ImportRunRequestFromSql;

export function isImportFromSql(req: ImportRunRequest): req is ImportRunRequestFromSql {
  return 'fromSql' in req && req.fromSql != null;
}

export interface ImportRunResult {
  success: boolean;
  command: string;
  stdout: string;
  stderr: string;
  cancelled?: boolean;
}

export interface ImportProgress {
  phase: string;
  message: string;
  percent: number;
  level?: 'info' | 'warning' | 'error' | 'success';
}

interface ImportAssetsPanelProps {
  project: ProjectFile;
  isRunning: boolean;
  result: ImportRunResult | null;
  progress: ImportProgress | null;
  onClose: () => void;
  onRun: (request: ImportRunRequest) => void;
  onCancel: () => void;
  /** Path received from host after "Browse" for SQL file (optional). */
  pickedSqlFilePath?: string | null;
}

function FieldLabel({ text, help }: { text: string; help: string }) {
  const tooltipId = React.useId();
  const [showHelp, setShowHelp] = React.useState<boolean>(false);

  return (
    <div className="field-label-row">
      <label>{text}</label>
      <button
        type="button"
        className="help-icon-button"
        aria-label={`${text} help`}
        aria-expanded={showHelp}
        aria-describedby={showHelp ? tooltipId : undefined}
        aria-haspopup="true"
        onMouseEnter={() => setShowHelp(true)}
        onMouseLeave={() => setShowHelp(false)}
        onFocus={() => setShowHelp(true)}
        onBlur={() => setShowHelp(false)}
        onClick={() => setShowHelp((current) => !current)}
        onKeyDown={(event) => {
          if (event.key === 'Escape') {
            setShowHelp(false);
          }
        }}
      >
        ?
      </button>
      {showHelp && (
        <div id={tooltipId} role="tooltip" className="help-tooltip">
          {help}
        </div>
      )}
    </div>
  );
}

type ImportTab = 'databricks' | 'sql';

export function ImportAssetsPanel({
  project,
  isRunning,
  result,
  progress,
  onClose,
  onRun,
  onCancel,
  pickedSqlFilePath = null,
}: ImportAssetsPanelProps) {
  const envNames = Object.keys(project.provider?.environments || {});
  const [activeTab, setActiveTab] = React.useState<ImportTab>('databricks');
  const [target, setTarget] = React.useState<string>(envNames[0] || 'dev');
  const [profile, setProfile] = React.useState<string>('DEFAULT');
  const [warehouseId, setWarehouseId] = React.useState<string>('');
  const [catalog, setCatalog] = React.useState<string>('');
  const [schema, setSchema] = React.useState<string>('');
  const [table, setTable] = React.useState<string>('');
  const [catalogMappingsText, setCatalogMappingsText] = React.useState<string>(
    () => buildSuggestedCatalogMappingsText(project, envNames[0] || 'dev')
  );
  const [dryRun, setDryRun] = React.useState<boolean>(true);
  const [adoptBaseline, setAdoptBaseline] = React.useState<boolean>(false);
  const [validationError, setValidationError] = React.useState<string | null>(null);
  const [bindingsTouched, setBindingsTouched] = React.useState<boolean>(false);
  const [lastSuggestedMappings, setLastSuggestedMappings] = React.useState<string>(
    () => buildSuggestedCatalogMappingsText(project, envNames[0] || 'dev')
  );
  // From SQL file tab
  const [sqlPath, setSqlPath] = React.useState<string>('');
  const [sqlMode, setSqlMode] = React.useState<'diff' | 'replace'>('diff');
  const [sqlDryRun, setSqlDryRun] = React.useState<boolean>(true);
  const [sqlTarget, setSqlTarget] = React.useState<string>(envNames[0] || 'dev');

  React.useEffect(() => {
    if (pickedSqlFilePath != null && pickedSqlFilePath !== '') {
      setSqlPath(pickedSqlFilePath);
    }
  }, [pickedSqlFilePath]);

  React.useEffect(() => {
    const suggestion = buildSuggestedCatalogMappingsText(project, target);
    setLastSuggestedMappings(suggestion);
    if (!bindingsTouched || catalogMappingsText === lastSuggestedMappings) {
      setCatalogMappingsText(suggestion);
      setBindingsTouched(false);
    }
  }, [project, target, bindingsTouched, catalogMappingsText, lastSuggestedMappings]);

  React.useEffect(() => {
    // Clear validation errors when switching between SQL and Databricks tabs
    setValidationError(null);
  }, [activeTab]);
  const handleSubmit = () => {
    setValidationError(null);
    if (activeTab === 'sql') {
      const pathTrimmed = sqlPath.trim();
      if (!pathTrimmed) {
        setValidationError('SQL file path is required');
        return;
      }
      onRun({
        fromSql: {
          sqlPath: pathTrimmed,
          mode: sqlMode,
          dryRun: sqlDryRun,
          target: sqlTarget || undefined,
        },
      });
      return;
    }
    if (!warehouseId.trim()) {
      setValidationError('Warehouse ID is required');
      return;
    }
    if (schema.trim() && !catalog.trim()) {
      setValidationError('Schema requires a catalog');
      return;
    }
    if (table.trim() && !schema.trim()) {
      setValidationError('Table requires a schema');
      return;
    }
    let catalogMappings: Record<string, string> | undefined;
    try {
      catalogMappings = parseCatalogMappings(catalogMappingsText);
    } catch (error) {
      setValidationError(String(error));
      return;
    }
    onRun({
      target,
      profile: profile.trim() || 'DEFAULT',
      warehouseId: warehouseId.trim(),
      catalog: catalog.trim() || undefined,
      schema: schema.trim() || undefined,
      table: table.trim() || undefined,
      catalogMappings,
      dryRun,
      adoptBaseline: dryRun ? false : adoptBaseline,
    });
  };

  return (
    <div className="modal-overlay" onClick={() => !isRunning && onClose()}>
      <div className="modal-content import-assets-panel" onClick={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <h2>Import Existing Assets</h2>
          <button className="close-btn" onClick={onClose} disabled={isRunning}>×</button>
        </div>

        <div className="modal-body">
          <p className="section-description">
            Import objects from your provider into SchemaX. Start with dry-run to preview operations.
          </p>

          <div className="import-tabs" role="tablist" aria-label="Import source">
            <button
              type="button"
              id="import-tab-databricks"
              role="tab"
              aria-selected={activeTab === 'databricks'}
              aria-controls="import-panel-databricks"
              className={`import-tab ${activeTab === 'databricks' ? 'import-tab--active' : ''}`}
              onClick={() => setActiveTab('databricks')}
              disabled={isRunning}
            >
              From Databricks
            </button>
            <button
              type="button"
              id="import-tab-sql"
              role="tab"
              aria-selected={activeTab === 'sql'}
              aria-controls="import-panel-sql"
              className={`import-tab ${activeTab === 'sql' ? 'import-tab--active' : ''}`}
              onClick={() => setActiveTab('sql')}
              disabled={isRunning}
            >
              From SQL file
            </button>
          </div>

          {activeTab === 'sql' && (
            <div
              id="import-panel-sql"
              className="import-tab-panel"
              role="tabpanel"
              aria-labelledby="import-tab-sql"
            >
              <div className="modal-field">
                <FieldLabel
                  text="SQL file path"
                  help="Path to a .sql DDL file. Use Browse to pick a file in the workspace or enter a path."
                />
                <div className="import-sql-path-row">
                  <VSCodeTextField
                    value={sqlPath}
                    aria-label="SQL file path"
                    placeholder="e.g. scripts/schema.sql or /absolute/path/to/file.sql"
                    onInput={(event: React.FormEvent<HTMLInputElement>) => setSqlPath((event.target as HTMLInputElement).value)}
                    disabled={isRunning}
                  />
                  <VSCodeButton
                    appearance="secondary"
                    onClick={() => vscode.postMessage({ type: 'import-pick-sql-file' })}
                    disabled={isRunning}
                  >
                    Browse
                  </VSCodeButton>
                </div>
              </div>
              <div className="modal-field">
                <FieldLabel
                  text="Mode"
                  help="Diff: append operations to current changelog. Replace: treat parsed state as new baseline (ops from empty state)."
                />
                <VSCodeDropdown
                  value={sqlMode}
                  aria-label="Import mode"
                  onInput={(e: React.FormEvent) => setSqlMode(((e.target as HTMLInputElement)?.value ?? 'diff') as 'diff' | 'replace')}
                  disabled={isRunning}
                >
                  <VSCodeOption value="diff">Diff (append to changelog)</VSCodeOption>
                  <VSCodeOption value="replace">Replace (new baseline)</VSCodeOption>
                </VSCodeDropdown>
              </div>
              <div className="modal-field">
                <FieldLabel text="Target environment (optional)" help="Used for catalog mapping consistency; can be left as default." />
                <VSCodeDropdown
                  value={sqlTarget}
                  aria-label="Target environment"
                  onInput={(e: React.FormEvent) => setSqlTarget((e.target as HTMLInputElement)?.value ?? '')}
                  disabled={isRunning}
                >
                  {envNames.map((env) => (
                    <VSCodeOption key={env} value={env}>{env}</VSCodeOption>
                  ))}
                </VSCodeDropdown>
              </div>
              <div className="modal-field">
                <label className="import-checkbox">
                  <input
                    type="checkbox"
                    checked={sqlDryRun}
                    disabled={isRunning}
                    onChange={(e) => setSqlDryRun(e.target.checked)}
                  />
                  <span>Dry-run (preview only, do not write changelog)</span>
                </label>
              </div>
            </div>
          )}

          {activeTab === 'databricks' && (
            <div
              id="import-panel-databricks"
              className="import-tab-panel"
              role="tabpanel"
              aria-labelledby="import-tab-databricks"
            >
          <div className="modal-field">
            <FieldLabel
              text="Target Environment"
              help="Select the environment whose catalog mapping and state should be used for this import."
            />
            <VSCodeDropdown
              value={target}
              aria-label="Target Environment"
              onInput={(event: any) => setTarget(event.target.value)}
              disabled={isRunning}
            >
              {envNames.map((env) => (
                <VSCodeOption key={env} value={env}>{env}</VSCodeOption>
              ))}
            </VSCodeDropdown>
          </div>

          <div className="modal-field">
            <FieldLabel
              text="Databricks Profile"
              help="Authentication profile from your Databricks config (for example, DEFAULT or dev)."
            />
            <VSCodeTextField
              value={profile}
              aria-label="Databricks Profile"
              onInput={(event: any) => setProfile(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <FieldLabel
              text="Warehouse ID"
              help="SQL Warehouse used to run metadata discovery queries against Unity Catalog."
            />
            <VSCodeTextField
              value={warehouseId}
              aria-label="Warehouse ID"
              placeholder="e.g. 1234abcd5678efgh"
              onInput={(event: any) => setWarehouseId(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <FieldLabel
              text="Catalog (optional)"
              help="Limit discovery to a single catalog. Leave empty to discover across all visible catalogs."
            />
            <VSCodeTextField
              value={catalog}
              aria-label="Catalog (optional)"
              placeholder="main"
              onInput={(event: any) => setCatalog(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <FieldLabel
              text="Schema (optional)"
              help="Limit discovery to a schema within the selected catalog."
            />
            <VSCodeTextField
              value={schema}
              aria-label="Schema (optional)"
              placeholder="analytics"
              onInput={(event: any) => setSchema(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <FieldLabel
              text="Table (optional)"
              help="Limit discovery to one table or view within the selected schema."
            />
            <VSCodeTextField
              value={table}
              aria-label="Table (optional)"
              placeholder="users"
              onInput={(event: any) => setTable(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <FieldLabel
              text="Catalog mappings (optional)"
              help="Map SchemaX logical catalog names to physical catalog names in this environment. Format: logical=physical."
            />
            <textarea
              className="import-bindings-textarea"
              aria-label="Catalog mappings (optional)"
              value={catalogMappingsText}
              onChange={(event) => {
                setCatalogMappingsText(event.target.value);
                setBindingsTouched(true);
              }}
              placeholder={'schemax_demo=dev_schemax_demo\ncore=dev_finance_core'}
              disabled={isRunning}
            />
            <p className="field-help">
              One mapping per line: <code>logical=physical</code>
            </p>
          </div>

          <FieldLabel
            text="Execution settings"
            help="Choose whether to preview only or write import operations. Optionally adopt the imported snapshot as the deployed baseline."
          />
          <div className="import-settings-grid">
            <div className="import-setting">
              <fieldset className="import-radio-group" aria-label="Run type">
                <legend>Run type</legend>
                <label className="import-radio-option">
                  <input
                    type="radio"
                    name="import-run-type"
                    checked={dryRun}
                    disabled={isRunning}
                    onChange={() => setDryRun(true)}
                  />
                  <span>Dry-run preview (no file changes)</span>
                </label>
                <label className="import-radio-option">
                  <input
                    type="radio"
                    name="import-run-type"
                    checked={!dryRun}
                    disabled={isRunning}
                    onChange={() => setDryRun(false)}
                  />
                  <span>Import and write operations</span>
                </label>
              </fieldset>
            </div>
            <div className="import-setting">
              <label className="import-checkbox">
                <input
                  type="checkbox"
                  checked={adoptBaseline}
                  disabled={isRunning || dryRun}
                  onChange={(event) => setAdoptBaseline(event.target.checked)}
                />
                <span>Adopt imported snapshot as deployment baseline</span>
              </label>
              {dryRun && (
                <p className="field-help import-checkbox-subtext">
                  Available only when run type is <code>Import and write operations</code>.
                </p>
              )}
            </div>
          </div>
          <p className="field-help">
            {dryRun
              ? 'Execution summary: preview only. SchemaX will discover assets and show operations without writing files.'
              : (adoptBaseline
                  ? 'Execution summary: write import operations and adopt the imported snapshot as deployed baseline.'
                  : 'Execution summary: write import operations without adopting baseline.')}
          </p>
            </div>
          )}

          {validationError && (
            <p className="form-error">{validationError}</p>
          )}

          {(isRunning || progress) && (
            <div className={`import-progress ${progress?.level || 'info'}`}>
              <div className="import-progress__header">
                <strong>Import progress</strong>
                <span>{Math.max(0, Math.min(100, progress?.percent ?? (isRunning ? 5 : 0)))}%</span>
              </div>
              <div className="import-progress__bar">
                <div
                  className="import-progress__fill"
                  style={{ width: `${Math.max(0, Math.min(100, progress?.percent ?? (isRunning ? 5 : 0)))}%` }}
                />
              </div>
              <p className="import-progress__message">
                {progress?.message || 'Running import...'}
              </p>
            </div>
          )}

          {result && (
            <div className={`import-result ${result.cancelled ? 'warning' : (result.success ? 'success' : 'error')}`}>
              <strong>{result.cancelled ? 'Import cancelled' : (result.success ? 'Import completed' : 'Import failed')}</strong>
              {result.command && <p><code>{result.command}</code></p>}
              {result.stdout && <pre>{result.stdout}</pre>}
              {result.stderr && <pre>{result.stderr}</pre>}
            </div>
          )}
        </div>

        <div className="modal-footer">
          {isRunning ? (
            <VSCodeButton appearance="secondary" onClick={onCancel}>
              Cancel import
            </VSCodeButton>
          ) : (
            <VSCodeButton appearance="secondary" onClick={onClose}>
              Close
            </VSCodeButton>
          )}
          <VSCodeButton onClick={handleSubmit} disabled={isRunning}>
            {isRunning ? 'Running...' : 'Run'}
          </VSCodeButton>
        </div>
      </div>
    </div>
  );
}

function parseCatalogMappings(input: string): Record<string, string> | undefined {
  const trimmed = input.trim();
  if (!trimmed) return undefined;

  const bindings: Record<string, string> = {};
  const lines = trimmed.split('\n').map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    const eqIndex = line.indexOf('=');
    if (eqIndex <= 0 || eqIndex === line.length - 1) {
      throw new Error(`Invalid catalog mapping '${line}'. Expected logical=physical`);
    }
    const logical = line.slice(0, eqIndex).trim();
    const physical = line.slice(eqIndex + 1).trim();
    if (!logical || !physical) {
      throw new Error(`Invalid catalog mapping '${line}'. Expected logical=physical`);
    }
    bindings[logical] = physical;
  }
  return bindings;
}

function buildSuggestedCatalogMappingsText(project: ProjectFile, target: string): string {
  const envCfg = project.provider?.environments?.[target] || {};
  const existingMappings = envCfg.catalogMappings as Record<string, string> | undefined;
  if (existingMappings && Object.keys(existingMappings).length > 0) {
    return Object.entries(existingMappings)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([logical, physical]) => `${logical}=${physical}`)
      .join('\n');
  }

  const logicalCatalogs = project.state?.catalogs || [];
  const topLevelName = String(envCfg.topLevelName || '').trim();
  if (logicalCatalogs.length === 1 && topLevelName) {
    const logicalName = String(logicalCatalogs[0]?.name || '').trim();
    if (logicalName && logicalName !== topLevelName) {
      return `${logicalName}=${topLevelName}`;
    }
  }

  return '';
}
