import React from 'react';
import { VSCodeButton, VSCodeDropdown, VSCodeOption, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';
import { ProjectFile } from '../../providers/unity/models';

export interface ImportRunRequest {
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

export interface ImportRunResult {
  success: boolean;
  command: string;
  stdout: string;
  stderr: string;
}

interface ImportAssetsPanelProps {
  project: ProjectFile;
  isRunning: boolean;
  result: ImportRunResult | null;
  onClose: () => void;
  onRun: (request: ImportRunRequest) => void;
}

export function ImportAssetsPanel({ project, isRunning, result, onClose, onRun }: ImportAssetsPanelProps) {
  const envNames = Object.keys(project.provider?.environments || {});
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

  React.useEffect(() => {
    const suggestion = buildSuggestedCatalogMappingsText(project, target);
    setLastSuggestedMappings(suggestion);
    if (!bindingsTouched || catalogMappingsText === lastSuggestedMappings) {
      setCatalogMappingsText(suggestion);
      setBindingsTouched(false);
    }
  }, [project, target, bindingsTouched, catalogMappingsText, lastSuggestedMappings]);

  const handleSubmit = () => {
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
    setValidationError(null);
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
            Import objects from your provider into Schematic. Start with dry-run to preview operations.
          </p>

          <div className="modal-field">
            <label>Target Environment</label>
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
            <label>Databricks Profile</label>
            <VSCodeTextField
              value={profile}
              aria-label="Databricks Profile"
              onInput={(event: any) => setProfile(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <label>Warehouse ID</label>
            <VSCodeTextField
              value={warehouseId}
              aria-label="Warehouse ID"
              placeholder="e.g. 1234abcd5678efgh"
              onInput={(event: any) => setWarehouseId(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <label>Catalog (optional)</label>
            <VSCodeTextField
              value={catalog}
              aria-label="Catalog (optional)"
              placeholder="main"
              onInput={(event: any) => setCatalog(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <label>Schema (optional)</label>
            <VSCodeTextField
              value={schema}
              aria-label="Schema (optional)"
              placeholder="analytics"
              onInput={(event: any) => setSchema(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <label>Table (optional)</label>
            <VSCodeTextField
              value={table}
              aria-label="Table (optional)"
              placeholder="users"
              onInput={(event: any) => setTable(event.target.value)}
              disabled={isRunning}
            />
          </div>

          <div className="modal-field">
            <label>Catalog mappings (optional)</label>
            <textarea
              className="import-bindings-textarea"
              aria-label="Catalog mappings (optional)"
              value={catalogMappingsText}
              onChange={(event) => {
                setCatalogMappingsText(event.target.value);
                setBindingsTouched(true);
              }}
              placeholder={'schematic_demo=dev_schematic_demo\ncore=dev_finance_core'}
              disabled={isRunning}
            />
            <p className="field-help">
              One mapping per line: <code>logical=physical</code>
            </p>
          </div>

          <div className="import-mode-row">
            <VSCodeButton
              appearance={dryRun ? 'primary' : 'secondary'}
              onClick={() => setDryRun(true)}
              disabled={isRunning}
            >
              Dry run
            </VSCodeButton>
            <VSCodeButton
              appearance={!dryRun ? 'primary' : 'secondary'}
              onClick={() => setDryRun(false)}
              disabled={isRunning}
            >
              Import
            </VSCodeButton>
            {!dryRun && (
              <VSCodeButton
                appearance={adoptBaseline ? 'primary' : 'secondary'}
                onClick={() => setAdoptBaseline(!adoptBaseline)}
                disabled={isRunning}
              >
                {adoptBaseline ? 'Adopt baseline: ON' : 'Adopt baseline: OFF'}
              </VSCodeButton>
            )}
          </div>

          {validationError && (
            <p className="form-error">{validationError}</p>
          )}

          {result && (
            <div className={`import-result ${result.success ? 'success' : 'error'}`}>
              <strong>{result.success ? 'Import completed' : 'Import failed'}</strong>
              {result.command && <p><code>{result.command}</code></p>}
              {result.stdout && <pre>{result.stdout}</pre>}
              {result.stderr && <pre>{result.stderr}</pre>}
            </div>
          )}
        </div>

        <div className="modal-footer">
          <VSCodeButton appearance="secondary" onClick={onClose} disabled={isRunning}>
            Close
          </VSCodeButton>
          <VSCodeButton onClick={handleSubmit} disabled={isRunning}>
            {isRunning ? 'Running...' : (dryRun ? 'Run dry-run' : 'Run import')}
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
