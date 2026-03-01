import React, { useState, useEffect } from 'react';
import { VSCodeButton, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';
import type { ProjectFile } from '../models/unity';
import { getVsCodeApi } from '../vscode-api';

const vscode = getVsCodeApi();

interface ProjectSettingsPanelProps {
  project: ProjectFile;
  onClose: () => void;
}

interface LocationDefinition {
  description?: string;
  paths: Record<string, string>; // environmentName -> physical path
}

const ALL_MANAGED_CATEGORIES = [
  { id: 'catalog_structure', label: 'Catalog structure' },
  { id: 'schema_structure', label: 'Schema structure' },
  { id: 'table_structure', label: 'Table structure' },
  { id: 'view_structure', label: 'View structure' },
  { id: 'volume_structure', label: 'Volume structure' },
  { id: 'function_structure', label: 'Function structure' },
  { id: 'materialized_view_structure', label: 'Materialized view structure' },
  { id: 'governance', label: 'Governance' },
] as const;

interface EnvironmentConfig {
  topLevelName: string;
  catalogMappings?: Record<string, string>;
  description?: string;
  allowDrift: boolean;
  requireSnapshot: boolean;
  requireApproval?: boolean;
  autoCreateTopLevel: boolean;
  autoCreateSchemaxSchema: boolean;
  managedCategories?: string[];
  existingObjects?: { catalog?: string[]; schema?: string[]; table?: string[] };
}

interface LocationModalData {
  type: 'managed' | 'external';
  mode: 'add' | 'edit';
  name: string;
  description: string;
  paths: Record<string, string>; // env -> path
}

type LocationMap = Record<string, LocationDefinition>;
type EnvironmentMap = Record<string, EnvironmentConfig>;

function formatCatalogMappingsText(mappings?: Record<string, string>): string {
  if (!mappings) {
    return '';
  }
  return Object.entries(mappings)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([logical, physical]) => `${logical}=${physical}`)
    .join('\n');
}

function parseCatalogMappingsText(text: string): Record<string, string> {
  const mappings: Record<string, string> = {};
  const lines = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    const separator = line.indexOf('=');
    if (separator <= 0 || separator === line.length - 1) {
      throw new Error(`Invalid mapping '${line}'. Expected format logical=physical`);
    }
    const logical = line.slice(0, separator).trim();
    const physical = line.slice(separator + 1).trim();
    if (!logical || !physical) {
      throw new Error(`Invalid mapping '${line}'. Expected format logical=physical`);
    }
    mappings[logical] = physical;
  }

  return mappings;
}

export function ProjectSettingsPanel({ project, onClose }: ProjectSettingsPanelProps) {
  const [editedProject, setEditedProject] = useState<ProjectFile>(JSON.parse(JSON.stringify(project)));
  const [expandedEnvs, setExpandedEnvs] = useState<Set<string>>(new Set([project.activeEnvironment || 'dev']));
  const [editingCatalog, setEditingCatalog] = useState<string | null>(null);
  const [editCatalogValue, setEditCatalogValue] = useState('');
  const [showLocationModal, setShowLocationModal] = useState(false);
  const [locationModalData, setLocationModalData] = useState<LocationModalData | null>(null);
  const [deleteConfirmation, setDeleteConfirmation] = useState<{
    type: 'managed' | 'external';
    name: string;
  } | null>(null);
  const [isDirty, setIsDirty] = useState(false);
  const [locationNameError, setLocationNameError] = useState<string | null>(null);
  const [mappingTextByEnv, setMappingTextByEnv] = useState<Record<string, string>>({});
  const [mappingErrorByEnv, setMappingErrorByEnv] = useState<Record<string, string>>({});

  const activeEnv = project.activeEnvironment || 'dev';
  const environments: EnvironmentMap = (editedProject.provider?.environments as EnvironmentMap) || {};
  const environmentNames = Object.keys(environments);
  const logicalCatalogs = editedProject.state?.catalogs || [];
  const hasMappingErrors = Object.values(mappingErrorByEnv).some(Boolean);

  useEffect(() => {
    const initialText: Record<string, string> = {};
    environmentNames.forEach((envName) => {
      initialText[envName] = formatCatalogMappingsText(environments[envName]?.catalogMappings);
    });
    setMappingTextByEnv(initialText);
    setMappingErrorByEnv({});
  }, [editedProject.provider?.environments]);

  const toggleEnv = (envName: string) => {
    const newExpanded = new Set(expandedEnvs);
    if (newExpanded.has(envName)) {
      newExpanded.delete(envName);
    } else {
      newExpanded.add(envName);
    }
    setExpandedEnvs(newExpanded);
  };

  const handleSave = () => {
    vscode.postMessage({
      type: 'update-project-config',
      payload: editedProject
    });
    setIsDirty(false);
    onClose();
  };

  const handleCancel = () => {
    if (isDirty) {
      if (confirm('You have unsaved changes. Are you sure you want to cancel?')) {
        onClose();
      }
    } else {
      onClose();
    }
  };

  const startEditCatalog = (envName: string) => {
    setEditingCatalog(envName);
    setEditCatalogValue(environments[envName]?.topLevelName || '');
  };

  const saveCatalogEdit = (envName: string) => {
    const updated = { ...editedProject };
    if (updated.provider?.environments?.[envName]) {
      updated.provider.environments[envName].topLevelName = editCatalogValue;
      setEditedProject(updated);
      setIsDirty(true);
    }
    setEditingCatalog(null);
  };

  const updateCatalogMappings = (envName: string, rawText: string) => {
    setMappingTextByEnv((current) => ({ ...current, [envName]: rawText }));

    try {
      const mappings = parseCatalogMappingsText(rawText);
      const updated = { ...editedProject };
      if (updated.provider?.environments?.[envName]) {
        updated.provider.environments[envName].catalogMappings = mappings;
        setEditedProject(updated);
        setIsDirty(true);
      }
      setMappingErrorByEnv((current) => ({ ...current, [envName]: '' }));
    } catch (error) {
      setMappingErrorByEnv((current) => ({ ...current, [envName]: String(error) }));
    }
  };

  const cancelCatalogEdit = () => {
    setEditingCatalog(null);
    setEditCatalogValue('');
  };

  const toggleManagedCategory = (envName: string, categoryId: string) => {
    const updated = { ...editedProject };
    const providerEnvironments = updated.provider?.environments;
    if (!providerEnvironments) return;
    const env = providerEnvironments[envName];
    if (!env) return;
    const current = env.managedCategories ?? ALL_MANAGED_CATEGORIES.map((c) => c.id);
    const set = new Set(current);
    if (set.has(categoryId)) {
      set.delete(categoryId);
    } else {
      set.add(categoryId);
    }
    const next = [...set];
    providerEnvironments[envName] = {
      ...env,
      managedCategories: next.length === ALL_MANAGED_CATEGORIES.length ? undefined : next,
    };
    setEditedProject(updated);
    setIsDirty(true);
  };

  const updateExistingCatalogs = (envName: string, rawText: string) => {
    const updated = { ...editedProject };
    const providerEnvironments = updated.provider?.environments;
    if (!providerEnvironments) return;
    const env = providerEnvironments[envName];
    if (!env) return;
    const list = rawText
      .split(/[\n,]/)
      .map((s) => s.trim())
      .filter(Boolean);
    providerEnvironments[envName] = {
      ...env,
      existingObjects: {
        ...(env.existingObjects ?? {}),
        catalog: list.length > 0 ? list : undefined,
      },
    };
    setEditedProject(updated);
    setIsDirty(true);
  };

  // Add Location
  const openAddLocationModal = (type: 'managed' | 'external') => {
    const initialPaths: Record<string, string> = {};
    environmentNames.forEach(env => {
      initialPaths[env] = '';
    });

    setLocationModalData({
      type,
      mode: 'add',
      name: '',
      description: '',
      paths: initialPaths
    });
    setLocationNameError(null);
    setShowLocationModal(true);
  };

  // Edit Location
  const openEditLocationModal = (type: 'managed' | 'external', name: string) => {
    const locations = type === 'managed' 
      ? (editedProject.managedLocations || {})
      : (editedProject.externalLocations || {});
    
    const location = locations[name];
    if (!location) return;

    // Ensure all environments have a path entry (even if empty)
    const paths: Record<string, string> = { ...location.paths };
    environmentNames.forEach(env => {
      if (!(env in paths)) {
        paths[env] = '';
      }
    });

    setLocationModalData({
      type,
      mode: 'edit',
      name,
      description: location.description || '',
      paths
    });
    setShowLocationModal(true);
  };

  // Save Location
  const saveLocation = () => {
    if (!locationModalData) return;

    const { type, mode, name, description, paths } = locationModalData;

    // Validation
    if (!name.trim()) {
      setLocationNameError('Location name is required');
      return;
    }

    // Unity Catalog identifiers: letters, numbers, underscores only (no hyphens)
    if (!/^[a-z][a-z0-9_]*$/.test(name)) {
      setLocationNameError('Use only lowercase letters, numbers, and underscores (e.g. my_location). Hyphens are not allowed.');
      return;
    }
    setLocationNameError(null);

    // Check for at least one path
    const hasPath = Object.values(paths).some(p => p.trim());
    if (!hasPath) {
      alert('At least one environment path must be configured');
      return;
    }

    // Check for duplicate name (only in add mode)
    if (mode === 'add') {
      const existingLocations = type === 'managed'
        ? (editedProject.managedLocations || {})
        : (editedProject.externalLocations || {});
      
      if (name in existingLocations) {
        alert(`A ${type} location with name "${name}" already exists`);
        return;
      }
    }

    // Clean up paths (remove empty ones)
    const cleanedPaths: Record<string, string> = {};
    Object.entries(paths).forEach(([env, path]) => {
      if (path.trim()) {
        cleanedPaths[env] = path.trim();
      }
    });

    const newLocation: LocationDefinition = {
      description: description.trim() || undefined,
      paths: cleanedPaths
    };

    const updated = { ...editedProject };
    
    if (type === 'managed') {
      if (!updated.managedLocations) updated.managedLocations = {};
      updated.managedLocations[name] = newLocation;
    } else {
      if (!updated.externalLocations) updated.externalLocations = {};
      updated.externalLocations[name] = newLocation;
    }

    setEditedProject(updated);
    setIsDirty(true);
    setShowLocationModal(false);
    setLocationModalData(null);
    setLocationNameError(null);
  };

  // Delete Location
  const confirmDeleteLocation = (type: 'managed' | 'external', name: string) => {
    setDeleteConfirmation({ type, name });
  };

  const deleteLocation = () => {
    if (!deleteConfirmation) return;

    const { type, name } = deleteConfirmation;
    const updated = { ...editedProject };

    if (type === 'managed' && updated.managedLocations) {
      delete updated.managedLocations[name];
    } else if (type === 'external' && updated.externalLocations) {
      delete updated.externalLocations[name];
    }

    setEditedProject(updated);
    setIsDirty(true);
    setDeleteConfirmation(null);
  };

  return (
    <div className="modal-overlay" onClick={handleCancel}>
      <div className="modal-content project-settings-panel" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>Project Settings</h2>
          <button className="close-btn" onClick={handleCancel}>×</button>
        </div>

        <div className="modal-body">
          {/* Project Metadata (Read-only) */}
          <div className="settings-section">
            <h3>Project Overview</h3>
            <div className="info-grid">
              <div className="info-row">
                <span className="info-label">Project name</span>
                <span className="info-value">{editedProject.name}</span>
              </div>
              <div className="info-row">
                <span className="info-label">Provider</span>
                <span className="info-value">{editedProject.provider?.type} · v{editedProject.provider?.version}</span>
              </div>
              <div className="info-row">
                <span className="info-label">Latest snapshot</span>
                <span className="info-value">{editedProject.latestSnapshot || 'None yet'}</span>
              </div>
              <div className="info-row">
                <span className="info-label">Snapshots</span>
                <span className="info-value">{editedProject.snapshots?.length || 0}</span>
              </div>
            </div>
          </div>

          {/* Project-level Managed Locations */}
          <div className="settings-section">
            <h3>Physical Isolation (Managed Tables)</h3>
            <p className="section-description">
              Configure storage locations for managed tables at the catalog or schema level. Define location names here and specify paths for each environment.
            </p>

            {Object.keys((editedProject.managedLocations as LocationMap) || {}).length > 0 ? (
              <div className="location-list">
                {Object.entries((editedProject.managedLocations as LocationMap) || {}).map(([name, location]) => (
                  <div key={name} className="location-item">
                    <div className="location-header">
                      <strong>{name}</strong>
                      <div className="location-actions">
                        <VSCodeButton appearance="icon" onClick={() => openEditLocationModal('managed', name)}>
                          ✏️
                        </VSCodeButton>
                        <VSCodeButton appearance="icon" onClick={() => confirmDeleteLocation('managed', name)}>
                          🗑️
                        </VSCodeButton>
                      </div>
                    </div>
                    {location.description && (
                      <p className="location-description">{location.description}</p>
                    )}
                    <div className="location-paths">
                      {Object.entries(location.paths).map(([env, path]) => (
                        <div key={env} className="path-row">
                          <span className="path-env">{env}</span>
                          <code className="path-value">{path}</code>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="empty-state">
                No managed locations configured. Catalogs and schemas will use Unity Catalog's default storage.
              </div>
            )}

            <VSCodeButton onClick={() => openAddLocationModal('managed')}>
              + Add Managed Location
            </VSCodeButton>
          </div>

          {/* Project-level External Locations */}
          <div className="settings-section">
            <h3>External Locations (External Tables)</h3>
            <p className="section-description">
              Configure storage locations for external tables. Define location names here and specify paths for each environment.
            </p>

            {Object.keys((editedProject.externalLocations as LocationMap) || {}).length > 0 ? (
              <div className="location-list">
                {Object.entries((editedProject.externalLocations as LocationMap) || {}).map(([name, location]) => (
                  <div key={name} className="location-item">
                    <div className="location-header">
                      <strong>{name}</strong>
                      <div className="location-actions">
                        <VSCodeButton appearance="icon" onClick={() => openEditLocationModal('external', name)}>
                          ✏️
                        </VSCodeButton>
                        <VSCodeButton appearance="icon" onClick={() => confirmDeleteLocation('external', name)}>
                          🗑️
                        </VSCodeButton>
                      </div>
                    </div>
                    {location.description && (
                      <p className="location-description">{location.description}</p>
                    )}
                    <div className="location-paths">
                      {Object.entries(location.paths).map(([env, path]) => (
                        <div key={env} className="path-row">
                          <span className="path-env">{env}</span>
                          <code className="path-value">{path}</code>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="empty-state">
                No external locations configured.
              </div>
            )}

            <VSCodeButton onClick={() => openAddLocationModal('external')}>
              + Add External Location
            </VSCodeButton>
          </div>

          {/* Environment Configuration */}
          <div className="settings-section">
            <h3>Environment Configuration</h3>
            <p className="section-description">
              Configure catalog mappings (Logical Isolation) for each environment.
            </p>

            {Object.entries(environments).map(([envName, envConfig]) => (
              <div key={envName} className="environment-section">
                <div className="env-header" onClick={() => toggleEnv(envName)}>
                  <span className="env-toggle">{expandedEnvs.has(envName) ? '▼' : '▶'}</span>
                  <span className="env-name">{envName}</span>
                  {envName === activeEnv && <span className="active-indicator">● Active</span>}
                </div>

                {expandedEnvs.has(envName) && (
                  <div className="env-content">
                    {/* Logical Isolation (Catalog Mapping) */}
                    <div className="isolation-section">
                      <h4>Logical Isolation</h4>
                      <p className="help-text">
                        Configure logical-to-physical catalog mapping for this environment.
                      </p>

                      {editingCatalog === envName ? (
                        <div className="inline-edit">
                          <VSCodeTextField
                            value={editCatalogValue}
                            onInput={(e) => setEditCatalogValue((e.target as HTMLInputElement).value)}
                            placeholder="physical_catalog_name"
                          />
                          <VSCodeButton onClick={() => saveCatalogEdit(envName)}>
                            Save
                          </VSCodeButton>
                          <VSCodeButton appearance="secondary" onClick={cancelCatalogEdit}>
                            Cancel
                          </VSCodeButton>
                        </div>
                      ) : (
                        <div className="catalog-display">
                          <span className="label">Tracking Catalog:</span>
                          <code>{envConfig.topLevelName}</code>
                          <VSCodeButton appearance="icon" onClick={() => startEditCatalog(envName)}>
                            ✏️
                          </VSCodeButton>
                        </div>
                      )}
                      <div className="modal-field" style={{ marginTop: '10px' }}>
                        <label>Catalog mappings (logical=physical)</label>
                        <textarea
                          className="import-bindings-textarea"
                          value={mappingTextByEnv[envName] ?? ''}
                          onChange={(e) => updateCatalogMappings(envName, e.target.value)}
                          placeholder={'schemax_demo=dev_schemax_demo\nsamples=dev_samples'}
                        />
                        <p className="field-help">
                          One mapping per line. SQL/apply requires mappings for all logical catalogs.
                        </p>
                        {mappingErrorByEnv[envName] && (
                          <p className="field-error" style={{ color: 'var(--vscode-errorForeground)', marginTop: '4px', fontSize: '12px' }}>
                            {mappingErrorByEnv[envName]}
                          </p>
                        )}
                        {logicalCatalogs.length > 0 && (
                          <p className="field-help">
                            Logical catalogs in project: {logicalCatalogs.map((catalog) => catalog.name).join(', ')}
                          </p>
                        )}
                      </div>
                    </div>

                    {/* Environment Details */}
                    <div className="isolation-section">
                      <h4>Settings</h4>
                      <div className="info-grid">
                        <div className="info-row">
                          <span className="info-label">Allow drift</span>
                          <span className="info-value">{envConfig.allowDrift ? 'Yes' : 'No'}</span>
                        </div>
                        <div className="info-row">
                          <span className="info-label">Require snapshot</span>
                          <span className="info-value">{envConfig.requireSnapshot ? 'Yes' : 'No'}</span>
                        </div>
                      </div>
                    </div>

                    {/* Deployment scope (managed categories) */}
                    <div className="isolation-section">
                      <h4>Deployment scope</h4>
                      <p className="help-text">
                        What SchemaX manages in this environment. Governance only = comments, tags,
                        grants, row filters, column masks (no CREATE catalog/schema/table/volume/function/materialized view).
                      </p>
                      <div className="managed-categories-list">
                        {ALL_MANAGED_CATEGORIES.map(({ id, label }) => {
                          const effective = envConfig.managedCategories ?? ALL_MANAGED_CATEGORIES.map((c) => c.id);
                          const checked = effective.includes(id);
                          return (
                            <label key={id} className="managed-category-checkbox">
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={() => toggleManagedCategory(envName, id)}
                              />
                              <span>{label}</span>
                            </label>
                          );
                        })}
                      </div>
                    </div>

                    {/* Existing objects (skip CREATE) */}
                    <div className="isolation-section">
                      <h4>Existing objects</h4>
                      <p className="help-text">
                        Objects that already exist; SchemaX will not emit CREATE for these. Use when
                        catalogs are created outside SchemaX.
                      </p>
                      <div className="modal-field" style={{ marginTop: '8px' }}>
                        <label>Catalogs (logical names, comma or newline)</label>
                        <textarea
                          className="import-bindings-textarea"
                          rows={2}
                          value={(envConfig.existingObjects?.catalog ?? []).join(', ')}
                          onChange={(e) => updateExistingCatalogs(envName, e.target.value)}
                          placeholder="analytics, main"
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        <div className="modal-footer">
          <VSCodeButton appearance="primary" onClick={handleSave} disabled={!isDirty || hasMappingErrors}>
            Save Changes
          </VSCodeButton>
          <VSCodeButton appearance="secondary" onClick={handleCancel}>
            Cancel
          </VSCodeButton>
        </div>

        {/* Location Add/Edit Modal */}
        {showLocationModal && locationModalData && (
          <div className="modal-overlay" onClick={() => setShowLocationModal(false)}>
            <div className="modal-content location-modal" onClick={(e) => e.stopPropagation()}>
              <h3>
                {locationModalData.mode === 'add' ? 'Add' : 'Edit'}{' '}
                {locationModalData.type === 'managed' ? 'Managed' : 'External'} Location
              </h3>

              <div className="modal-field">
                <label>Location Name *</label>
                <VSCodeTextField
                  value={locationModalData.name}
                  placeholder="location_name"
                  disabled={locationModalData.mode === 'edit'}
                  onInput={(e) => {
                    const value = (e.target as HTMLInputElement).value;
                    setLocationModalData({ ...locationModalData, name: value });
                    if (!value.trim()) {
                      setLocationNameError(null);
                    } else if (!/^[a-z][a-z0-9_]*$/.test(value)) {
                      setLocationNameError('Use only lowercase letters, numbers, and underscores (e.g. my_location). Hyphens are not allowed.');
                    } else {
                      setLocationNameError(null);
                    }
                  }}
                />
                {locationNameError && (
                  <p className="field-error" style={{ color: 'var(--vscode-errorForeground)', marginTop: '4px', fontSize: '12px' }}>
                    {locationNameError}
                  </p>
                )}
                <p className="field-help">Lowercase with underscores. Must be unique across all locations.</p>
              </div>

              <div className="modal-field">
                <label>Description (optional)</label>
                <VSCodeTextField
                  value={locationModalData.description}
                  placeholder="Describe this location's purpose"
                  onInput={(e) => setLocationModalData({
                    ...locationModalData,
                    description: (e.target as HTMLInputElement).value
                  })}
                />
              </div>

              <div className="modal-field">
                <label>Environment Paths *</label>
                <p className="field-help">Configure path for each environment. At least one path is required.</p>
                
                <div className="env-paths-list">
                  {environmentNames.map(envName => (
                    <div key={envName} className="env-path-item">
                      <label className="env-path-label">{envName}</label>
                      <VSCodeTextField
                        value={locationModalData.paths[envName] || ''}
                        placeholder={`s3://bucket-${envName}/path`}
                        onInput={(e) => setLocationModalData({
                          ...locationModalData,
                          paths: {
                            ...locationModalData.paths,
                            [envName]: (e.target as HTMLInputElement).value
                          }
                        })}
                      />
                    </div>
                  ))}
                </div>
                
                <p className="field-help">Valid URI schemes: s3://, abfss://, gs://, dbfs://</p>
              </div>

              <div className="modal-actions">
                <VSCodeButton onClick={saveLocation}>
                  {locationModalData.mode === 'add' ? 'Add' : 'Save'} Location
                </VSCodeButton>
                <VSCodeButton appearance="secondary" onClick={() => setShowLocationModal(false)}>
                  Cancel
                </VSCodeButton>
              </div>
            </div>
          </div>
        )}

        {/* Delete Confirmation Modal */}
        {deleteConfirmation && (
          <div className="modal-overlay" onClick={() => setDeleteConfirmation(null)}>
            <div className="modal-content delete-confirmation" onClick={(e) => e.stopPropagation()}>
              <h3>Delete {deleteConfirmation.type === 'managed' ? 'Managed' : 'External'} Location</h3>
              <p>
                Are you sure you want to delete <strong>{deleteConfirmation.name}</strong>?
              </p>
              <p className="warning-text">
                ⚠️ This location may be referenced by catalogs, schemas, or tables. Deleting it will cause SQL generation errors.
              </p>
              <div className="modal-actions">
                <VSCodeButton onClick={deleteLocation}>Delete</VSCodeButton>
                <VSCodeButton appearance="secondary" onClick={() => setDeleteConfirmation(null)}>
                  Cancel
                </VSCodeButton>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
