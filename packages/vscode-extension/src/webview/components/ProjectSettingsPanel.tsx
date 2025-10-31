import React, { useState, useEffect } from 'react';
import { VSCodeButton, VSCodeTextField, VSCodeDropdown, VSCodeOption } from '@vscode/webview-ui-toolkit/react';
import { ProjectFile } from '../providers/unity/models';
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

interface EnvironmentConfig {
  topLevelName: string;
  description?: string;
  allowDrift: boolean;
  requireSnapshot: boolean;
  requireApproval?: boolean;
  autoCreateTopLevel: boolean;
  autoCreateSchematicSchema: boolean;
}

interface LocationModalData {
  type: 'managed' | 'external';
  mode: 'add' | 'edit';
  name: string;
  description: string;
  paths: Record<string, string>; // env -> path
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

  const activeEnv = project.activeEnvironment || 'dev';
  const environments = editedProject.provider?.environments || {};
  const environmentNames = Object.keys(environments);

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

  const cancelCatalogEdit = () => {
    setEditingCatalog(null);
    setEditCatalogValue('');
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
      alert('Location name is required');
      return;
    }

    if (!/^[a-z][a-z0-9_]*$/.test(name)) {
      alert('Location name must start with lowercase letter and contain only lowercase letters, numbers, and underscores');
      return;
    }

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

            {Object.keys(editedProject.managedLocations || {}).length > 0 ? (
              <div className="location-list">
                {Object.entries(editedProject.managedLocations || {}).map(([name, location]) => (
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

            {Object.keys(editedProject.externalLocations || {}).length > 0 ? (
              <div className="location-list">
                {Object.entries(editedProject.externalLocations || {}).map(([name, location]) => (
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
                      <p className="help-text">Physical catalog name for this environment</p>

                      {editingCatalog === envName ? (
                        <div className="inline-edit">
                          <VSCodeTextField
                            value={editCatalogValue}
                            onInput={(e: any) => setEditCatalogValue(e.target.value)}
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
                          <span className="label">Catalog:</span>
                          <code>{envConfig.topLevelName}</code>
                          <VSCodeButton appearance="icon" onClick={() => startEditCatalog(envName)}>
                            ✏️
                          </VSCodeButton>
                        </div>
                      )}
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
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        <div className="modal-footer">
          <VSCodeButton appearance="primary" onClick={handleSave} disabled={!isDirty}>
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
                  onInput={(e: any) => setLocationModalData({
                    ...locationModalData,
                    name: e.target.value
                  })}
                />
                <p className="field-help">Lowercase with underscores. Must be unique across all locations.</p>
              </div>

              <div className="modal-field">
                <label>Description (optional)</label>
                <VSCodeTextField
                  value={locationModalData.description}
                  placeholder="Describe this location's purpose"
                  onInput={(e: any) => setLocationModalData({
                    ...locationModalData,
                    description: e.target.value
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
                        onInput={(e: any) => setLocationModalData({
                          ...locationModalData,
                          paths: {
                            ...locationModalData.paths,
                            [envName]: e.target.value
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
