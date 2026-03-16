import React, { useState } from "react";
import { VSCodeButton, VSCodeTextField } from "@vscode/webview-ui-toolkit/react";
import type { NamingStandardsConfig, ProjectFile, TargetConfig } from "../models/unity";
import { getVsCodeApi } from "../vscode-api";
import { CollapsibleSection } from "./settings/CollapsibleSection";
import { NamingStandardsSettings } from "./settings/NamingStandardsSettings";
import { UnityTargetSettings } from "./settings/UnityTargetSettings";

const vscode = getVsCodeApi();

interface ProjectSettingsPanelProps {
  project: ProjectFile;
  onClose: () => void;
}

interface LocationDefinition {
  description?: string;
  paths: Record<string, string>;
}

interface LocationModalData {
  type: "managed" | "external";
  mode: "add" | "edit";
  name: string;
  description: string;
  paths: Record<string, string>;
}

type LocationMap = Record<string, LocationDefinition>;

export function ProjectSettingsPanel({ project, onClose }: ProjectSettingsPanelProps) {
  const [editedProject, setEditedProject] = useState<ProjectFile>(
    JSON.parse(JSON.stringify(project))
  );
  const [isDirty, setIsDirty] = useState(false);
  const [hasMappingErrors, setHasMappingErrors] = useState(false);

  // Target tab state
  const targetNames = Object.keys(editedProject.targets || {});
  const [activeTargetTab, setActiveTargetTab] = useState(
    editedProject.defaultTarget || targetNames[0] || "default"
  );

  // Section expand/collapse
  const [envOpen, setEnvOpen] = useState(true);
  const [namingOpen, setNamingOpen] = useState(true);
  const [physicalOpen, setPhysicalOpen] = useState(true);
  const [externalOpen, setExternalOpen] = useState(true);
  const allExpanded = envOpen && namingOpen && physicalOpen && externalOpen;
  const toggleAllSections = () => {
    const next = !allExpanded;
    setEnvOpen(next);
    setNamingOpen(next);
    setPhysicalOpen(next);
    setExternalOpen(next);
  };

  // Location modal state
  const [showLocationModal, setShowLocationModal] = useState(false);
  const [locationModalData, setLocationModalData] = useState<LocationModalData | null>(null);
  const [locationNameError, setLocationNameError] = useState<string | null>(null);
  const [deleteConfirmation, setDeleteConfirmation] = useState<{
    type: "managed" | "external";
    name: string;
  } | null>(null);

  const activeTargetConfig = editedProject.targets?.[activeTargetTab];
  const activeEnv = project.activeEnvironment || "dev";
  const logicalCatalogs = editedProject.state?.catalogs || [];

  // Derive environment names from active target for location modal
  const environmentNames = Object.keys(activeTargetConfig?.environments || {});

  const handleSave = () => {
    const naming = (editedProject as { settings?: { namingStandards?: Record<string, { pattern?: string; enabled?: boolean }> } })
      .settings?.namingStandards;
    if (naming) {
      const objectTypes = ["catalog", "schema", "table", "view", "column"] as const;
      for (const key of objectTypes) {
        const rule = naming[key];
        if (rule && (rule.pattern ?? "").trim() === "") {
          alert(`Naming rule for "${key}" has an empty pattern. Pattern is required.`);
          return;
        }
      }
    }
    vscode.postMessage({
      type: "update-project-config",
      payload: editedProject,
    });
    setIsDirty(false);
    onClose();
  };

  const handleCancel = () => {
    if (isDirty) {
      if (confirm("You have unsaved changes. Are you sure you want to cancel?")) {
        onClose();
      }
    } else {
      onClose();
    }
  };

  const handleTargetConfigChange = (targetName: string, updated: TargetConfig) => {
    const next = { ...editedProject };
    next.targets = { ...next.targets, [targetName]: updated };
    setEditedProject(next);
    setIsDirty(true);
  };

  // ── Location helpers ──────────────────────────────────────────────

  const openAddLocationModal = (type: "managed" | "external") => {
    const initialPaths: Record<string, string> = {};
    environmentNames.forEach((env) => {
      initialPaths[env] = "";
    });
    setLocationModalData({ type, mode: "add", name: "", description: "", paths: initialPaths });
    setLocationNameError(null);
    setShowLocationModal(true);
  };

  const openEditLocationModal = (type: "managed" | "external", name: string) => {
    const locations =
      type === "managed"
        ? editedProject.managedLocations || {}
        : editedProject.externalLocations || {};
    const location = locations[name];
    if (!location) return;

    const paths: Record<string, string> = { ...location.paths };
    environmentNames.forEach((env) => {
      if (!(env in paths)) paths[env] = "";
    });
    setLocationModalData({ type, mode: "edit", name, description: location.description || "", paths });
    setShowLocationModal(true);
  };

  const saveLocation = () => {
    if (!locationModalData) return;
    const { type, mode, name, description, paths } = locationModalData;

    if (!name.trim()) {
      setLocationNameError("Location name is required");
      return;
    }
    if (!/^[a-z][a-z0-9_]*$/.test(name)) {
      setLocationNameError(
        "Use only lowercase letters, numbers, and underscores (e.g. my_location). Hyphens are not allowed."
      );
      return;
    }
    setLocationNameError(null);

    const hasPath = Object.values(paths).some((p) => p.trim());
    if (!hasPath) {
      alert("At least one environment path must be configured");
      return;
    }

    if (mode === "add") {
      const existing =
        type === "managed" ? editedProject.managedLocations || {} : editedProject.externalLocations || {};
      if (name in existing) {
        alert(`A ${type} location with name "${name}" already exists`);
        return;
      }
    }

    const cleanedPaths: Record<string, string> = {};
    Object.entries(paths).forEach(([env, path]) => {
      if (path.trim()) cleanedPaths[env] = path.trim();
    });

    const newLocation: LocationDefinition = {
      description: description.trim() || undefined,
      paths: cleanedPaths,
    };

    const updated = { ...editedProject };
    if (type === "managed") {
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

  const confirmDeleteLocation = (type: "managed" | "external", name: string) => {
    setDeleteConfirmation({ type, name });
  };

  const deleteLocation = () => {
    if (!deleteConfirmation) return;
    const { type, name } = deleteConfirmation;
    const updated = { ...editedProject };
    if (type === "managed" && updated.managedLocations) {
      delete updated.managedLocations[name];
    } else if (type === "external" && updated.externalLocations) {
      delete updated.externalLocations[name];
    }
    setEditedProject(updated);
    setIsDirty(true);
    setDeleteConfirmation(null);
  };

  // ── Provider-specific panel renderer ──────────────────────────────

  function renderTargetSettings(tName: string, tConfig: TargetConfig) {
    switch (tConfig.type) {
      case "unity":
        return (
          <UnityTargetSettings
            targetConfig={tConfig}
            activeEnv={activeEnv}
            logicalCatalogs={logicalCatalogs}
            onTargetConfigChange={(updated) => handleTargetConfigChange(tName, updated)}
            onMappingErrorChange={setHasMappingErrors}
          />
        );
      default:
        return (
          <div className="settings-section">
            <h3>Provider: {tConfig.type}</h3>
            <p className="section-description">
              Settings for provider <strong>{tConfig.type}</strong> are not yet available in the UI.
            </p>
          </div>
        );
    }
  }

  // ── Render ─────────────────────────────────────────────────────────

  return (
    <div className="modal-overlay" onClick={handleCancel}>
      <div className="modal-content project-settings-panel" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>Project Settings</h2>
          <button className="close-btn" onClick={handleCancel}>
            ×
          </button>
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
                <span className="info-label">Latest snapshot</span>
                <span className="info-value">{editedProject.latestSnapshot || "None yet"}</span>
              </div>
              <div className="info-row">
                <span className="info-label">Snapshots</span>
                <span className="info-value">{editedProject.snapshots?.length || 0}</span>
              </div>
              <div className="info-row">
                <span className="info-label">Targets</span>
                <span className="info-value">{targetNames.length}</span>
              </div>
            </div>
          </div>

          {/* Sections toolbar */}
          <div className="sections-toolbar">
            <button
              type="button"
              className="sections-toggle-all-btn"
              onClick={toggleAllSections}
              title={allExpanded ? "Collapse all sections" : "Expand all sections"}
            >
              {allExpanded ? "−" : "+"}
            </button>
          </div>

          {/* Environment Configuration */}
          {targetNames.length > 0 && (
            <CollapsibleSection
              title="Environment Configuration"
              isOpen={envOpen}
              onToggle={() => setEnvOpen((v) => !v)}
            >
              <div className="target-tabs">
                {targetNames.map((tName) => {
                  const tCfg = editedProject.targets[tName];
                  const isActive = tName === activeTargetTab;
                  return (
                    <button
                      key={tName}
                      className={`target-tab ${isActive ? "target-tab--active" : ""}`}
                      onClick={() => setActiveTargetTab(tName)}
                    >
                      <span className="target-tab__name">{tName}</span>
                      <span className="target-tab__provider">{tCfg?.type} · v{tCfg?.version}</span>
                    </button>
                  );
                })}
              </div>

              {activeTargetConfig && renderTargetSettings(activeTargetTab, activeTargetConfig)}
            </CollapsibleSection>
          )}

          <CollapsibleSection
            title="Naming Standards"
            isOpen={namingOpen}
            onToggle={() => setNamingOpen((v) => !v)}
          >
            <NamingStandardsSettings
              config={editedProject.settings?.namingStandards as NamingStandardsConfig | undefined}
              onChange={(updated) => {
                setEditedProject({
                  ...editedProject,
                  settings: { ...editedProject.settings, namingStandards: updated },
                });
                setIsDirty(true);
              }}
            />
          </CollapsibleSection>

          <CollapsibleSection
            title="Physical Isolation (Managed Tables)"
            isOpen={physicalOpen}
            onToggle={() => setPhysicalOpen((v) => !v)}
          >
            <div className="settings-section">
              <p className="section-description">
              Configure storage locations for managed tables at the catalog or schema level. Define
              location names here and specify paths for each environment.
            </p>

            {Object.keys((editedProject.managedLocations as LocationMap) || {}).length > 0 ? (
              <div className="location-list">
                {Object.entries((editedProject.managedLocations as LocationMap) || {}).map(
                  ([name, location]) => (
                    <div key={name} className="location-item">
                      <div className="location-header">
                        <strong>{name}</strong>
                        <div className="location-actions">
                          <VSCodeButton
                            appearance="icon"
                            onClick={() => openEditLocationModal("managed", name)}
                          >
                            ✏️
                          </VSCodeButton>
                          <VSCodeButton
                            appearance="icon"
                            onClick={() => confirmDeleteLocation("managed", name)}
                          >
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
                  )
                )}
              </div>
            ) : (
              <div className="empty-state">
                No managed locations configured. Catalogs and schemas will use the provider's
                default storage.
              </div>
            )}

            <VSCodeButton onClick={() => openAddLocationModal("managed")}>
              + Add Managed Location
            </VSCodeButton>
            </div>
          </CollapsibleSection>

          <CollapsibleSection
            title="External Locations (External Tables)"
            isOpen={externalOpen}
            onToggle={() => setExternalOpen((v) => !v)}
          >
            <div className="settings-section">
              <p className="section-description">
              Configure storage locations for external tables. Define location names here and
              specify paths for each environment.
            </p>

            {Object.keys((editedProject.externalLocations as LocationMap) || {}).length > 0 ? (
              <div className="location-list">
                {Object.entries((editedProject.externalLocations as LocationMap) || {}).map(
                  ([name, location]) => (
                    <div key={name} className="location-item">
                      <div className="location-header">
                        <strong>{name}</strong>
                        <div className="location-actions">
                          <VSCodeButton
                            appearance="icon"
                            onClick={() => openEditLocationModal("external", name)}
                          >
                            ✏️
                          </VSCodeButton>
                          <VSCodeButton
                            appearance="icon"
                            onClick={() => confirmDeleteLocation("external", name)}
                          >
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
                  )
                )}
              </div>
            ) : (
              <div className="empty-state">No external locations configured.</div>
            )}

            <VSCodeButton onClick={() => openAddLocationModal("external")}>
              + Add External Location
            </VSCodeButton>
            </div>
          </CollapsibleSection>
        </div>

        <div className="modal-footer">
          <VSCodeButton
            appearance="primary"
            onClick={handleSave}
            disabled={!isDirty || hasMappingErrors}
          >
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
                {locationModalData.mode === "add" ? "Add" : "Edit"}{" "}
                {locationModalData.type === "managed" ? "Managed" : "External"} Location
              </h3>

              <div className="modal-field">
                <label>Location Name *</label>
                <VSCodeTextField
                  value={locationModalData.name}
                  placeholder="location_name"
                  disabled={locationModalData.mode === "edit"}
                  onInput={(e) => {
                    const value = (e.target as HTMLInputElement).value;
                    setLocationModalData({ ...locationModalData, name: value });
                    if (!value.trim()) {
                      setLocationNameError(null);
                    } else if (!/^[a-z][a-z0-9_]*$/.test(value)) {
                      setLocationNameError(
                        "Use only lowercase letters, numbers, and underscores (e.g. my_location). Hyphens are not allowed."
                      );
                    } else {
                      setLocationNameError(null);
                    }
                  }}
                />
                {locationNameError && (
                  <p
                    className="field-error"
                    style={{
                      color: "var(--vscode-errorForeground)",
                      marginTop: "4px",
                      fontSize: "12px",
                    }}
                  >
                    {locationNameError}
                  </p>
                )}
                <p className="field-help">
                  Lowercase with underscores. Must be unique across all locations.
                </p>
              </div>

              <div className="modal-field">
                <label>Description (optional)</label>
                <VSCodeTextField
                  value={locationModalData.description}
                  placeholder="Describe this location's purpose"
                  onInput={(e) =>
                    setLocationModalData({
                      ...locationModalData,
                      description: (e.target as HTMLInputElement).value,
                    })
                  }
                />
              </div>

              <div className="modal-field">
                <label>Environment Paths *</label>
                <p className="field-help">
                  Configure path for each environment. At least one path is required.
                </p>

                <div className="env-paths-list">
                  {environmentNames.map((envName) => (
                    <div key={envName} className="env-path-item">
                      <label className="env-path-label">{envName}</label>
                      <VSCodeTextField
                        value={locationModalData.paths[envName] || ""}
                        placeholder={`s3://bucket-${envName}/path`}
                        onInput={(e) =>
                          setLocationModalData({
                            ...locationModalData,
                            paths: {
                              ...locationModalData.paths,
                              [envName]: (e.target as HTMLInputElement).value,
                            },
                          })
                        }
                      />
                    </div>
                  ))}
                </div>

                <p className="field-help">Valid URI schemes: s3://, abfss://, gs://, dbfs://</p>
              </div>

              <div className="modal-actions">
                <VSCodeButton onClick={saveLocation}>
                  {locationModalData.mode === "add" ? "Add" : "Save"} Location
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
              <h3>
                Delete {deleteConfirmation.type === "managed" ? "Managed" : "External"} Location
              </h3>
              <p>
                Are you sure you want to delete <strong>{deleteConfirmation.name}</strong>?
              </p>
              <p className="warning-text">
                ⚠️ This location may be referenced by catalogs, schemas, or tables. Deleting it will
                cause SQL generation errors.
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
