import React, { useState, useEffect } from "react";
import { VSCodeButton, VSCodeTextField } from "@vscode/webview-ui-toolkit/react";
import type { TargetConfig } from "../../models/unity";

const ALL_MANAGED_CATEGORIES = [
  { id: "catalog_structure", label: "Catalog structure" },
  { id: "schema_structure", label: "Schema structure" },
  { id: "table_structure", label: "Table structure" },
  { id: "view_structure", label: "View structure" },
  { id: "volume_structure", label: "Volume structure" },
  { id: "function_structure", label: "Function structure" },
  { id: "materialized_view_structure", label: "Materialized view structure" },
  { id: "governance", label: "Governance" },
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

type EnvironmentMap = Record<string, EnvironmentConfig>;

interface UnityTargetSettingsProps {
  targetConfig: TargetConfig;
  activeEnv: string;
  logicalCatalogs: Array<{ name: string }>;
  onTargetConfigChange: (updated: TargetConfig) => void;
  /** Report whether catalog-mapping textareas have parse errors. */
  onMappingErrorChange: (hasErrors: boolean) => void;
}

function formatCatalogMappingsText(mappings?: Record<string, string>): string {
  if (!mappings) {
    return "";
  }
  return Object.entries(mappings)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([logical, physical]) => `${logical}=${physical}`)
    .join("\n");
}

function parseCatalogMappingsText(text: string): Record<string, string> {
  const mappings: Record<string, string> = {};
  const lines = text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    const separator = line.indexOf("=");
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

export function UnityTargetSettings({
  targetConfig,
  activeEnv,
  logicalCatalogs,
  onTargetConfigChange,
  onMappingErrorChange,
}: UnityTargetSettingsProps) {
  const environments: EnvironmentMap =
    (targetConfig.environments as EnvironmentMap) || {};
  const environmentNames = Object.keys(environments);

  const [expandedEnvs, setExpandedEnvs] = useState<Set<string>>(
    new Set([activeEnv])
  );
  const [editingCatalog, setEditingCatalog] = useState<string | null>(null);
  const [editCatalogValue, setEditCatalogValue] = useState("");
  const [mappingTextByEnv, setMappingTextByEnv] = useState<Record<string, string>>({});
  const [mappingErrorByEnv, setMappingErrorByEnv] = useState<Record<string, string>>({});

  useEffect(() => {
    const initialText: Record<string, string> = {};
    environmentNames.forEach((envName) => {
      initialText[envName] = formatCatalogMappingsText(environments[envName]?.catalogMappings);
    });
    setMappingTextByEnv(initialText);
    setMappingErrorByEnv({});
  }, [targetConfig.environments]);

  useEffect(() => {
    onMappingErrorChange(Object.values(mappingErrorByEnv).some(Boolean));
  }, [mappingErrorByEnv]);

  const toggleEnv = (envName: string) => {
    const next = new Set(expandedEnvs);
    if (next.has(envName)) {
      next.delete(envName);
    } else {
      next.add(envName);
    }
    setExpandedEnvs(next);
  };

  const emitUpdate = (envs: EnvironmentMap) => {
    onTargetConfigChange({ ...targetConfig, environments: envs });
  };

  const startEditCatalog = (envName: string) => {
    setEditingCatalog(envName);
    setEditCatalogValue(environments[envName]?.topLevelName || "");
  };

  const saveCatalogEdit = (envName: string) => {
    const envs = { ...environments };
    if (envs[envName]) {
      envs[envName] = { ...envs[envName], topLevelName: editCatalogValue };
      emitUpdate(envs);
    }
    setEditingCatalog(null);
  };

  const cancelCatalogEdit = () => {
    setEditingCatalog(null);
    setEditCatalogValue("");
  };

  const updateCatalogMappings = (envName: string, rawText: string) => {
    setMappingTextByEnv((cur) => ({ ...cur, [envName]: rawText }));
    try {
      const mappings = parseCatalogMappingsText(rawText);
      const envs = { ...environments };
      if (envs[envName]) {
        envs[envName] = { ...envs[envName], catalogMappings: mappings };
        emitUpdate(envs);
      }
      setMappingErrorByEnv((cur) => ({ ...cur, [envName]: "" }));
    } catch (error) {
      setMappingErrorByEnv((cur) => ({ ...cur, [envName]: String(error) }));
    }
  };

  const toggleManagedCategory = (envName: string, categoryId: string) => {
    const env = environments[envName];
    if (!env) return;
    const current = env.managedCategories ?? ALL_MANAGED_CATEGORIES.map((c) => c.id);
    const set = new Set(current);
    if (set.has(categoryId)) {
      set.delete(categoryId);
    } else {
      set.add(categoryId);
    }
    const next = [...set];
    const envs = { ...environments };
    envs[envName] = {
      ...env,
      managedCategories: next.length === ALL_MANAGED_CATEGORIES.length ? undefined : next,
    };
    emitUpdate(envs);
  };

  const updateExistingCatalogs = (envName: string, rawText: string) => {
    const env = environments[envName];
    if (!env) return;
    const list = rawText
      .split(/[\n,]/)
      .map((s) => s.trim())
      .filter(Boolean);
    const envs = { ...environments };
    envs[envName] = {
      ...env,
      existingObjects: {
        ...(env.existingObjects ?? {}),
        catalog: list.length > 0 ? list : undefined,
      },
    };
    emitUpdate(envs);
  };

  return (
    <>
      {/* Environment Configuration */}
      <div className="settings-section">
        <h3>Environment Configuration</h3>
        <p className="section-description">
          Configure catalog mappings (Logical Isolation) for each environment.
        </p>

        {Object.entries(environments).map(([envName, envConfig]) => (
          <div key={envName} className="environment-section">
            <div className="env-header" onClick={() => toggleEnv(envName)}>
              <span className="env-toggle">{expandedEnvs.has(envName) ? "▼" : "▶"}</span>
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
                        onInput={(e) =>
                          setEditCatalogValue((e.target as HTMLInputElement).value)
                        }
                        placeholder="physical_catalog_name"
                      />
                      <VSCodeButton onClick={() => saveCatalogEdit(envName)}>Save</VSCodeButton>
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
                  <div className="modal-field" style={{ marginTop: "10px" }}>
                    <label>Catalog mappings (logical=physical)</label>
                    <textarea
                      className="import-bindings-textarea"
                      value={mappingTextByEnv[envName] ?? ""}
                      onChange={(e) => updateCatalogMappings(envName, e.target.value)}
                      placeholder={"schemax_demo=dev_schemax_demo\nsamples=dev_samples"}
                    />
                    <p className="field-help">
                      One mapping per line. SQL/apply requires mappings for all logical
                      catalogs.
                    </p>
                    {mappingErrorByEnv[envName] && (
                      <p
                        className="field-error"
                        style={{
                          color: "var(--vscode-errorForeground)",
                          marginTop: "4px",
                          fontSize: "12px",
                        }}
                      >
                        {mappingErrorByEnv[envName]}
                      </p>
                    )}
                    {logicalCatalogs.length > 0 && (
                      <p className="field-help">
                        Logical catalogs in project:{" "}
                        {logicalCatalogs.map((catalog) => catalog.name).join(", ")}
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
                      <span className="info-value">{envConfig.allowDrift ? "Yes" : "No"}</span>
                    </div>
                    <div className="info-row">
                      <span className="info-label">Require snapshot</span>
                      <span className="info-value">
                        {envConfig.requireSnapshot ? "Yes" : "No"}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Deployment scope (managed categories) */}
                <div className="isolation-section">
                  <h4>Deployment scope</h4>
                  <p className="help-text">
                    What SchemaX manages in this environment. Governance only = comments, tags,
                    grants, row filters, column masks (no CREATE
                    catalog/schema/table/volume/function/materialized view).
                  </p>
                  <div className="managed-categories-list">
                    {ALL_MANAGED_CATEGORIES.map(({ id, label }) => {
                      const effective =
                        envConfig.managedCategories ?? ALL_MANAGED_CATEGORIES.map((c) => c.id);
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
                  <div className="modal-field" style={{ marginTop: "8px" }}>
                    <label>Catalogs (logical names, comma or newline)</label>
                    <textarea
                      className="import-bindings-textarea"
                      rows={2}
                      value={(envConfig.existingObjects?.catalog ?? []).join(", ")}
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
    </>
  );
}
