import React, { useState } from "react";
import { VSCodeButton, VSCodeTextField } from "@vscode/webview-ui-toolkit/react";
import type { NamingStandardsConfig, NamingRule } from "../../models/unity";

interface NamingStandardsSettingsProps {
  config: NamingStandardsConfig | undefined;
  onChange: (config: NamingStandardsConfig) => void;
}

type ObjectTypeName = "catalog" | "schema" | "table" | "view" | "column";

const OBJECT_TYPES: { key: ObjectTypeName; label: string }[] = [
  { key: "catalog", label: "Catalog" },
  { key: "schema", label: "Schema" },
  { key: "table", label: "Table" },
  { key: "view", label: "View" },
  { key: "column", label: "Column" },
];

interface TemplatePreset {
  label: string;
  description: string;
  rules: Partial<Record<ObjectTypeName, Pick<NamingRule, "pattern" | "description">>>;
}

const TEMPLATES: TemplatePreset[] = [
  {
    label: "Databricks Best Practices",
    description: "Lowercase snake_case for all object types",
    rules: {
      catalog: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
      schema: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
      table: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
      view: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
      column: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
    },
  },
  {
    label: "Data Warehouse Patterns",
    description: "Prefixed tables (dim_, fact_, stg_, int_), snake_case others",
    rules: {
      catalog: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
      schema: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
      table: {
        pattern: "^(dim_|fact_|stg_|int_)[a-z0-9_]+$",
        description: "Prefixed table names",
      },
      view: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
      column: { pattern: "^[a-z][a-z0-9_]*$", description: "Lowercase snake_case" },
    },
  },
];

function emptyRule(): NamingRule {
  return { pattern: "", enabled: true, description: "" };
}

function applyTemplate(
  current: NamingStandardsConfig | undefined,
  template: TemplatePreset
): NamingStandardsConfig {
  const base: NamingStandardsConfig = current ?? { applyToRenames: false, strictMode: false };
  const updated = { ...base };
  for (const { key } of OBJECT_TYPES) {
    const t = template.rules[key];
    if (t) {
      updated[key] = {
        pattern: t.pattern,
        description: t.description,
        enabled: true,
      };
    }
  }
  return updated;
}

export function NamingStandardsSettings({
  config,
  onChange,
}: NamingStandardsSettingsProps): React.ReactElement {
  const [showTemplates, setShowTemplates] = useState(false);

  const baseConfig = (): NamingStandardsConfig =>
    config ?? { applyToRenames: false, strictMode: false };

  const handleApplyToRenamesChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({ ...baseConfig(), applyToRenames: e.target.checked });
  };

  const handleStrictModeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({ ...baseConfig(), strictMode: e.target.checked });
  };

  const handleRuleChange = (
    key: ObjectTypeName,
    field: keyof NamingRule,
    value: string | boolean
  ) => {
    const currentRule: NamingRule = config?.[key] ?? emptyRule();
    const updatedRule: NamingRule = { ...currentRule, [field]: value };
    onChange({ ...baseConfig(), [key]: updatedRule });
  };

  const handleEnableRule = (key: ObjectTypeName, enabled: boolean) => {
    if (!enabled && !config?.[key]) return;
    const currentRule: NamingRule = config?.[key] ?? emptyRule();
    onChange({ ...baseConfig(), [key]: { ...currentRule, enabled } });
  };

  return (
    <div className="settings-section">
      <p className="naming-standards-info">
        Unity Catalog always stores and shows object names in lowercase, regardless of how they are
        defined here.
      </p>
      <p
        className="section-description"
        title="Violations on add are blocked; violations on rename show a warning."
      >
        Enforce naming conventions on catalog objects (validated by Python SDK at add/rename).
      </p>

      <div className="naming-standards-header">
        <div className="naming-standards-options">
          <label className="naming-standards-checkbox" title="Shows warning, not a hard block">
            <input
              type="checkbox"
              checked={config?.applyToRenames ?? false}
              onChange={handleApplyToRenamesChange}
            />
            <span>Enforce on renames (warning only)</span>
          </label>
          <div>
            <label
              className="naming-standards-checkbox"
              title="When ON: applies to both existing and new objects — validate/sql/apply fail and new non-compliant adds are blocked. When OFF: existing objects may violate (warnings only); new objects are still validated on add."
            >
              <input
                type="checkbox"
                checked={config?.strictMode ?? false}
                onChange={handleStrictModeChange}
              />
              <span>Strict mode</span>
            </label>
            <p className="naming-standards-hint">
              When on: existing and new must comply; CLI fails, non-compliant adds blocked.
            </p>
          </div>
        </div>
        <VSCodeButton
          appearance="secondary"
          onClick={() => setShowTemplates((v) => !v)}
          title="Apply a naming template to fill in patterns"
        >
          {showTemplates ? "Hide Templates" : "Load Template"}
        </VSCodeButton>
      </div>

      {showTemplates && (
        <div className="naming-standards-templates">
          {TEMPLATES.map((tpl) => (
            <div key={tpl.label} className="naming-standards-template-item">
              <div className="naming-standards-template-info">
                <strong>{tpl.label}</strong>
                <span>{tpl.description}</span>
              </div>
              <VSCodeButton
                appearance="secondary"
                onClick={() => {
                  onChange(applyTemplate(config, tpl));
                  setShowTemplates(false);
                }}
              >
                Apply
              </VSCodeButton>
            </div>
          ))}
        </div>
      )}

      <div className="naming-standards-rules">
        {OBJECT_TYPES.map(({ key, label }) => {
          const rule = config?.[key];
          const enabled = rule?.enabled ?? false;
          return (
            <div key={key} className={`naming-standards-rule ${enabled ? "is-enabled" : ""}`}>
              <div className="naming-standards-rule-header">
                <label className="naming-standards-checkbox">
                  <input
                    type="checkbox"
                    checked={enabled}
                    onChange={(e) => handleEnableRule(key, e.target.checked)}
                  />
                  <strong>{label}</strong>
                </label>
              </div>

              {enabled && (
                <div className="naming-standards-rule-fields">
                  <div className="naming-standards-field">
                    <label>Pattern (regex)</label>
                    <VSCodeTextField
                      value={rule?.pattern ?? ""}
                      placeholder="e.g. ^[a-z][a-z0-9_]*$"
                      onInput={(e) =>
                        handleRuleChange(key, "pattern", (e.target as HTMLInputElement).value)
                      }
                    />
                    {(rule?.pattern ?? "").trim() === "" && (
                      <p className="field-error" style={{ marginTop: 4, fontSize: 12 }}>
                        Pattern is required.
                      </p>
                    )}
                  </div>
                  <div className="naming-standards-field">
                    <label>Description (optional)</label>
                    <VSCodeTextField
                      value={rule?.description ?? ""}
                      placeholder="e.g. Lowercase snake_case"
                      onInput={(e) =>
                        handleRuleChange(key, "description", (e.target as HTMLInputElement).value)
                      }
                    />
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
