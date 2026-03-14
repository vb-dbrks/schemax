import React, { useState } from 'react';
import { VSCodeButton, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';
import type { NamingStandardsConfig, NamingRule } from '../../models/unity';

interface NamingStandardsSettingsProps {
  config: NamingStandardsConfig | undefined;
  onChange: (config: NamingStandardsConfig) => void;
}

type ObjectTypeName = 'catalog' | 'schema' | 'table' | 'view' | 'column';

const OBJECT_TYPES: { key: ObjectTypeName; label: string }[] = [
  { key: 'catalog', label: 'Catalog' },
  { key: 'schema', label: 'Schema' },
  { key: 'table', label: 'Table' },
  { key: 'view', label: 'View' },
  { key: 'column', label: 'Column' },
];

interface TemplatePreset {
  label: string;
  description: string;
  rules: Partial<Record<ObjectTypeName, Pick<NamingRule, 'pattern' | 'description'>>>;
}

const TEMPLATES: TemplatePreset[] = [
  {
    label: 'Databricks Best Practices',
    description: 'Lowercase snake_case for all object types',
    rules: {
      catalog: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
      schema: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
      table: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
      view: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
      column: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
    },
  },
  {
    label: 'Data Warehouse Patterns',
    description: 'Prefixed tables (dim_, fact_, stg_, int_), snake_case others',
    rules: {
      catalog: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
      schema: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
      table: {
        pattern: '^(dim_|fact_|stg_|int_)[a-z0-9_]+$',
        description: 'Prefixed table names',
      },
      view: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
      column: { pattern: '^[a-z][a-z0-9_]*$', description: 'Lowercase snake_case' },
    },
  },
  {
    label: 'camelCase',
    description: 'camelCase identifiers for all types',
    rules: {
      catalog: { pattern: '^[a-z][a-zA-Z0-9]*$', description: 'camelCase' },
      schema: { pattern: '^[a-z][a-zA-Z0-9]*$', description: 'camelCase' },
      table: { pattern: '^[a-z][a-zA-Z0-9]*$', description: 'camelCase' },
      view: { pattern: '^[a-z][a-zA-Z0-9]*$', description: 'camelCase' },
      column: { pattern: '^[a-z][a-zA-Z0-9]*$', description: 'camelCase' },
    },
  },
  {
    label: 'PascalCase',
    description: 'PascalCase identifiers for all types',
    rules: {
      catalog: { pattern: '^[A-Z][a-zA-Z0-9]*$', description: 'PascalCase' },
      schema: { pattern: '^[A-Z][a-zA-Z0-9]*$', description: 'PascalCase' },
      table: { pattern: '^[A-Z][a-zA-Z0-9]*$', description: 'PascalCase' },
      view: { pattern: '^[A-Z][a-zA-Z0-9]*$', description: 'PascalCase' },
      column: { pattern: '^[A-Z][a-zA-Z0-9]*$', description: 'PascalCase' },
    },
  },
];

function emptyRule(): NamingRule {
  return { pattern: '', enabled: true, description: '' };
}

function applyTemplate(
  current: NamingStandardsConfig | undefined,
  template: TemplatePreset
): NamingStandardsConfig {
  const base: NamingStandardsConfig = current ?? { applyToRenames: false };
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

  const handleApplyToRenamesChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({ ...(config ?? { applyToRenames: false }), applyToRenames: e.target.checked });
  };

  const handleRuleChange = (
    key: ObjectTypeName,
    field: keyof NamingRule,
    value: string | boolean
  ) => {
    const currentRule: NamingRule = config?.[key] ?? emptyRule();
    const updatedRule: NamingRule = { ...currentRule, [field]: value };
    onChange({ ...(config ?? { applyToRenames: false }), [key]: updatedRule });
  };

  const handleEnableRule = (key: ObjectTypeName, enabled: boolean) => {
    if (!enabled && !config?.[key]) return;
    const currentRule: NamingRule = config?.[key] ?? emptyRule();
    onChange({ ...(config ?? { applyToRenames: false }), [key]: { ...currentRule, enabled } });
  };

  return (
    <div className="settings-section">
      <h3>Naming Standards</h3>
      <p className="section-description">
        Enforce naming conventions on catalog objects. Names are validated by the Python SDK at
        creation time; violations on add are blocked, and violations on rename show a warning.
      </p>

      <div className="naming-standards-header">
        <label className="naming-standards-checkbox">
          <input
            type="checkbox"
            checked={config?.applyToRenames ?? false}
            onChange={handleApplyToRenamesChange}
          />
          <span>Also enforce on renames (shows warning, not a hard block)</span>
        </label>

        <VSCodeButton
          appearance="secondary"
          onClick={() => setShowTemplates((v) => !v)}
          title="Apply a naming template to fill in patterns"
        >
          {showTemplates ? 'Hide Templates' : 'Load Template'}
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
            <div key={key} className={`naming-standards-rule ${enabled ? 'is-enabled' : ''}`}>
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
                      value={rule?.pattern ?? ''}
                      placeholder="e.g. ^[a-z][a-z0-9_]*$"
                      onInput={(e) =>
                        handleRuleChange(key, 'pattern', (e.target as HTMLInputElement).value)
                      }
                    />
                  </div>
                  <div className="naming-standards-field">
                    <label>Description (optional)</label>
                    <VSCodeTextField
                      value={rule?.description ?? ''}
                      placeholder="e.g. Lowercase snake_case"
                      onInput={(e) =>
                        handleRuleChange(key, 'description', (e.target as HTMLInputElement).value)
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
