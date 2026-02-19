import React from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { getVsCodeApi } from '../vscode-api';

interface EnvironmentConfig {
  topLevelName: string;
  catalogMappings?: Record<string, string>;
  description?: string;
  allowDrift?: boolean;
  requireSnapshot?: boolean;
  requireApproval?: boolean;
  autoCreateCatalog?: boolean;
  autoCreateSchemaxSchema?: boolean;
  [key: string]: unknown;
}

interface EnvironmentSummaryProps {
  className?: string;
}

export const EnvironmentSummary: React.FC<EnvironmentSummaryProps> = ({ className }) => {
  const { project } = useDesignerStore();
  const vscode = React.useMemo(() => {
    try {
      return getVsCodeApi();
    } catch {
      return null;
    }
  }, []);

  const environments = project?.provider?.environments;
  if (!environments || Object.keys(environments).length === 0) {
    return null;
  }

  const environmentEntries = Object.entries(environments as Record<string, EnvironmentConfig>);
  const containerClassName = ['environment-summary', className].filter(Boolean).join(' ');

  return (
    <section className={containerClassName} aria-label="Environment catalog mapping overview">
      <header className="environment-summary__header">
        <div>
          <p className="environment-summary__eyebrow">Environment mapping</p>
          <h3 className="environment-summary__title">Logical → Physical catalogs</h3>
        </div>
        <VSCodeButton
          appearance="secondary"
          type="button"
          className="environment-summary__cta"
          onClick={() => {
            vscode?.postMessage({
              type: 'open-docs',
              payload: {
                url: 'https://vb-dbrks.github.io/schemax-vscode/guide/environments-and-scope/'
              }
            });
          }}
        >
          Docs
        </VSCodeButton>
      </header>
      <div className="environment-summary__list">
        {environmentEntries.map(([envName, config]) => (
          <article key={envName} className="environment-chip" aria-label={`Environment ${envName}`}>
            <div className="environment-chip__top">
              <span className="environment-chip__name">{envName}</span>
              <span className="environment-chip__target" title="Tracking catalog">{config.topLevelName}</span>
            </div>
            <p className="environment-chip__description">
              {(config.catalogMappings && Object.keys(config.catalogMappings).length > 0)
                ? Object.entries(config.catalogMappings)
                    .map(([logical, physical]) => `${logical}→${physical}`)
                    .join(', ')
                : 'No catalog mappings configured'}
            </p>
            {config.description && (
              <p className="environment-chip__description">{config.description}</p>
            )}
            <div className="environment-chip__flags">
              <span className={`chip-flag ${config.allowDrift ? 'chip-flag--warn' : 'chip-flag--success'}`}>
                {config.allowDrift ? 'Drift allowed' : 'Drift locked'}
              </span>
              {config.requireSnapshot && <span className="chip-flag chip-flag--accent">Snapshot required</span>}
              {config.requireApproval && <span className="chip-flag chip-flag--info">Approval required</span>}
              {config.autoCreateCatalog && <span className="chip-flag chip-flag--neutral">Auto-create catalog</span>}
              {config.autoCreateSchemaxSchema && (
                <span className="chip-flag chip-flag--neutral">Auto-create schema</span>
              )}
            </div>
          </article>
        ))}
      </div>
    </section>
  );
};

