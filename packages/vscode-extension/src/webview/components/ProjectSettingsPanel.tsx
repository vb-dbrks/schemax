import React from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { getVsCodeApi } from '../vscode-api';
import { EnvironmentSummary } from './EnvironmentSummary';

interface OverviewFieldProps {
  label: string;
  value: string;
}

const OverviewField: React.FC<OverviewFieldProps> = ({ label, value }) => (
  <div className="project-settings__field">
    <span className="project-settings__field-label">{label}</span>
    <span className="project-settings__field-value">{value}</span>
  </div>
);

export const ProjectSettingsPanel: React.FC = () => {
  const { project } = useDesignerStore();
  const vscode = React.useMemo(() => {
    try {
      return getVsCodeApi();
    } catch {
      return null;
    }
  }, []);

  if (!project) {
    return null;
  }

  const providerInfo = `${project.provider?.type ?? 'n/a'}${project.provider?.version ? ` · v${project.provider.version}` : ''}`;
  const latestSnapshot = project.latestSnapshot ?? 'None yet';
  const snapshotCount = project.snapshots?.length ?? 0;
  const pendingOps = project.ops?.length ?? 0;
  const hasEnvironmentMappings = Boolean(project.provider?.environments && Object.keys(project.provider.environments).length > 0);
  const settings = project.settings ?? { autoIncrementVersion: false, versionPrefix: '' };

  return (
    <div className="project-settings">
      <header className="project-settings__header">
        <div>
          <h2>Project settings</h2>
          <p>Inspect project metadata, versioning preferences, and environment configuration.</p>
        </div>
        <VSCodeButton
          type="button"
          appearance="secondary"
          onClick={() =>
            vscode?.postMessage({ type: 'open-docs', payload: { path: 'docs/QUICKSTART.md', fragment: 'project-json' } })
          }
        >
          Docs
        </VSCodeButton>
      </header>

      <section className="project-settings__section" aria-label="Project overview">
        <div className="project-settings__card">
          <div className="project-settings__card-header">
            <div>
              <p className="project-settings__card-eyebrow">Project overview</p>
              <h3 className="project-settings__card-title">Core project metadata</h3>
              <p className="project-settings__card-description">Name, provider details, snapshot status, and pending changes.</p>
            </div>
          </div>
          <div className="project-settings__grid">
          <OverviewField label="Project name" value={project.name || 'Unnamed project'} />
          <OverviewField label="Provider" value={providerInfo} />
          <OverviewField label="Latest snapshot" value={latestSnapshot} />
          <OverviewField label="Snapshots" value={snapshotCount.toString()} />
          <OverviewField label="Pending changes" value={`${pendingOps} ${pendingOps === 1 ? 'op' : 'ops'}`} />
          <OverviewField label="Schema version" value={`v${project.version ?? '4'}`} />
          </div>
        </div>
      </section>

      <section className="project-settings__section" aria-label="Versioning preferences">
        <div className="project-settings__card">
          <div className="project-settings__card-header">
            <div>
              <p className="project-settings__card-eyebrow">Versioning preferences</p>
              <h3 className="project-settings__card-title">Snapshot numbering defaults</h3>
              <p className="project-settings__card-description">Controls automatic version bumps and prefix conventions.</p>
            </div>
          </div>
          <div className="project-settings__grid">
          <OverviewField
            label="Auto-increment snapshots"
            value={settings.autoIncrementVersion ? 'Enabled' : 'Disabled'}
          />
          <OverviewField
            label="Version prefix"
            value={settings.versionPrefix ? settings.versionPrefix : 'Not set'}
          />
          </div>
        </div>
      </section>

      {hasEnvironmentMappings && (
        <section className="project-settings__section" aria-label="Environment mapping">
          <EnvironmentSummary className="project-settings__environment-summary" />
        </section>
      )}
    </div>
  );
};


