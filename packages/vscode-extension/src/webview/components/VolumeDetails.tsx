import React from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import './ViewDetails.css';

interface VolumeDetailsProps {
  volumeId: string;
}

export const VolumeDetails: React.FC<VolumeDetailsProps> = ({ volumeId }) => {
  const { findVolume, updateVolume, dropVolume } = useDesignerStore();
  const info = findVolume(volumeId);

  if (!info) {
    return (
      <div className="view-details">
        <div className="empty-state"><p>Volume not found</p></div>
      </div>
    );
  }

  const { catalog, schema, volume } = info;
  const vol = volume as { id: string; name: string; volumeType?: string; comment?: string; location?: string; grants?: any[] };

  return (
    <div className="view-details">
      <div className="view-header">
        <div className="view-title">
          <i className="codicon codicon-folder" aria-hidden="true" />
          <h2>{catalog.name}.{schema.name}.{vol.name}</h2>
        </div>
        <span className="view-badge" style={{ background: 'var(--vscode-charts-purple)' }}>VOLUME</span>
      </div>
      <div className="view-section">
        <h3>Properties</h3>
        <dl className="properties-list">
          <dt>Type</dt>
          <dd>{vol.volumeType === 'external' ? 'External' : 'Managed'}</dd>
          {vol.location && (
            <>
              <dt>Location</dt>
              <dd><code>{vol.location}</code></dd>
            </>
          )}
          {vol.comment != null && vol.comment !== '' && (
            <>
              <dt>Comment</dt>
              <dd>{vol.comment}</dd>
            </>
          )}
        </dl>
      </div>
      {vol.grants && vol.grants.length > 0 && (
        <div className="view-section">
          <h3>Grants</h3>
          <ul>{vol.grants.map((g: any, i: number) => (
            <li key={i}>{g.principal}: {Array.isArray(g.privileges) ? g.privileges.join(', ') : ''}</li>
          ))}</ul>
        </div>
      )}
    </div>
  );
};
