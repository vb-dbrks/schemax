import React, { useState, useEffect } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import { VSCodeButton, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';

interface CatalogDetailsProps {
  catalogId: string;
}

export const CatalogDetails: React.FC<CatalogDetailsProps> = ({ catalogId }) => {
  const { project, findCatalog, updateCatalog } = useDesignerStore();
  const catalog = findCatalog(catalogId);

  const [comment, setComment] = useState(catalog?.comment || '');
  const [managedLocationName, setManagedLocationName] = useState(catalog?.managedLocationName || '');
  const [tags, setTags] = useState<Record<string, string>>(catalog?.tags || {});
  const [tagInput, setTagInput] = useState({ tagName: '', tagValue: '' });
  const [hasChanges, setHasChanges] = useState(false);
  const [copySuccess, setCopySuccess] = useState(false);

  // Update local state when catalog changes
  useEffect(() => {
    if (catalog) {
      setComment(catalog.comment || '');
      setManagedLocationName(catalog.managedLocationName || '');
      setTags(catalog.tags || {});
      setHasChanges(false);
    }
  }, [catalog]);

  // Detect changes
  useEffect(() => {
    if (catalog) {
      const commentChanged = comment !== (catalog.comment || '');
      const locationChanged = managedLocationName !== (catalog.managedLocationName || '');
      const tagsChanged = JSON.stringify(tags) !== JSON.stringify(catalog.tags || {});
      setHasChanges(commentChanged || locationChanged || tagsChanged);
    }
  }, [comment, managedLocationName, tags, catalog]);

  if (!catalog) {
    return (
      <div className="table-designer">
        <div className="empty-state">
          <h2>Catalog not found</h2>
          <p>The selected catalog could not be found.</p>
        </div>
      </div>
    );
  }

  const handleSaveChanges = () => {
    // Build the updates object
    const updates: any = {};
    
    if (managedLocationName !== (catalog.managedLocationName || '')) {
      updates.managedLocationName = managedLocationName || undefined;
    }
    
    if (comment !== (catalog.comment || '')) {
      updates.comment = comment || undefined;
    }
    
    if (JSON.stringify(tags) !== JSON.stringify(catalog.tags || {})) {
      updates.tags = tags;
    }
    
    // Apply all updates in a single operation
    if (Object.keys(updates).length > 0) {
      updateCatalog(catalogId, updates);
    }
    
    setHasChanges(false);
  };

  const handleAddTag = () => {
    if (tagInput.tagName && tagInput.tagValue) {
      setTags({ ...tags, [tagInput.tagName]: tagInput.tagValue });
      setTagInput({ tagName: '', tagValue: '' });
    }
  };

  const handleRemoveTag = (tagName: string) => {
    const newTags = { ...tags };
    delete newTags[tagName];
    setTags(newTags);
  };

  const handleCopyCatalogName = () => {
    const fullName = catalog.name;
    navigator.clipboard.writeText(fullName).then(() => {
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 2000);
    });
  };

  return (
    <div className="table-designer">
      <div className="table-header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <h2 style={{ marginBottom: 0 }}>{catalog.name}</h2>
            <button
              onClick={handleCopyCatalogName}
              title={copySuccess ? 'Copied!' : 'Copy catalog name'}
              style={{
                background: 'transparent',
                border: 'none',
                cursor: 'pointer',
                padding: '2px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: copySuccess ? 'var(--vscode-testing-iconPassed)' : 'var(--vscode-foreground)',
                opacity: copySuccess ? 1 : 0.6,
                height: '20px',
                width: '20px',
              }}
            >
              <i className={`codicon ${copySuccess ? 'codicon-check' : 'codicon-copy'}`} style={{ fontSize: '14px' }}></i>
            </button>
          </div>
          {hasChanges && (
            <VSCodeButton onClick={handleSaveChanges}>
              Save Changes
            </VSCodeButton>
          )}
        </div>
        <div className="table-metadata">
          <span className="badge">CATALOG</span>
        </div>
      </div>

      {/* Comment */}
      <div className="table-properties">
        <div className="property-row">
          <label>Comment</label>
          <div className="property-value">
            <VSCodeTextField
              value={comment}
              placeholder="Enter catalog description"
              style={{ width: '100%' }}
              onInput={(e: Event) => {
                const target = e.target as HTMLInputElement;
                setComment(target.value);
              }}
            />
          </div>
        </div>
      </div>

      {/* Managed Location */}
      <div className="table-properties">
        <div className="property-row">
          <label>
            Managed Location
            <span className="info-icon" title="Storage location for managed tables"> ℹ️</span>
          </label>
          <div className="property-value">
            <VSCodeTextField
              value={managedLocationName}
              placeholder="e.g., s3://bucket/catalog-data or abfss://..."
              style={{ width: '100%' }}
              onInput={(e: Event) => {
                const target = e.target as HTMLInputElement;
                setManagedLocationName(target.value);
              }}
            />
          </div>
        </div>
        <div style={{ fontSize: '11px', color: 'var(--vscode-descriptionForeground)', marginTop: '4px' }}>
          Storage path where Unity Catalog stores data for managed tables in this catalog
        </div>
      </div>

      {/* Tags */}
      <div className="table-properties">
        <h3 style={{ marginBottom: '12px', fontSize: '14px', fontWeight: 600 }}>
          Tags
          <span className="info-icon" title="Key-value pairs for metadata and governance"> ℹ️</span>
        </h3>
        
        {/* Tag input form */}
        <div style={{ display: 'flex', gap: '8px', marginBottom: '12px' }}>
          <VSCodeTextField
            placeholder="Tag name"
            value={tagInput.tagName}
            style={{ flex: '1' }}
            onInput={(e: Event) => {
              const target = e.target as HTMLInputElement;
              setTagInput({ ...tagInput, tagName: target.value });
            }}
            onKeyDown={(e: any) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                handleAddTag();
              }
            }}
          />
          <VSCodeTextField
            placeholder="Tag value"
            value={tagInput.tagValue}
            style={{ flex: '1' }}
            onInput={(e: Event) => {
              const target = e.target as HTMLInputElement;
              setTagInput({ ...tagInput, tagValue: target.value });
            }}
            onKeyDown={(e: any) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                handleAddTag();
              }
            }}
          />
          <VSCodeButton onClick={handleAddTag}>
            Add Tag
          </VSCodeButton>
        </div>

        {/* Display tags */}
        {Object.keys(tags).length > 0 ? (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
            {Object.entries(tags).map(([tagName, tagValue]) => (
              <span key={tagName} className="badge">
                <strong>{tagName}:</strong> {tagValue}
                <button
                  type="button"
                  onClick={() => handleRemoveTag(tagName)}
                  style={{
                    background: 'transparent',
                    border: 'none',
                    color: 'inherit',
                    cursor: 'pointer',
                    padding: '0 2px',
                    marginLeft: '4px',
                    fontSize: '12px',
                  }}
                  title="Remove tag"
                >
                  ×
                </button>
              </span>
            ))}
          </div>
        ) : (
          <div style={{ fontStyle: 'italic', color: 'var(--vscode-descriptionForeground)', fontSize: '12px' }}>
            No tags defined
          </div>
        )}
      </div>
    </div>
  );
};

