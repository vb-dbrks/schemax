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
    return <div className="right-panel empty">Select a catalog to view details</div>;
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

  return (
    <div className="right-panel">
      <div className="panel-header">
        <h2>Catalog Details</h2>
        {hasChanges && (
          <VSCodeButton onClick={handleSaveChanges}>
            Save Changes
          </VSCodeButton>
        )}
      </div>

      <div className="panel-content">
        {/* Catalog Name (Read-only) */}
        <div className="field-group">
          <label htmlFor="catalog-name">Catalog Name</label>
          <VSCodeTextField
            id="catalog-name"
            value={catalog.name}
            readOnly
            disabled
          />
          <p className="field-help">Catalog name cannot be changed after creation</p>
        </div>

        {/* Comment */}
        <div className="field-group">
          <label htmlFor="catalog-comment">Comment</label>
          <VSCodeTextField
            id="catalog-comment"
            value={comment}
            placeholder="Enter catalog description"
            onInput={(e: Event) => {
              const target = e.target as HTMLInputElement;
              setComment(target.value);
            }}
          />
          <p className="field-help">Optional description for this catalog</p>
        </div>

        {/* Managed Location */}
        <div className="field-group">
          <label htmlFor="catalog-location">
            Managed Location Path
            <span className="info-icon" title="Storage location for managed tables"> ℹ️</span>
          </label>
          <VSCodeTextField
            id="catalog-location"
            value={managedLocationName}
            placeholder="e.g., s3://bucket/catalog-data or abfss://container@account.dfs.core.windows.net/path"
            onInput={(e: Event) => {
              const target = e.target as HTMLInputElement;
              setManagedLocationName(target.value);
            }}
          />
          <p className="field-help">
            Storage path where Unity Catalog stores data for managed tables in this catalog
          </p>
        </div>

        {/* Tags */}
        <div className="field-group">
          <label>
            Tags
            <span className="info-icon" title="Key-value pairs for metadata and governance"> ℹ️</span>
          </label>

          {/* Tag input form */}
          <div style={{ display: 'flex', gap: '8px', marginBottom: '8px' }}>
            <input
              type="text"
              placeholder="Tag name"
              value={tagInput.tagName}
              style={{ flex: '1' }}
              onInput={(e: React.FormEvent<HTMLInputElement>) => {
                setTagInput({ ...tagInput, tagName: (e.target as HTMLInputElement).value });
              }}
              onKeyDown={(e: React.KeyboardEvent) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  handleAddTag();
                }
              }}
            />
            <input
              type="text"
              placeholder="Tag value"
              value={tagInput.tagValue}
              style={{ flex: '1' }}
              onInput={(e: React.FormEvent<HTMLInputElement>) => {
                setTagInput({ ...tagInput, tagValue: (e.target as HTMLInputElement).value });
              }}
              onKeyDown={(e: React.KeyboardEvent) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  handleAddTag();
                }
              }}
            />
            <button
              type="button"
              onClick={handleAddTag}
              style={{ padding: '0 12px', whiteSpace: 'nowrap' }}
            >
              Add Tag
            </button>
          </div>

          {/* Display tags */}
          {Object.keys(tags).length > 0 ? (
            <div
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: '4px',
                padding: '8px',
                background: 'var(--vscode-editor-background)',
                border: '1px solid var(--vscode-input-border)',
                borderRadius: '2px',
              }}
            >
              {Object.entries(tags).map(([tagName, tagValue]) => (
                <span
                  key={tagName}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '4px',
                    padding: '2px 6px',
                    background: 'var(--vscode-badge-background)',
                    color: 'var(--vscode-badge-foreground)',
                    borderRadius: '2px',
                    fontSize: '11px',
                  }}
                >
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
                      marginLeft: '2px',
                      fontSize: '12px',
                      lineHeight: '1',
                    }}
                    title="Remove tag"
                  >
                    ×
                  </button>
                </span>
              ))}
            </div>
          ) : (
            <p className="field-help" style={{ fontStyle: 'italic', color: 'var(--vscode-descriptionForeground)' }}>
              No tags defined
            </p>
          )}
        </div>
      </div>
    </div>
  );
};

