import React, { useState, useEffect } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import { VSCodeButton, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';

interface SchemaDetailsProps {
  schemaId: string;
}

export const SchemaDetails: React.FC<SchemaDetailsProps> = ({ schemaId }) => {
  const { project, findSchema, updateSchema } = useDesignerStore();
  const schemaInfo = findSchema(schemaId);
  const schema = schemaInfo?.schema;
  const catalog = schemaInfo?.catalog;

  const [comment, setComment] = useState(schema?.comment || '');
  const [managedLocationName, setManagedLocationName] = useState(schema?.managedLocationName || '');
  const [tags, setTags] = useState<Record<string, string>>(schema?.tags || {});
  const [tagInput, setTagInput] = useState({ tagName: '', tagValue: '' });
  const [hasChanges, setHasChanges] = useState(false);

  // Update local state when schema changes
  useEffect(() => {
    if (schema) {
      setComment(schema.comment || '');
      setManagedLocationName(schema.managedLocationName || '');
      setTags(schema.tags || {});
      setHasChanges(false);
    }
  }, [schema]);

  // Detect changes
  useEffect(() => {
    if (schema) {
      const commentChanged = comment !== (schema.comment || '');
      const locationChanged = managedLocationName !== (schema.managedLocationName || '');
      const tagsChanged = JSON.stringify(tags) !== JSON.stringify(schema.tags || {});
      setHasChanges(commentChanged || locationChanged || tagsChanged);
    }
  }, [comment, managedLocationName, tags, schema]);

  if (!schema || !catalog) {
    return <div className="right-panel empty">Select a schema to view details</div>;
  }

  const handleSaveChanges = () => {
    // Build the updates object
    const updates: any = {};
    
    if (managedLocationName !== (schema.managedLocationName || '')) {
      updates.managedLocationName = managedLocationName || undefined;
    }
    
    if (comment !== (schema.comment || '')) {
      updates.comment = comment || undefined;
    }
    
    if (JSON.stringify(tags) !== JSON.stringify(schema.tags || {})) {
      updates.tags = tags;
    }
    
    // Apply all updates in a single operation
    if (Object.keys(updates).length > 0) {
      updateSchema(schemaId, updates);
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
        <h2>Schema Details</h2>
        {hasChanges && (
          <VSCodeButton onClick={handleSaveChanges}>
            Save Changes
          </VSCodeButton>
        )}
      </div>

      <div className="panel-content">
        {/* Catalog Name (Read-only) */}
        <div className="field-group">
          <label htmlFor="schema-catalog">Catalog</label>
          <VSCodeTextField
            id="schema-catalog"
            value={catalog.name}
            readOnly
            disabled
          />
          <p className="field-help">Parent catalog (read-only)</p>
        </div>

        {/* Schema Name (Read-only) */}
        <div className="field-group">
          <label htmlFor="schema-name">Schema Name</label>
          <VSCodeTextField
            id="schema-name"
            value={schema.name}
            readOnly
            disabled
          />
          <p className="field-help">Schema name cannot be changed after creation</p>
        </div>

        {/* Comment */}
        <div className="field-group">
          <label htmlFor="schema-comment">Comment</label>
          <VSCodeTextField
            id="schema-comment"
            value={comment}
            placeholder="Enter schema description"
            onInput={(e: Event) => {
              const target = e.target as HTMLInputElement;
              setComment(target.value);
            }}
          />
          <p className="field-help">Optional description for this schema</p>
        </div>

        {/* Managed Location */}
        <div className="field-group">
          <label htmlFor="schema-location">
            Managed Location Path
            <span className="info-icon" title="Storage location for managed tables"> ℹ️</span>
          </label>
          <VSCodeTextField
            id="schema-location"
            value={managedLocationName}
            placeholder="e.g., s3://bucket/schema-data or abfss://container@account.dfs.core.windows.net/path"
            onInput={(e: Event) => {
              const target = e.target as HTMLInputElement;
              setManagedLocationName(target.value);
            }}
          />
          <p className="field-help">
            Storage path where Unity Catalog stores data for managed tables in this schema
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

