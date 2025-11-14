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
    return (
      <div className="table-designer">
        <div className="empty-state">
          <h2>Schema not found</h2>
          <p>The selected schema could not be found.</p>
        </div>
      </div>
    );
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
    <div className="table-designer">
      <div className="table-header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h2>{catalog.name}.{schema.name}</h2>
          {hasChanges && (
            <VSCodeButton onClick={handleSaveChanges}>
              Save Changes
            </VSCodeButton>
          )}
        </div>
        <div className="table-metadata">
          <span className="badge">SCHEMA</span>
        </div>
      </div>

      {/* Comment */}
      <div className="table-properties">
        <div className="property-row">
          <label>Comment</label>
          <div className="property-value">
            <VSCodeTextField
              value={comment}
              placeholder="Enter schema description"
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
              placeholder="e.g., s3://bucket/schema-data or abfss://..."
              style={{ width: '100%' }}
              onInput={(e: Event) => {
                const target = e.target as HTMLInputElement;
                setManagedLocationName(target.value);
              }}
            />
          </div>
        </div>
        <div style={{ fontSize: '11px', color: 'var(--vscode-descriptionForeground)', marginTop: '4px' }}>
          Storage path where Unity Catalog stores data for managed tables in this schema
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

