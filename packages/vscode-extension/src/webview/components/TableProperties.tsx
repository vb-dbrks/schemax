import React, { useState } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';
import type { UnityCatalog, UnityTable } from '../models/unity';

interface TablePropertiesProps {
  tableId: string;
}

// Reserved property keys that cannot be set directly
// Based on https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-tblproperties
const RESERVED_KEYS = ['external', 'location', 'owner', 'provider'];

export function TableProperties({ tableId }: TablePropertiesProps) {
  const { project, setTableProperty, unsetTableProperty } = useDesignerStore();
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editedKey, setEditedKey] = useState('');
  const [editedValue, setEditedValue] = useState('');
  const [isAdding, setIsAdding] = useState(false);
  const [newKey, setNewKey] = useState('');
  const [newValue, setNewValue] = useState('');
  const [error, setError] = useState('');
  const [deleteDialog, setDeleteDialog] = useState<string | null>(null);

  // Find the table
  const table = React.useMemo(() => {
    if (!project?.state?.catalogs) return null;
    for (const catalog of project.state.catalogs as UnityCatalog[]) {
      for (const schema of catalog.schemas) {
        const found = schema.tables.find((t: UnityTable) => t.id === tableId);
        if (found) return found;
      }
    }
    return null;
  }, [project, tableId]);

  if (!table) return null;

  const properties = table.properties || {};
  const propertyEntries = Object.entries(properties);

  const validateKey = (key: string): string | null => {
    if (!key.trim()) {
      return 'Property key cannot be empty';
    }
    if (RESERVED_KEYS.includes(key.toLowerCase())) {
      return `Cannot set reserved property: ${key}`;
    }
    if (key.startsWith('option.')) {
      return 'Property keys starting with "option." are not allowed';
    }
    return null;
  };

  const handleAddProperty = () => {
    const validationError = validateKey(newKey);
    if (validationError) {
      setError(validationError);
      return;
    }
    if (!newValue.trim()) {
      setError('Property value cannot be empty');
      return;
    }
    if (properties[newKey]) {
      setError(`Property "${newKey}" already exists`);
      return;
    }

    setTableProperty(tableId, newKey, newValue);
    setNewKey('');
    setNewValue('');
    setIsAdding(false);
    setError('');
  };

  const handleStartEdit = (key: string, value: string) => {
    setEditingKey(key);
    setEditedKey(key);
    setEditedValue(value);
    setError('');
  };

  const handleSaveEdit = (originalKey: string) => {
    const validationError = validateKey(editedKey);
    if (validationError) {
      setError(validationError);
      return;
    }
    if (!editedValue.trim()) {
      setError('Property value cannot be empty');
      return;
    }

    // If key changed, delete old and add new
    if (editedKey !== originalKey) {
      if (properties[editedKey]) {
        setError(`Property "${editedKey}" already exists`);
        return;
      }
      unsetTableProperty(tableId, originalKey);
      setTableProperty(tableId, editedKey, editedValue);
    } else {
      // Just update value
      setTableProperty(tableId, editedKey, editedValue);
    }

    setEditingKey(null);
    setError('');
  };

  const handleCancelEdit = () => {
    setEditingKey(null);
    setError('');
  };

  const handleDeleteProperty = (key: string) => {
    setDeleteDialog(key);
  };

  const confirmDelete = () => {
    if (deleteDialog) {
      unsetTableProperty(tableId, deleteDialog);
      setDeleteDialog(null);
    }
  };

  const handleCancelAdd = () => {
    setIsAdding(false);
    setNewKey('');
    setNewValue('');
    setError('');
  };

  return (
    <div className="table-properties-section">
      <h3>Table Properties (TBLPROPERTIES)</h3>
      
      {error && <div className="error-message">{error}</div>}
      
      {propertyEntries.length === 0 && !isAdding ? (
        <div className="empty-properties">
          <p>No table properties defined</p>
          <p className="hint">Table properties are key-value pairs for metadata and Delta Lake configuration</p>
        </div>
      ) : (
        <table className="properties-table">
          <thead>
            <tr>
              <th>Property Key</th>
              <th>Value</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {propertyEntries.map(([key, value]) => {
              const isEditing = editingKey === key;
              return (
                <tr key={key} className={isEditing ? 'editing' : ''}>
                  {isEditing ? (
                    <>
                      <td>
                        <input
                          type="text"
                          value={editedKey}
                          onChange={(e) => setEditedKey(e.target.value)}
                          autoFocus
                        />
                      </td>
                      <td>
                        <input
                          type="text"
                          value={editedValue}
                          onChange={(e) => setEditedValue(e.target.value)}
                        />
                      </td>
                      <td className="actions-cell">
                        <button onClick={() => handleSaveEdit(key)}>✓ Save</button>
                        <button onClick={handleCancelEdit}>✕ Cancel</button>
                      </td>
                    </>
                  ) : (
                    <>
                      <td><code>{key}</code></td>
                      <td>{String(value)}</td>
                      <td className="actions-cell">
                        <button onClick={() => handleStartEdit(key, String(value))}>✏️ Edit</button>
                        <button 
                          onClick={() => handleDeleteProperty(key)}
                          style={{ color: 'var(--vscode-errorForeground)' }}
                        >
                          🗑️ Delete
                        </button>
                      </td>
                    </>
                  )}
                </tr>
              );
            })}
            
            {isAdding && (
              <tr className="adding-row">
                <td>
                  <input
                    type="text"
                    value={newKey}
                    onChange={(e) => setNewKey(e.target.value)}
                    placeholder="e.g., delta.appendOnly"
                    autoFocus
                  />
                </td>
                <td>
                  <input
                    type="text"
                    value={newValue}
                    onChange={(e) => setNewValue(e.target.value)}
                    placeholder="e.g., true"
                  />
                </td>
                <td className="actions-cell">
                  <button onClick={handleAddProperty}>✓ Add</button>
                  <button onClick={handleCancelAdd}>✕ Cancel</button>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
      
      {!isAdding && (
        <button 
          className="add-property-btn"
          onClick={() => setIsAdding(true)}
        >
          + Add Property
        </button>
      )}
      
      <div className="properties-help">
        <details>
          <summary>Common Delta Lake Properties</summary>
          <ul>
            <li><code>delta.appendOnly</code> - Set to <code>true</code> to disable UPDATE and DELETE operations</li>
            <li><code>delta.dataSkippingNumIndexedCols</code> - Number of leading columns for statistics (integer)</li>
            <li><code>delta.deletedFileRetentionDuration</code> - Retention for VACUUM, e.g., <code>interval 7 days</code></li>
            <li><code>delta.logRetentionDuration</code> - History retention for time travel, e.g., <code>interval 30 days</code></li>
            <li><code>delta.enableChangeDataFeed</code> - Enable CDC (Change Data Feed)</li>
            <li><code>delta.columnMapping.mode</code> - Column mapping mode: <code>name</code> or <code>id</code></li>
          </ul>
          <p>
            <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-tblproperties" target="_blank">
              View full documentation →
            </a>
          </p>
        </details>
      </div>

      {deleteDialog && (
        <div className="modal" onClick={() => setDeleteDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Delete Property</h3>
            <p>Are you sure you want to delete property <code>{deleteDialog}</code>?</p>
            <p className="warning-text">This will generate an UNSET TBLPROPERTIES operation.</p>
            <div className="modal-buttons">
              <button onClick={confirmDelete} style={{ backgroundColor: 'var(--vscode-errorForeground)' }}>
                Delete
              </button>
              <button onClick={() => setDeleteDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
