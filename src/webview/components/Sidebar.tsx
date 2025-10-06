import React, { useState } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';

export const Sidebar: React.FC = () => {
  const {
    project,
    selectedCatalogId,
    selectedSchemaId,
    selectedTableId,
    selectCatalog,
    selectSchema,
    selectTable,
    renameCatalog,
    renameSchema,
    renameTable,
    dropCatalog,
    dropSchema,
    dropTable,
  } = useDesignerStore();

  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());

  const toggleCatalog = (catalogId: string) => {
    const newExpanded = new Set(expandedCatalogs);
    if (newExpanded.has(catalogId)) {
      newExpanded.delete(catalogId);
    } else {
      newExpanded.add(catalogId);
    }
    setExpandedCatalogs(newExpanded);
  };

  const toggleSchema = (schemaId: string) => {
    const newExpanded = new Set(expandedSchemas);
    if (newExpanded.has(schemaId)) {
      newExpanded.delete(schemaId);
    } else {
      newExpanded.add(schemaId);
    }
    setExpandedSchemas(newExpanded);
  };

  const [renameDialog, setRenameDialog] = useState<{type: 'catalog'|'schema'|'table', id: string, name: string} | null>(null);
  const [dropDialog, setDropDialog] = useState<{type: 'catalog'|'schema'|'table', id: string, name: string} | null>(null);

  const handleRenameCatalog = (catalogId: string, currentName: string) => {
    setRenameDialog({type: 'catalog', id: catalogId, name: currentName});
  };

  const handleRenameSchema = (schemaId: string, currentName: string) => {
    setRenameDialog({type: 'schema', id: schemaId, name: currentName});
  };

  const handleRenameTable = (tableId: string, currentName: string) => {
    setRenameDialog({type: 'table', id: tableId, name: currentName});
  };

  const handleDropCatalog = (catalogId: string, name: string) => {
    setDropDialog({type: 'catalog', id: catalogId, name});
  };

  const handleDropSchema = (schemaId: string, name: string) => {
    setDropDialog({type: 'schema', id: schemaId, name});
  };

  const handleDropTable = (tableId: string, name: string) => {
    setDropDialog({type: 'table', id: tableId, name});
  };

  const handleRenameConfirm = (newName: string) => {
    if (!renameDialog || !newName || newName === renameDialog.name) {
      setRenameDialog(null);
      return;
    }
    
    if (renameDialog.type === 'catalog') {
      renameCatalog(renameDialog.id, newName);
    } else if (renameDialog.type === 'schema') {
      renameSchema(renameDialog.id, newName);
    } else if (renameDialog.type === 'table') {
      renameTable(renameDialog.id, newName);
    }
    setRenameDialog(null);
  };

  const handleDropConfirm = () => {
    if (!dropDialog) return;
    
    if (dropDialog.type === 'catalog') {
      dropCatalog(dropDialog.id);
    } else if (dropDialog.type === 'schema') {
      dropSchema(dropDialog.id);
    } else if (dropDialog.type === 'table') {
      dropTable(dropDialog.id);
    }
    setDropDialog(null);
  };

  if (!project) {
    return <div className="sidebar">Loading...</div>;
  }

  return (
    <div className="sidebar">
      <h2>Unity Catalog</h2>
      <div className="tree">
        {project.state.catalogs.map((catalog) => (
          <div key={catalog.id} className="tree-node">
            <div
              className={`tree-item ${selectedCatalogId === catalog.id ? 'selected' : ''}`}
              onClick={() => {
                selectCatalog(catalog.id);
                selectSchema(null);
                selectTable(null);
              }}
            >
              <span className="expander" onClick={(e) => { e.stopPropagation(); toggleCatalog(catalog.id); }}>
                {expandedCatalogs.has(catalog.id) ? '▼' : '▶'}
              </span>
              <span className="icon">📁</span>
              <span className="name">{catalog.name}</span>
              <span className="actions">
                <button onClick={(e) => { e.stopPropagation(); handleRenameCatalog(catalog.id, catalog.name); }}>✏️</button>
                <button onClick={(e) => { e.stopPropagation(); handleDropCatalog(catalog.id, catalog.name); }}>🗑️</button>
              </span>
            </div>
            {expandedCatalogs.has(catalog.id) && (
              <div className="tree-children">
                {catalog.schemas.map((schema) => (
                  <div key={schema.id} className="tree-node">
                    <div
                      className={`tree-item ${selectedSchemaId === schema.id ? 'selected' : ''}`}
                      onClick={() => {
                        selectCatalog(catalog.id);
                        selectSchema(schema.id);
                        selectTable(null);
                      }}
                    >
                      <span className="expander" onClick={(e) => { e.stopPropagation(); toggleSchema(schema.id); }}>
                        {expandedSchemas.has(schema.id) ? '▼' : '▶'}
                      </span>
                      <span className="icon">📂</span>
                      <span className="name">{schema.name}</span>
                      <span className="actions">
                        <button onClick={(e) => { e.stopPropagation(); handleRenameSchema(schema.id, schema.name); }}>✏️</button>
                        <button onClick={(e) => { e.stopPropagation(); handleDropSchema(schema.id, schema.name); }}>🗑️</button>
                      </span>
                    </div>
                    {expandedSchemas.has(schema.id) && (
                      <div className="tree-children">
                        {schema.tables.map((table) => (
                          <div
                            key={table.id}
                            className={`tree-item ${selectedTableId === table.id ? 'selected' : ''}`}
                            onClick={() => {
                              selectCatalog(catalog.id);
                              selectSchema(schema.id);
                              selectTable(table.id);
                            }}
                          >
                            <span className="icon">📊</span>
                            <span className="name">{table.name}</span>
                            <span className="badge">{table.format}</span>
                            <span className="actions">
                              <button onClick={(e) => { e.stopPropagation(); handleRenameTable(table.id, table.name); }}>✏️</button>
                              <button onClick={(e) => { e.stopPropagation(); handleDropTable(table.id, table.name); }}>🗑️</button>
                            </span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>

      {renameDialog && (
        <div className="modal" onClick={() => setRenameDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Rename {renameDialog.type}</h3>
            <input
              type="text"
              defaultValue={renameDialog.name}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  handleRenameConfirm((e.target as HTMLInputElement).value);
                } else if (e.key === 'Escape') {
                  setRenameDialog(null);
                }
              }}
              id="rename-input"
            />
            <div className="modal-buttons">
              <button onClick={() => {
                const input = document.getElementById('rename-input') as HTMLInputElement;
                handleRenameConfirm(input.value);
              }}>Rename</button>
              <button onClick={() => setRenameDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {dropDialog && (
        <div className="modal" onClick={() => setDropDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Confirm Drop</h3>
            <p>Are you sure you want to drop {dropDialog.type} "{dropDialog.name}"?</p>
            {dropDialog.type === 'catalog' && <p className="warning">⚠️ This will also drop all schemas and tables.</p>}
            {dropDialog.type === 'schema' && <p className="warning">⚠️ This will also drop all tables.</p>}
            <div className="modal-buttons">
              <button onClick={handleDropConfirm} style={{backgroundColor: 'var(--vscode-errorForeground)'}}>Drop</button>
              <button onClick={() => setDropDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

