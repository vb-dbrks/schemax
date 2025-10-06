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
    addCatalog,
    addSchema,
    addTable,
    renameCatalog,
    renameSchema,
    renameTable,
    dropCatalog,
    dropSchema,
    dropTable,
  } = useDesignerStore();

  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [addDialog, setAddDialog] = useState<{type: 'catalog'|'schema'|'table', catalogId?: string, schemaId?: string} | null>(null);

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

  const handleAddConfirm = (name: string, format?: 'delta' | 'iceberg') => {
    if (!addDialog || !name) {
      setAddDialog(null);
      return;
    }
    
    if (addDialog.type === 'catalog') {
      addCatalog(name);
    } else if (addDialog.type === 'schema' && addDialog.catalogId) {
      addSchema(addDialog.catalogId, name);
      setExpandedCatalogs(new Set(expandedCatalogs).add(addDialog.catalogId));
    } else if (addDialog.type === 'table' && addDialog.schemaId) {
      addTable(addDialog.schemaId, name, format || 'delta');
      setExpandedSchemas(new Set(expandedSchemas).add(addDialog.schemaId));
    }
    setAddDialog(null);
  };

  if (!project) {
    return <div className="sidebar">Loading...</div>;
  }

  // Determine the context-aware add button based on selection
  const getAddButton = () => {
    if (selectedSchemaId) {
      // Schema selected - show "Add Table"
      return (
        <button 
          className="add-btn-inline" 
          onClick={() => setAddDialog({type: 'table', schemaId: selectedSchemaId})}
          title="Add Table"
        >
          + Table
        </button>
      );
    } else if (selectedCatalogId) {
      // Catalog selected - show "Add Schema"
      return (
        <button 
          className="add-btn-inline" 
          onClick={() => setAddDialog({type: 'schema', catalogId: selectedCatalogId})}
          title="Add Schema"
        >
          + Schema
        </button>
      );
    } else {
      // Nothing selected - show "Add Catalog"
      return (
        <button 
          className="add-btn-inline" 
          onClick={() => setAddDialog({type: 'catalog'})}
          title="Add Catalog"
        >
          + Catalog
        </button>
      );
    }
  };

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <h2>Unity Catalog</h2>
        {getAddButton()}
      </div>
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
              <span className="icon">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                  <path fillRule="evenodd" d="M14 .75a.75.75 0 0 0-.75-.75H4.5A2.5 2.5 0 0 0 2 2.5v10.75A2.75 2.75 0 0 0 4.75 16h8.5a.75.75 0 0 0 .75-.75zM3.5 4.792v8.458c0 .69.56 1.25 1.25 1.25h7.75V5h-8c-.356 0-.694-.074-1-.208m9-1.292v-2h-8a1 1 0 0 0 0 2z" clipRule="evenodd"/>
                </svg>
              </span>
              <span className="name">{catalog.name}</span>
              <span className="actions">
                <button onClick={(e) => { e.stopPropagation(); setAddDialog({type: 'schema', catalogId: catalog.id}); }} title="Add Schema">+</button>
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
                      <span className="icon">
                        <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                          <path fillRule="evenodd" d="M2.727 3.695c-.225.192-.227.298-.227.305s.002.113.227.305c.223.19.59.394 1.108.58C4.865 5.256 6.337 5.5 8 5.5s3.135-.244 4.165-.615c.519-.186.885-.39 1.108-.58.225-.192.227-.298.227-.305s-.002-.113-.227-.305c-.223-.19-.59-.394-1.108-.58C11.135 2.744 9.663 2.5 8 2.5s-3.135.244-4.165.615c-.519.186-.885.39-1.108.58M13.5 5.94a7 7 0 0 1-.826.358C11.442 6.74 9.789 7 8 7s-3.442-.26-4.673-.703a7 7 0 0 1-.827-.358V8c0 .007.002.113.227.305.223.19.59.394 1.108.58C4.865 9.256 6.337 9.5 8 9.5s3.135-.244 4.165-.615c.519-.186.885-.39 1.108-.58.225-.192.227-.298.227-.305zM15 8V4c0-.615-.348-1.1-.755-1.447-.41-.349-.959-.63-1.571-.85C11.442 1.26 9.789 1 8 1s-3.442.26-4.673.703c-.613.22-1.162.501-1.572.85C1.348 2.9 1 3.385 1 4v8c0 .615.348 1.1.755 1.447.41.349.959.63 1.572.85C4.558 14.74 6.21 15 8 15s3.441-.26 4.674-.703c.612-.22 1.161-.501 1.571-.85.407-.346.755-.832.755-1.447zm-1.5 1.939a7 7 0 0 1-.826.358C11.442 10.74 9.789 11 8 11s-3.442-.26-4.673-.703a7 7 0 0 1-.827-.358V12c0 .007.002.113.227.305.223.19.59.394 1.108.58 1.03.371 2.502.615 4.165.615s3.135-.244 4.165-.615c.519-.186.885-.39 1.108-.58.225-.192.227-.298.227-.305z" clipRule="evenodd"/>
                        </svg>
                      </span>
                      <span className="name">{schema.name}</span>
                      <span className="actions">
                        <button onClick={(e) => { e.stopPropagation(); setAddDialog({type: 'table', schemaId: schema.id}); }} title="Add Table">+</button>
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
                            <span className="icon">
                              <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                                <path fillRule="evenodd" d="M1 1.75A.75.75 0 0 1 1.75 1h12.5a.75.75 0 0 1 .75.75v12.5a.75.75 0 0 1-.75.75H1.75a.75.75 0 0 1-.75-.75zm1.5.75v3h11v-3zm0 11V7H5v6.5zm4 0h3V7h-3zM11 7v6.5h2.5V7z" clipRule="evenodd"/>
                              </svg>
                            </span>
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

      {addDialog && (
        <div className="modal" onClick={() => setAddDialog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <h3>Add {addDialog.type}</h3>
            <label>Name:</label>
            <input
              type="text"
              placeholder={`Enter ${addDialog.type} name`}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  const name = (e.target as HTMLInputElement).value;
                  if (addDialog.type === 'table') {
                    const format = (document.getElementById('table-format') as HTMLSelectElement)?.value as 'delta' | 'iceberg';
                    handleAddConfirm(name, format);
                  } else {
                    handleAddConfirm(name);
                  }
                } else if (e.key === 'Escape') {
                  setAddDialog(null);
                }
              }}
              id="add-name-input"
            />
            {addDialog.type === 'table' && (
              <>
                <label style={{marginTop: '12px'}}>Format:</label>
                <select id="table-format" defaultValue="delta">
                  <option value="delta">Delta</option>
                  <option value="iceberg">Iceberg</option>
                </select>
              </>
            )}
            <div className="modal-buttons">
              <button onClick={() => {
                const nameInput = document.getElementById('add-name-input') as HTMLInputElement;
                const name = nameInput.value;
                if (addDialog.type === 'table') {
                  const format = (document.getElementById('table-format') as HTMLSelectElement)?.value as 'delta' | 'iceberg';
                  handleAddConfirm(name, format);
                } else {
                  handleAddConfirm(name);
                }
              }}>Add</button>
              <button onClick={() => setAddDialog(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

