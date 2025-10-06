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

  const handleRenameCatalog = (catalogId: string, currentName: string) => {
    const newName = prompt('Rename catalog:', currentName);
    if (newName && newName !== currentName) {
      renameCatalog(catalogId, newName);
    }
  };

  const handleRenameSchema = (schemaId: string, currentName: string) => {
    const newName = prompt('Rename schema:', currentName);
    if (newName && newName !== currentName) {
      renameSchema(schemaId, newName);
    }
  };

  const handleRenameTable = (tableId: string, currentName: string) => {
    const newName = prompt('Rename table:', currentName);
    if (newName && newName !== currentName) {
      renameTable(tableId, newName);
    }
  };

  const handleDropCatalog = (catalogId: string, name: string) => {
    if (confirm(`Drop catalog "${name}"? This will also drop all schemas and tables.`)) {
      dropCatalog(catalogId);
    }
  };

  const handleDropSchema = (schemaId: string, name: string) => {
    if (confirm(`Drop schema "${name}"? This will also drop all tables.`)) {
      dropSchema(schemaId);
    }
  };

  const handleDropTable = (tableId: string, name: string) => {
    if (confirm(`Drop table "${name}"?`)) {
      dropTable(tableId);
    }
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
    </div>
  );
};

