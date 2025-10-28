import React, { useState, useRef, useEffect } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';

// Environment config type for tooltip
interface EnvironmentConfig {
  topLevelName: string;
  description?: string;
  [key: string]: any;
}

// Tooltip component for showing logical → physical catalog name mappings
const TopLevelMappingTooltip: React.FC<{
  logicalName: string;
  environments: Record<string, EnvironmentConfig>;
  topLevelDisplayName: string;
  anchorElement: HTMLElement | null;
}> = ({ logicalName, environments, topLevelDisplayName, anchorElement }) => {
  const [position, setPosition] = useState({ top: 0, left: 0 });
  const tooltipRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (anchorElement && tooltipRef.current) {
      const rect = anchorElement.getBoundingClientRect();
      const tooltipRect = tooltipRef.current.getBoundingClientRect();
      const viewportWidth = window.innerWidth;
      const viewportHeight = window.innerHeight;
      
      // Default position: to the right of the icon
      let left = rect.right + 8;
      let top = rect.top + rect.height / 2;
      
      // Check if tooltip would go off-screen to the right
      if (left + tooltipRect.width > viewportWidth - 20) {
        // Position to the left of the icon instead
        left = rect.left - tooltipRect.width - 8;
      }
      
      // Ensure tooltip doesn't go off top or bottom of viewport
      const tooltipHalfHeight = tooltipRect.height / 2;
      if (top - tooltipHalfHeight < 20) {
        // Too close to top, align to top edge
        top = tooltipHalfHeight + 20;
      } else if (top + tooltipHalfHeight > viewportHeight - 20) {
        // Too close to bottom, align to bottom edge
        top = viewportHeight - tooltipHalfHeight - 20;
      }
      
      setPosition({ top, left });
    }
  }, [anchorElement]);

  return (
    <div 
      ref={tooltipRef}
      className="mapping-tooltip" 
      style={{ 
        top: `${position.top}px`, 
        left: `${position.left}px`,
        transform: 'translateY(-50%)'
      }}
    >
      <div className="tooltip-header">
        Logical {topLevelDisplayName}: {logicalName}
      </div>
      <div className="tooltip-section">
        <div className="tooltip-label">Physical names per environment:</div>
        {Object.entries(environments).map(([env, config]) => (
          <div className="tooltip-mapping" key={env}>
            <span className="env-name">{env}</span>
            <span className="arrow">→</span>
            <span className="physical-name">{config.topLevelName}</span>
          </div>
        ))}
      </div>
      <div className="tooltip-footer">
        Applied at deployment time via CLI
      </div>
    </div>
  );
};

export const Sidebar: React.FC = () => {
  const {
    project,
    provider,
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
  const [hoveredCatalogId, setHoveredCatalogId] = useState<string | null>(null);
  const [tooltipAnchor, setTooltipAnchor] = useState<HTMLElement | null>(null);

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

  // Debug logging
  console.log('[Sidebar] Project loaded:', {
    hasState: !!project.state,
    hasCatalogs: !!project.state?.catalogs,
    catalogCount: project.state?.catalogs?.length || 0,
    hasProvider: !!provider,
    hasEnvironments: !!project.provider?.environments,
  });

  // Safety checks
  if (!project.state || !project.state.catalogs) {
    return <div className="sidebar">Error: Invalid project state</div>;
  }

  // Get hierarchy level names from provider for provider-agnostic labels
  // Note: hierarchy is a plain object after JSON serialization, not a class instance
  const hierarchyLevels = provider?.capabilities?.hierarchy?.levels || [];
  const topLevelName = hierarchyLevels[0]?.displayName || 'Catalog';
  const secondLevelName = hierarchyLevels[1]?.displayName || 'Schema';
  const thirdLevelName = hierarchyLevels[2]?.displayName || 'Table';

  // Determine the context-aware add button based on selection
  const getAddButton = () => {
    if (selectedSchemaId) {
      return (
        <button 
          className="add-btn-inline" 
          onClick={() => setAddDialog({type: 'table', schemaId: selectedSchemaId})}
          title={`Add ${thirdLevelName}`}
        >
          + {thirdLevelName}
        </button>
      );
    } else if (selectedCatalogId) {
      return (
        <button 
          className="add-btn-inline" 
          onClick={() => setAddDialog({type: 'schema', catalogId: selectedCatalogId})}
          title={`Add ${secondLevelName}`}
        >
          + {secondLevelName}
        </button>
      );
    } else {
      // Block multi-catalog: Hide "+ Catalog" button if one already exists
      const catalogCount = project.state.catalogs.length;
      if (catalogCount >= 1) {
        return null; // Don't show "+ Catalog" button
      }
      
      return (
        <button 
          className="add-btn-inline" 
          onClick={() => setAddDialog({type: 'catalog'})}
          title={`Add ${topLevelName}`}
        >
          + {topLevelName}
        </button>
      );
    }
  };

  // Render full hierarchy tree (catalog → schema → table)
  const renderTree = () => {
    return (
      <>
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
              <span className="name">
                {catalog.name}
              </span>
              {/* Mapping indicator icon with independent hover */}
              <span 
                className="mapping-indicator" 
                onMouseEnter={(e) => {
                  e.stopPropagation();
                  setHoveredCatalogId(catalog.id);
                  setTooltipAnchor(e.currentTarget as HTMLElement);
                }}
                onMouseLeave={(e) => {
                  e.stopPropagation();
                  setHoveredCatalogId(null);
                  setTooltipAnchor(null);
                }}
                title="View environment mappings"
              >
                <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
                  <path fillRule="evenodd" d="M7.775 3.275a.75.75 0 001.06 1.06l1.25-1.25a2 2 0 112.83 2.83l-2.5 2.5a2 2 0 01-2.83 0 .75.75 0 00-1.06 1.06 3.5 3.5 0 004.95 0l2.5-2.5a3.5 3.5 0 00-4.95-4.95l-1.25 1.25zm-4.69 9.64a2 2 0 010-2.83l2.5-2.5a2 2 0 012.83 0 .75.75 0 001.06-1.06 3.5 3.5 0 00-4.95 0l-2.5 2.5a3.5 3.5 0 004.95 4.95l1.25-1.25a.75.75 0 00-1.06-1.06l-1.25 1.25a2 2 0 01-2.83 0z" clipRule="evenodd"/>
                </svg>
              </span>
              <span className="actions">
                <button onClick={(e) => { e.stopPropagation(); setAddDialog({type: 'schema', catalogId: catalog.id}); }} title={`Add ${secondLevelName}`}>+</button>
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
                      <button onClick={(e) => { e.stopPropagation(); setAddDialog({type: 'table', schemaId: schema.id}); }} title={`Add ${thirdLevelName}`}>+</button>
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
      </>
    );
  };

  // Get the hovered catalog for tooltip
  const hoveredCatalog = hoveredCatalogId 
    ? project.state.catalogs.find(c => c.id === hoveredCatalogId)
    : null;

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <h2>{provider?.name || 'Schema Designer'}</h2>
        {getAddButton()}
      </div>
      <div className="tree">
        {renderTree()}
      </div>

      {/* Render tooltip at root level with fixed positioning */}
      {hoveredCatalogId && tooltipAnchor && hoveredCatalog && project.provider.environments && (
        <TopLevelMappingTooltip
          logicalName={hoveredCatalog.name}
          environments={project.provider.environments}
          topLevelDisplayName={topLevelName}
          anchorElement={tooltipAnchor}
        />
      )}

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
