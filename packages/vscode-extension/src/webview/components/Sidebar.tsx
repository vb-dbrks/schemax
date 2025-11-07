import React, { useState, useRef, useEffect } from 'react';
import { VSCodeButton, VSCodeDropdown, VSCodeOption, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';
import { useDesignerStore } from '../state/useDesignerStore';
import { extractDependenciesFromView } from '../../providers/base/sql-parser';

// Environment config type for tooltip
interface EnvironmentConfig {
  topLevelName: string;
  description?: string;
  [key: string]: any;
}

// SQL parsing and validation helpers for views
const parseViewSQL = (sql: string): { name: string | null; cleanSQL: string } => {
  // Try to extract view name from CREATE VIEW statement
  const createViewPattern = /CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w.]+\.)?(\w+)\s+AS\s+([\s\S]+)/i;
  const match = sql.match(createViewPattern);
  
  if (match) {
    return {
      name: match[1],
      cleanSQL: match[2].trim()
    };
  }
  
  // If no CREATE VIEW found, return SQL as-is
  return {
    name: null,
    cleanSQL: sql.trim()
  };
};

const validateViewSQL = (sql: string): string | null => {
  if (!sql.trim()) {
    return 'SQL definition is required';
  }
  
  if (!/SELECT/i.test(sql)) {
    return 'View must contain a SELECT statement';
  }
  
  return null;
};

// Codicon icons - theme-aware and vector-based
const IconPlus: React.FC = () => (
  <i slot="start" className="codicon codicon-add" aria-hidden="true"></i>
);

const IconEdit: React.FC = () => (
  <i slot="start" className="codicon codicon-edit" aria-hidden="true"></i>
);

const IconTrash: React.FC = () => (
  <i slot="start" className="codicon codicon-trash" aria-hidden="true"></i>
);

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
    addView,
    renameCatalog,
    updateCatalog,
    renameSchema,
    updateSchema,
    renameTable,
    renameView,
    updateView,
    dropCatalog,
    dropSchema,
    dropTable,
    dropView,
  } = useDesignerStore();

  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [addDialog, setAddDialog] = useState<{
    type: 'catalog'|'schema'|'table',
    objectType?: 'table'|'view',
    catalogId?: string,
    schemaId?: string
  } | null>(null);
  const [hoveredCatalogId, setHoveredCatalogId] = useState<string | null>(null);
  const [tooltipAnchor, setTooltipAnchor] = useState<HTMLElement | null>(null);
  const [renameValue, setRenameValue] = useState('');
  const [renameError, setRenameError] = useState<string | null>(null);
  const [addNameInput, setAddNameInput] = useState('');
  const [addError, setAddError] = useState<string | null>(null);
  const [addFormatInput, setAddFormatInput] = useState<'delta' | 'iceberg'>('delta');
  const [addTableType, setAddTableType] = useState<'managed' | 'external'>('managed');
  const [addExternalLocationName, setAddExternalLocationName] = useState('');
  const [addTablePath, setAddTablePath] = useState('');
  const [addManagedLocationName, setAddManagedLocationName] = useState('');
  
  // View-specific state
  const [addViewSQL, setAddViewSQL] = useState('');
  const [addViewName, setAddViewName] = useState('');
  const [addViewNameManual, setAddViewNameManual] = useState(false);
  const [addViewComment, setAddViewComment] = useState('');

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

  const [renameDialog, setRenameDialog] = useState<{type: 'catalog'|'schema'|'table', id: string, name: string, managedLocationName?: string} | null>(null);
  const [dropDialog, setDropDialog] = useState<{type: 'catalog'|'schema'|'table', id: string, name: string} | null>(null);
  const [editManagedLocationName, setEditManagedLocationName] = useState('');

  useEffect(() => {
    if (renameDialog) {
      setRenameValue(renameDialog.name);
      setRenameError(null);
      setEditManagedLocationName(renameDialog.managedLocationName || '');
    }
  }, [renameDialog]);

  useEffect(() => {
    if (addDialog) {
      setAddNameInput('');
      setAddError(null);
      setAddFormatInput('delta');
      setAddTableType('managed');
      setAddExternalLocationName('');
      setAddTablePath('');
      setAddManagedLocationName('');
      
      // Initialize view-specific state
      setAddViewSQL('');
      setAddViewName('');
      setAddViewNameManual(false);
      setAddViewComment('');
      
      // Default to 'table' if objectType not specified (for schema-level additions)
      if (addDialog.type === 'table' && !addDialog.objectType) {
        setAddDialog({...addDialog, objectType: 'table'});
      }
    }
  }, [addDialog]);

  const handleRenameCatalog = (catalogId: string, currentName: string) => {
    // Find the catalog to get its current managed location
    const catalog = project?.state?.catalogs?.find((c: any) => c.id === catalogId);
    setRenameDialog({
      type: 'catalog', 
      id: catalogId, 
      name: currentName,
      managedLocationName: catalog?.managedLocationName
    });
  };

  const handleRenameSchema = (schemaId: string, currentName: string) => {
    // Find the schema to get its current managed location
    let schema: any = null;
    for (const catalog of (project?.state?.catalogs || [])) {
      schema = catalog.schemas?.find((s: any) => s.id === schemaId);
      if (schema) break;
    }
    setRenameDialog({
      type: 'schema', 
      id: schemaId, 
      name: currentName,
      managedLocationName: schema?.managedLocationName
    });
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

  const closeRenameDialog = () => {
    setRenameDialog(null);
    setRenameError(null);
  };

  const closeAddDialog = () => {
    setAddDialog(null);
    setAddError(null);
    setAddNameInput('');
    setAddFormatInput('delta');
  };

  const handleRenameConfirm = (newName: string) => {
    if (!renameDialog) {
      return;
    }

    const trimmedName = newName.trim();
    if (!trimmedName) {
      setRenameError('Name is required.');
      return;
    }

    const nameChanged = trimmedName !== renameDialog.name;
    const locationChanged = editManagedLocationName !== (renameDialog.managedLocationName || '');

    if (!nameChanged && !locationChanged) {
      closeRenameDialog();
      return;
    }
    
    // Handle catalog updates
    if (renameDialog.type === 'catalog') {
      if (nameChanged) {
        renameCatalog(renameDialog.id, trimmedName);
      }
      if (locationChanged) {
        updateCatalog(renameDialog.id, {
          managedLocationName: editManagedLocationName || undefined
        });
      }
    } 
    // Handle schema updates
    else if (renameDialog.type === 'schema') {
      if (nameChanged) {
        renameSchema(renameDialog.id, trimmedName);
      }
      if (locationChanged) {
        updateSchema(renameDialog.id, {
          managedLocationName: editManagedLocationName || undefined
        });
      }
    } 
    // Handle table rename (no location for tables)
    else if (renameDialog.type === 'table') {
      if (nameChanged) {
        renameTable(renameDialog.id, trimmedName);
      }
    }
    // Handle view rename
    else if ((renameDialog.type as any) === 'view') {
      if (nameChanged) {
        renameView(renameDialog.id, trimmedName);
      }
    }
    setRenameError(null);
    closeRenameDialog();
  };

  const handleDropConfirm = () => {
    if (!dropDialog) return;
    
    if (dropDialog.type === 'catalog') {
      dropCatalog(dropDialog.id);
    } else if (dropDialog.type === 'schema') {
      dropSchema(dropDialog.id);
    } else if (dropDialog.type === 'table') {
      dropTable(dropDialog.id);
    } else if ((dropDialog.type as any) === 'view') {
      dropView(dropDialog.id);
    }
    setDropDialog(null);
  };

  const handleAddConfirm = (name: string, format?: 'delta' | 'iceberg') => {
    if (!addDialog) {
      return;
    }

    const trimmedName = name.trim();
    if (!trimmedName) {
      setAddError('Name is required.');
      return;
    }

    if (addDialog.type === 'catalog') {
      const options = addManagedLocationName ? { managedLocationName: addManagedLocationName } : undefined;
      addCatalog(trimmedName, options);
    } else if (addDialog.type === 'schema' && addDialog.catalogId) {
      const options = addManagedLocationName ? { managedLocationName: addManagedLocationName } : undefined;
      addSchema(addDialog.catalogId, trimmedName, options);
      setExpandedCatalogs(new Set(expandedCatalogs).add(addDialog.catalogId));
    } else if (addDialog.type === 'table' && addDialog.schemaId) {
      if (addDialog.objectType === 'view') {
        // VIEW CREATION
        const sqlError = validateViewSQL(addViewSQL);
        if (sqlError) {
          setAddError(sqlError);
          return;
        }
        
        // Extract dependencies using sql-parser
        const dependencies = extractDependenciesFromView(addViewSQL);
        
        // Clean SQL (remove CREATE VIEW if present)
        const parsed = parseViewSQL(addViewSQL);
        const cleanSQL = parsed.cleanSQL || addViewSQL;
        
        addView(addDialog.schemaId, trimmedName, cleanSQL, {
          comment: addViewComment || undefined,
          extractedDependencies: dependencies
        });
        
        setExpandedSchemas(new Set(expandedSchemas).add(addDialog.schemaId));
      } else {
        // TABLE CREATION
        const options = addTableType === 'external' ? {
          external: true,
          externalLocationName: addExternalLocationName,
          path: addTablePath || undefined
        } : undefined;
        
        addTable(addDialog.schemaId, trimmedName, format || 'delta', options);
        setExpandedSchemas(new Set(expandedSchemas).add(addDialog.schemaId));
      }
    }
    setAddError(null);
    closeAddDialog();
  };

  if (!project) {
    return <div className="sidebar">Loading...</div>;
  }

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
        <VSCodeButton
          appearance="secondary"
          className="add-btn-inline"
          type="button"
          onClick={() => setAddDialog({type: 'table', schemaId: selectedSchemaId})}
          title={`Add ${thirdLevelName}`}
        >
          + {thirdLevelName}
        </VSCodeButton>
      );
    } else if (selectedCatalogId) {
      return (
        <VSCodeButton
          appearance="secondary"
          className="add-btn-inline"
          type="button"
          onClick={() => setAddDialog({type: 'schema', catalogId: selectedCatalogId})}
          title={`Add ${secondLevelName}`}
        >
          + {secondLevelName}
        </VSCodeButton>
      );
    } else {
      // Block multi-catalog: Hide "+ Catalog" button if one already exists
      const catalogCount = project.state.catalogs.length;
      if (catalogCount >= 1) {
        return null; // Don't show "+ Catalog" button
      }
      
      return (
        <VSCodeButton
          appearance="secondary"
          className="add-btn-inline"
          type="button"
          onClick={() => setAddDialog({type: 'catalog'})}
          title={`Add ${topLevelName}`}
        >
          + {topLevelName}
        </VSCodeButton>
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
                <i className="codicon codicon-link"></i>
              </span>
              <span className="actions">
                <VSCodeButton
                  appearance="icon"
                  aria-label={`Add ${secondLevelName}`}
                  onClick={(event: React.MouseEvent) => {
                    event.stopPropagation();
                    setAddDialog({type: 'schema', catalogId: catalog.id});
                  }}
                >
                  <IconPlus />
                </VSCodeButton>
                <VSCodeButton
                  appearance="icon"
                  aria-label={`Rename ${topLevelName}`}
                  onClick={(event: React.MouseEvent) => {
                    event.stopPropagation();
                    handleRenameCatalog(catalog.id, catalog.name);
                  }}
                >
                  <IconEdit />
                </VSCodeButton>
                <VSCodeButton
                  appearance="icon"
                  aria-label={`Drop ${topLevelName}`}
                  onClick={(event: React.MouseEvent) => {
                    event.stopPropagation();
                    handleDropCatalog(catalog.id, catalog.name);
                  }}
                >
                  <IconTrash />
                </VSCodeButton>
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
                      <VSCodeButton
                        appearance="icon"
                        aria-label={`Add ${thirdLevelName}`}
                        onClick={(event: React.MouseEvent) => {
                          event.stopPropagation();
                          setAddDialog({type: 'table', schemaId: schema.id});
                        }}
                      >
                        <IconPlus />
                      </VSCodeButton>
                      <VSCodeButton
                        appearance="icon"
                        aria-label={`Rename ${secondLevelName}`}
                        onClick={(event: React.MouseEvent) => {
                          event.stopPropagation();
                          handleRenameSchema(schema.id, schema.name);
                        }}
                      >
                        <IconEdit />
                      </VSCodeButton>
                      <VSCodeButton
                        appearance="icon"
                        aria-label={`Drop ${secondLevelName}`}
                        onClick={(event: React.MouseEvent) => {
                          event.stopPropagation();
                          handleDropSchema(schema.id, schema.name);
                        }}
                      >
                        <IconTrash />
                      </VSCodeButton>
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
                            <i className="codicon codicon-table"></i>
                          </span>
                          <span className="name">{table.name}</span>
                          <span className="badge">{table.format}</span>
                          <span className="actions">
                            <VSCodeButton
                              appearance="icon"
                              aria-label="Rename table"
                              onClick={(event: React.MouseEvent) => {
                                event.stopPropagation();
                                handleRenameTable(table.id, table.name);
                              }}
                            >
                              <IconEdit />
                            </VSCodeButton>
                            <VSCodeButton
                              appearance="icon"
                              aria-label="Drop table"
                              onClick={(event: React.MouseEvent) => {
                                event.stopPropagation();
                                handleDropTable(table.id, table.name);
                              }}
                            >
                              <IconTrash />
                            </VSCodeButton>
                          </span>
                        </div>
                      ))}
                      
                      {/* VIEWS */}
                      {schema.views && schema.views.length > 0 && schema.views.map((view: any) => (
                        <div
                          key={view.id}
                          className={`tree-item ${selectedTableId === view.id ? 'selected' : ''}`}
                          onClick={() => {
                            selectCatalog(catalog.id);
                            selectSchema(schema.id);
                            selectTable(view.id);
                          }}
                        >
                          <span className="icon">
                            <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" fill="none" viewBox="0 0 16 16" aria-hidden="true" focusable="false">
                              <path fill="currentColor" fillRule="evenodd" d="M1.75 1a.75.75 0 0 0-.75.75v12.5c0 .414.336.75.75.75H4v-1.5H2.5V7H5v2h1.5V7h3v2H11V7h2.5v2H15V1.75a.75.75 0 0 0-.75-.75zM13.5 5.5v-3h-11v3z" clipRule="evenodd"></path>
                              <path fill="currentColor" fillRule="evenodd" d="M11.75 10a.75.75 0 0 0-.707.5H9.957a.75.75 0 0 0-.708-.5H5.75a.75.75 0 0 0-.75.75v1.75a2.5 2.5 0 0 0 5 0V12h1v.5a2.5 2.5 0 0 0 5 0v-1.75a.75.75 0 0 0-.75-.75zm.75 2.5v-1h2v1a1 1 0 1 1-2 0m-6-1v1a1 1 0 1 0 2 0v-1z" clipRule="evenodd"></path>
                            </svg>
                          </span>
                          <span className="name">{view.name}</span>
                          <span className="badge" style={{background: 'var(--vscode-charts-blue)'}}>VIEW</span>
                          <span className="actions">
                            <VSCodeButton
                              appearance="icon"
                              aria-label="Rename view"
                              onClick={(event: React.MouseEvent) => {
                                event.stopPropagation();
                                setRenameDialog({
                                  type: 'view' as any,
                                  id: view.id,
                                  name: view.name
                                });
                              }}
                            >
                              <IconEdit />
                            </VSCodeButton>
                            <VSCodeButton
                              appearance="icon"
                              aria-label="Drop view"
                              onClick={(event: React.MouseEvent) => {
                                event.stopPropagation();
                                setDropDialog({
                                  type: 'view' as any,
                                  id: view.id,
                                  name: view.name
                                });
                              }}
                            >
                              <IconTrash />
                            </VSCodeButton>
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
        <div className="modal" role="dialog" aria-modal="true" onClick={closeRenameDialog}>
          <form
            className="modal-content modal-surface"
            onClick={(e) => e.stopPropagation()}
            onSubmit={(event) => {
              event.preventDefault();
              handleRenameConfirm(renameValue);
            }}
          >
            <h3>Edit {renameDialog.type}</h3>
            
            <div className="modal-field-group">
              <label htmlFor="rename-name-input">Name</label>
              <VSCodeTextField
                id="rename-name-input"
                value={renameValue}
                onInput={(event: React.FormEvent<HTMLInputElement>) => {
                  setRenameValue((event.target as HTMLInputElement).value);
                  setRenameError(null);
                }}
                autoFocus
              />
              {renameError && <p className="form-error">{renameError}</p>}
            </div>

            {/* Managed Location for Catalog and Schema */}
            {(renameDialog.type === 'catalog' || renameDialog.type === 'schema') && (
              <div className="modal-field-group">
                <label htmlFor="edit-managed-location-select">
                  Managed Location (optional)
                  <span className="info-icon" title="Storage location for managed tables"> ℹ️</span>
                </label>
                <VSCodeDropdown
                  id="edit-managed-location-select"
                  value={editManagedLocationName}
                  onInput={(event: React.FormEvent<HTMLSelectElement>) => {
                    setEditManagedLocationName((event.target as HTMLSelectElement).value);
                  }}
                >
                  <VSCodeOption value="">-- Default --</VSCodeOption>
                  {Object.entries(project?.managedLocations || {}).map(([name, location]: [string, any]) => (
                    <VSCodeOption key={name} value={name}>
                      {name} {location.description && `(${location.description})`}
                    </VSCodeOption>
                  ))}
                </VSCodeDropdown>

                {editManagedLocationName && project?.managedLocations?.[editManagedLocationName] && (
                  <div className="location-preview">
                    <strong>Paths:</strong>
                    <div className="env-paths-list">
                      {Object.entries(project.managedLocations[editManagedLocationName].paths || {}).map(([env, path]) => (
                        <div key={env} className="path-row">
                          <span className="env-label">{env}:</span>
                          <code className="path-value">{path}</code>
                        </div>
                      ))}
                      {Object.keys(project.managedLocations[editManagedLocationName].paths || {}).length === 0 && (
                        <div className="path-row muted">No paths configured</div>
                      )}
                    </div>
                  </div>
                )}
                
                <p className="field-help">
                  Specifies where Unity Catalog stores data for managed tables in this {renameDialog.type}.
                </p>
              </div>
            )}

            <div className="modal-buttons">
              <VSCodeButton type="submit">Save</VSCodeButton>
              <VSCodeButton type="button" appearance="secondary" onClick={closeRenameDialog}>
                Cancel
              </VSCodeButton>
            </div>
          </form>
        </div>
      )}

      {dropDialog && (
        <div className="modal" role="alertdialog" aria-modal="true" onClick={() => setDropDialog(null)}>
          <form
            className="modal-content modal-surface"
            onClick={(e) => e.stopPropagation()}
            onSubmit={(event) => {
              event.preventDefault();
              handleDropConfirm();
            }}
          >
            <h3>Confirm Drop</h3>
            <p>Are you sure you want to drop {dropDialog.type} "{dropDialog.name}"?</p>
            {dropDialog.type === 'catalog' && (
              <p className="warning-banner">
                <span className="warning-banner__icon" aria-hidden="true" />
                This will also drop every schema and table underneath this catalog.
              </p>
            )}
            {dropDialog.type === 'schema' && (
              <p className="warning-banner">
                <span className="warning-banner__icon" aria-hidden="true" />
                This will also drop all tables inside this schema.
              </p>
            )}
            <div className="modal-buttons">
              <VSCodeButton type="button" appearance="secondary" onClick={() => setDropDialog(null)}>
                Cancel
              </VSCodeButton>
              <VSCodeButton type="submit" className="danger-button">
                Drop
              </VSCodeButton>
            </div>
          </form>
        </div>
      )}

      {addDialog && (
        <div className="modal" role="dialog" aria-modal="true" onClick={closeAddDialog}>
          <form
            className="modal-content modal-surface"
            onClick={(e) => e.stopPropagation()}
            onSubmit={(event) => {
              event.preventDefault();
              handleAddConfirm(
                addNameInput,
                addDialog.type === 'table' ? addFormatInput : undefined
              );
            }}
          >
            <h3>Add {addDialog.type}</h3>
            
            {/* Object Type Selector - Only for schema-level additions */}
            {addDialog.type === 'table' && (
              <div className="modal-field-group">
                <label>Object Type</label>
                <div className="radio-group">
                  <label>
                    <input
                      type="radio"
                      value="table"
                      checked={addDialog.objectType === 'table'}
                      onChange={() => setAddDialog({...addDialog, objectType: 'table'})}
                    />
                    Table
                  </label>
                  <label>
                    <input
                      type="radio"
                      value="view"
                      checked={addDialog.objectType === 'view'}
                      onChange={() => setAddDialog({...addDialog, objectType: 'view'})}
                    />
                    View
                  </label>
                </div>
              </div>
            )}
            
            {/* VIEW FIELDS */}
            {addDialog.objectType === 'view' && (
              <>
                {/* SQL Definition */}
                <div className="modal-field-group">
                  <label htmlFor="view-sql">
                    SQL Definition *
                    <span className="info-icon" title="Paste CREATE VIEW...AS or just SELECT statement"> ℹ️</span>
                  </label>
                  <textarea
                    id="view-sql"
                    value={addViewSQL}
                    placeholder="CREATE VIEW my_view AS SELECT... or just SELECT..."
                    rows={8}
                    style={{ 
                      width: '100%', 
                      fontFamily: 'var(--vscode-editor-font-family, monospace)',
                      fontSize: '12px',
                      padding: '8px',
                      border: '1px solid var(--vscode-input-border)',
                      background: 'var(--vscode-input-background)',
                      color: 'var(--vscode-input-foreground)',
                      resize: 'vertical'
                    }}
                    onInput={(e: React.FormEvent<HTMLTextAreaElement>) => {
                      const sql = (e.target as HTMLTextAreaElement).value;
                      setAddViewSQL(sql);
                      setAddError(null);
                      
                      // Auto-extract view name if not manually set
                      if (!addViewNameManual) {
                        const parsed = parseViewSQL(sql);
                        if (parsed.name) {
                          setAddViewName(parsed.name);
                          setAddNameInput(parsed.name);
                        }
                      }
                    }}
                    onBlur={() => {
                      // Validate on blur
                      const error = validateViewSQL(addViewSQL);
                      if (error) {
                        setAddError(error);
                      }
                    }}
                  />
                </div>
                
                {/* View Name (auto-extracted or manual) */}
                <div className="modal-field-group">
                  <label htmlFor="view-name">
                    View Name *
                    <span className="info-icon" title="Auto-extracted from SQL or enter manually"> ℹ️</span>
                  </label>
                  <VSCodeTextField
                    id="view-name"
                    value={addNameInput}
                    placeholder="Enter view name"
                    onInput={(event: React.FormEvent<HTMLInputElement>) => {
                      setAddNameInput((event.target as HTMLInputElement).value);
                      setAddViewNameManual(true); // Mark as manually edited
                      setAddError(null);
                    }}
                  />
                  {addViewName && !addViewNameManual && (
                    <small style={{ color: 'var(--vscode-descriptionForeground)', display: 'block', marginTop: '4px' }}>
                      ✓ Name auto-extracted from SQL
                    </small>
                  )}
                </div>
                
                {/* Comment (optional) */}
                <div className="modal-field-group">
                  <label htmlFor="view-comment">Comment (optional)</label>
                  <VSCodeTextField
                    id="view-comment"
                    value={addViewComment}
                    placeholder="Describe this view"
                    onInput={(event: React.FormEvent<HTMLInputElement>) => {
                      setAddViewComment((event.target as HTMLInputElement).value);
                    }}
                  />
                </div>
              </>
            )}
            
            {/* TABLE/CATALOG/SCHEMA FIELDS */}
            {addDialog.objectType !== 'view' && (
              <>
                <VSCodeTextField
                  value={addNameInput}
                  placeholder={`Enter ${addDialog.type} name`}
                  autoFocus={addDialog.objectType !== 'view'}
                  onInput={(event: React.FormEvent<HTMLInputElement>) => {
                    setAddNameInput((event.target as HTMLInputElement).value);
                    setAddError(null);
                  }}
                />
              </>
            )}
            
            {/* Managed Location for Catalog and Schema */}
            {(addDialog.type === 'catalog' || addDialog.type === 'schema') && (
              <div className="modal-field-group">
                <label htmlFor="managed-location-select">
                  Managed Location (optional)
                  <span className="info-icon" title="Storage location for managed tables"> ℹ️</span>
                </label>
                <VSCodeDropdown
                  id="managed-location-select"
                  value={addManagedLocationName}
                  onInput={(event: React.FormEvent<HTMLSelectElement>) => {
                    setAddManagedLocationName((event.target as HTMLSelectElement).value);
                  }}
                >
                  <VSCodeOption value="">-- Default --</VSCodeOption>
                  {Object.entries(project?.managedLocations || {}).map(([name, location]: [string, any]) => (
                    <VSCodeOption key={name} value={name}>
                      {name} {location.description && `(${location.description})`}
                    </VSCodeOption>
                  ))}
                </VSCodeDropdown>

                {addManagedLocationName && project?.managedLocations?.[addManagedLocationName] && (
                  <div className="location-preview">
                    <strong>Paths:</strong>
                    <div className="env-paths-list">
                      {Object.entries(project.managedLocations[addManagedLocationName].paths || {}).map(([env, path]) => (
                        <div key={env} className="path-row">
                          <span className="env-label">{env}:</span>
                          <code className="path-value">{path}</code>
                        </div>
                      ))}
                      {Object.keys(project.managedLocations[addManagedLocationName].paths || {}).length === 0 && (
                        <div className="path-row muted">No paths configured</div>
                      )}
                    </div>
                  </div>
                )}
                
                <p className="field-help">
                  Specifies where Unity Catalog stores data for managed tables in this {addDialog.type}.
                </p>
              </div>
            )}
            
            {addDialog.type === 'table' && addDialog.objectType === 'table' && (
              <>
                {/* Table Type Selection */}
                <div className="modal-field-group">
                  <label>Table Type</label>
                  <div className="radio-group">
                    <label>
                      <input
                        type="radio"
                        value="managed"
                        checked={addTableType === 'managed'}
                        onChange={() => setAddTableType('managed')}
                      />
                      Managed (Recommended)
                    </label>
                    <label>
                      <input
                        type="radio"
                        value="external"
                        checked={addTableType === 'external'}
                        onChange={() => setAddTableType('external')}
                      />
                      External
                    </label>
                  </div>
                </div>

                {/* Format Selection */}
                <div className="modal-field-group">
                  <label htmlFor="table-format-select">Format</label>
                  <VSCodeDropdown
                    id="table-format-select"
                    value={addFormatInput}
                    onInput={(event: React.FormEvent<HTMLSelectElement>) => {
                      setAddFormatInput((event.target as HTMLSelectElement).value as 'delta' | 'iceberg');
                    }}
                  >
                    <VSCodeOption value="delta">Delta</VSCodeOption>
                    <VSCodeOption value="iceberg">Iceberg</VSCodeOption>
                  </VSCodeDropdown>
                </div>

                {/* External Location Controls */}
                {addTableType === 'external' && (
                  <>
                    <div className="modal-field-group">
                      <label htmlFor="external-location-select">
                        External Location
                        <span className="info-icon" title="Pre-configured external location in Unity Catalog"> ℹ️</span>
                      </label>
                      <VSCodeDropdown
                        id="external-location-select"
                        value={addExternalLocationName}
                        onInput={(event: React.FormEvent<HTMLSelectElement>) => {
                          setAddExternalLocationName((event.target as HTMLSelectElement).value);
                        }}
                      >
                        <VSCodeOption value="">-- Select Location --</VSCodeOption>
                        {Object.entries(project?.externalLocations || {}).map(([name, location]: [string, any]) => (
                          <VSCodeOption key={name} value={name}>
                            {name} {location.description && `(${location.description})`}
                          </VSCodeOption>
                        ))}
                      </VSCodeDropdown>

                      {addExternalLocationName && project?.externalLocations?.[addExternalLocationName] && (
                        <div className="location-preview">
                          <strong>Base Paths:</strong>
                          <div className="env-paths-list">
                            {Object.entries(project.externalLocations[addExternalLocationName].paths || {}).map(([env, path]) => (
                              <div key={env} className="path-row">
                                <span className="env-label">{env}:</span>
                                <code className="path-value">{path}</code>
                              </div>
                            ))}
                            {Object.keys(project.externalLocations[addExternalLocationName].paths || {}).length === 0 && (
                              <div className="path-row muted">No paths configured</div>
                            )}
                          </div>
                        </div>
                      )}
                    </div>

                    <div className="modal-field-group">
                      <label htmlFor="table-path-input">
                        Path (optional)
                        <span className="info-icon" title="Relative path under the external location"> ℹ️</span>
                      </label>
                      <VSCodeTextField
                        id="table-path-input"
                        value={addTablePath}
                        placeholder="orders or relative/path/to/table"
                        disabled={!addExternalLocationName}
                        onInput={(event: React.FormEvent<HTMLInputElement>) => {
                          setAddTablePath((event.target as HTMLInputElement).value);
                        }}
                      />

                      {addExternalLocationName && addTablePath && project?.externalLocations?.[addExternalLocationName] && (
                        <div className="location-preview">
                          <strong>Full Paths:</strong>
                          <div className="env-paths-list">
                            {Object.entries(project.externalLocations[addExternalLocationName].paths || {}).map(([env, path]) => (
                              <div key={env} className="path-row">
                                <span className="env-label">{env}:</span>
                                <code className="path-value">{path}/{addTablePath}</code>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>

                    <div className="warning-box">
                      <p>⚠️ <strong>Databricks recommends managed tables</strong></p>
                      <p className="help-text">
                        External locations must be pre-configured in Unity Catalog. 
                        Managed tables offer better performance and automatic maintenance.
                      </p>
                      <a 
                        href="https://learn.microsoft.com/en-gb/azure/databricks/tables/managed" 
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        Learn more about managed tables →
                      </a>
                    </div>
                  </>
                )}
              </>
            )}
            {addError && <p className="form-error">{addError}</p>}
            <div className="modal-buttons">
              <VSCodeButton type="submit">Add</VSCodeButton>
              <VSCodeButton type="button" appearance="secondary" onClick={closeAddDialog}>
                Cancel
              </VSCodeButton>
            </div>
          </form>
        </div>
      )}
    </div>
  );
};
