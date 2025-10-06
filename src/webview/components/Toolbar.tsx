import React, { useState } from 'react';
import { useDesignerStore } from '../state/useDesignerStore';

export const Toolbar: React.FC = () => {
  const {
    selectedCatalogId,
    selectedSchemaId,
    selectedTableId,
    addCatalog,
    addSchema,
    addTable,
    addColumn,
    findCatalog,
    findSchema,
    findTable,
  } = useDesignerStore();

  const [showAddCatalog, setShowAddCatalog] = useState(false);
  const [showAddSchema, setShowAddSchema] = useState(false);
  const [showAddTable, setShowAddTable] = useState(false);
  const [showAddColumn, setShowAddColumn] = useState(false);
  const [catalogName, setCatalogName] = useState('');
  const [schemaName, setSchemaName] = useState('');
  const [tableName, setTableName] = useState('');
  const [tableFormat, setTableFormat] = useState<'delta' | 'iceberg'>('delta');
  const [columnName, setColumnName] = useState('');
  const [columnType, setColumnType] = useState('STRING');
  const [columnNullable, setColumnNullable] = useState(true);

  const handleAddCatalog = () => {
    if (catalogName.trim()) {
      addCatalog(catalogName.trim());
      setCatalogName('');
      setShowAddCatalog(false);
    }
  };

  const handleAddSchema = () => {
    if (schemaName.trim() && selectedCatalogId) {
      addSchema(selectedCatalogId, schemaName.trim());
      setSchemaName('');
      setShowAddSchema(false);
    }
  };

  const handleAddTable = () => {
    if (tableName.trim() && selectedSchemaId) {
      addTable(selectedSchemaId, tableName.trim(), tableFormat);
      setTableName('');
      setShowAddTable(false);
    }
  };

  const handleAddColumn = () => {
    if (columnName.trim() && selectedTableId) {
      addColumn(selectedTableId, columnName.trim(), columnType, columnNullable);
      setColumnName('');
      setColumnType('STRING');
      setColumnNullable(true);
      setShowAddColumn(false);
    }
  };

  return (
    <div className="toolbar">
      <button onClick={() => setShowAddCatalog(true)}>Add Catalog</button>
      <button onClick={() => setShowAddSchema(true)} disabled={!selectedCatalogId}>
        Add Schema
      </button>
      <button onClick={() => setShowAddTable(true)} disabled={!selectedSchemaId}>
        Add Table
      </button>
      <button onClick={() => setShowAddColumn(true)} disabled={!selectedTableId}>
        Add Column
      </button>

      {showAddCatalog && (
        <div className="modal">
          <div className="modal-content">
            <h3>Add Catalog</h3>
            <input
              type="text"
              value={catalogName}
              onChange={(e) => setCatalogName(e.target.value)}
              placeholder="Catalog name"
              autoFocus
            />
            <div className="modal-buttons">
              <button onClick={handleAddCatalog}>Create</button>
              <button onClick={() => setShowAddCatalog(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {showAddSchema && (
        <div className="modal">
          <div className="modal-content">
            <h3>Add Schema</h3>
            <input
              type="text"
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              placeholder="Schema name"
              autoFocus
            />
            <div className="modal-buttons">
              <button onClick={handleAddSchema}>Create</button>
              <button onClick={() => setShowAddSchema(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {showAddTable && (
        <div className="modal">
          <div className="modal-content">
            <h3>Add Table</h3>
            <input
              type="text"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              placeholder="Table name"
              autoFocus
            />
            <div className="form-group">
              <label>Format:</label>
              <select value={tableFormat} onChange={(e) => setTableFormat(e.target.value as 'delta' | 'iceberg')}>
                <option value="delta">Delta</option>
                <option value="iceberg">Iceberg</option>
              </select>
            </div>
            <div className="modal-buttons">
              <button onClick={handleAddTable}>Create</button>
              <button onClick={() => setShowAddTable(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {showAddColumn && (
        <div className="modal">
          <div className="modal-content">
            <h3>Add Column</h3>
            <input
              type="text"
              value={columnName}
              onChange={(e) => setColumnName(e.target.value)}
              placeholder="Column name"
              autoFocus
            />
            <div className="form-group">
              <label>Type:</label>
              <select value={columnType} onChange={(e) => setColumnType(e.target.value)}>
                <option value="STRING">STRING</option>
                <option value="INT">INT</option>
                <option value="BIGINT">BIGINT</option>
                <option value="DOUBLE">DOUBLE</option>
                <option value="BOOLEAN">BOOLEAN</option>
                <option value="DATE">DATE</option>
                <option value="TIMESTAMP">TIMESTAMP</option>
                <option value="DECIMAL(10,2)">DECIMAL(10,2)</option>
              </select>
            </div>
            <div className="form-group">
              <label>
                <input
                  type="checkbox"
                  checked={columnNullable}
                  onChange={(e) => setColumnNullable(e.target.checked)}
                />
                Nullable
              </label>
            </div>
            <div className="modal-buttons">
              <button onClick={handleAddColumn}>Create</button>
              <button onClick={() => setShowAddColumn(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

