import React, { useMemo, useState } from 'react';
import { VSCodeButton, VSCodeDropdown, VSCodeOption, VSCodeTextField } from '@vscode/webview-ui-toolkit/react';
import type { Operation } from '../../providers/base/operations';
import { useDesignerStore } from '../state/useDesignerStore';
import { formatScopePreview } from '../utils/bulkUtils';
import type { GrantTargetType } from '../utils/bulkUtils';
import { parsePrivileges } from '../utils/grants';

const GRANT_OP_TYPES = [
  'add_catalog_grant',
  'add_schema_grants',
  'add_table_grants',
  'add_view_grants',
  'add_volume_grants',
  'add_function_grants',
  'add_materialized_view_grants',
] as const;
const TAG_OP_TYPES = ['add_table_tag', 'add_view_tag', 'add_schema_tag', 'add_catalog_tag'] as const;

export type BulkOperationType =
  | (typeof GRANT_OP_TYPES)[number]
  | (typeof TAG_OP_TYPES)[number];

const GRANT_OP_TO_TARGET: Record<(typeof GRANT_OP_TYPES)[number], GrantTargetType> = {
  add_catalog_grant: 'catalog',
  add_schema_grants: 'schema',
  add_table_grants: 'table',
  add_view_grants: 'view',
  add_volume_grants: 'volume',
  add_function_grants: 'function',
  add_materialized_view_grants: 'materialized_view',
};

function isGrantOp(op: BulkOperationType): op is (typeof GRANT_OP_TYPES)[number] {
  return GRANT_OP_TYPES.includes(op as (typeof GRANT_OP_TYPES)[number]);
}
function isTagOp(op: BulkOperationType): op is (typeof TAG_OP_TYPES)[number] {
  return TAG_OP_TYPES.includes(op as (typeof TAG_OP_TYPES)[number]);
}

interface BulkOperationsPanelProps {
  scope: 'catalog' | 'schema';
  catalogId?: string | null;
  schemaId?: string | null;
  onClose: () => void;
}

export const BulkOperationsPanel: React.FC<BulkOperationsPanelProps> = ({
  scope,
  catalogId,
  schemaId,
  onClose,
}) => {
  const {
    project,
    getObjectsInScope,
    buildBulkGrantOps,
    buildBulkTableTagOps,
    buildBulkViewTagOps,
    buildBulkSchemaTagOps,
    buildBulkCatalogTagOps,
    applyBulkOps,
  } = useDesignerStore();

  const scopeResult = useMemo(
    () => getObjectsInScope(scope, catalogId ?? undefined, schemaId ?? undefined),
    [getObjectsInScope, scope, catalogId, schemaId]
  );

  const [operationType, setOperationType] = useState<BulkOperationType>('add_table_grants');
  const [principal, setPrincipal] = useState('');
  const [privilegesStr, setPrivilegesStr] = useState('');
  const [tagName, setTagName] = useState('');
  const [tagValue, setTagValue] = useState('');

  const previewText = formatScopePreview(scopeResult);

  const opCount = useMemo(() => {
    if (isGrantOp(operationType)) {
      const targetType = GRANT_OP_TO_TARGET[operationType];
      return scopeResult.grantTargets.filter((g) => g.targetType === targetType).length;
    }
    switch (operationType) {
      case 'add_table_tag':
        return scopeResult.tables.length;
      case 'add_view_tag':
        return scopeResult.views.length;
      case 'add_schema_tag':
        return scopeResult.schemas.length;
      case 'add_catalog_tag':
        return scopeResult.catalog ? 1 : 0;
      default:
        return 0;
    }
  }, [operationType, scopeResult]);

  const canApply = useMemo(() => {
    if (opCount === 0) return false;
    if (isGrantOp(operationType)) {
      return principal.trim() !== '' && parsePrivileges(privilegesStr).length > 0;
    }
    if (isTagOp(operationType)) {
      return tagName.trim() !== '' && tagValue.trim() !== '';
    }
    return false;
  }, [operationType, opCount, principal, privilegesStr, tagName, tagValue]);

  const handleApply = () => {
    if (!canApply) return;
    let ops: Operation[] = [];
    if (isGrantOp(operationType)) {
      const privileges = parsePrivileges(privilegesStr);
      if (principal.trim() && privileges.length > 0) {
        ops = buildBulkGrantOps(
          scopeResult,
          principal.trim(),
          privileges,
          GRANT_OP_TO_TARGET[operationType]
        );
      }
    } else if (operationType === 'add_table_tag' && tagName.trim() && tagValue.trim()) {
      ops = buildBulkTableTagOps(scopeResult, tagName.trim(), tagValue.trim());
    } else if (operationType === 'add_view_tag' && tagName.trim() && tagValue.trim()) {
      ops = buildBulkViewTagOps(scopeResult, tagName.trim(), tagValue.trim());
    } else if (operationType === 'add_schema_tag' && tagName.trim() && tagValue.trim()) {
      ops = buildBulkSchemaTagOps(scopeResult, tagName.trim(), tagValue.trim());
    } else if (
      operationType === 'add_catalog_tag' &&
      tagName.trim() &&
      tagValue.trim() &&
      scopeResult.catalog
    ) {
      ops = buildBulkCatalogTagOps(scopeResult, tagName.trim(), tagValue.trim());
    }
    if (ops.length > 0) {
      applyBulkOps(ops);
      onClose();
    }
  };

  const showCatalogTagOption = scope === 'catalog' && scopeResult.catalog != null;

  if (!project) {
    return null;
  }

  return (
    <div
      className="modal-overlay"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-labelledby="bulk-ops-title"
    >
      <div className="modal-content bulk-ops-modal" onClick={(e) => e.stopPropagation()}>
        <h2 id="bulk-ops-title">Bulk operations</h2>

        <div className="modal-body">
          <div className="bulk-ops-scope">
            <label>Scope</label>
            <p className="bulk-ops-preview" aria-live="polite">
              {previewText}
            </p>
          </div>

          <div className="modal-field-group">
            <label htmlFor="bulk-op-type">Operation</label>
            <VSCodeDropdown
              id="bulk-op-type"
              value={operationType}
              onInput={(e) => {
                const value = (e.target as HTMLSelectElement).value as BulkOperationType;
                setOperationType(value || 'add_table_grants');
              }}
              aria-label="Operation type"
            >
              <VSCodeOption value="add_catalog_grant">Add catalog grant</VSCodeOption>
              <VSCodeOption value="add_schema_grants">Add schema grants</VSCodeOption>
              <VSCodeOption value="add_table_grants">Add table grants</VSCodeOption>
              <VSCodeOption value="add_view_grants">Add view grants</VSCodeOption>
              <VSCodeOption value="add_volume_grants">Add volume grants</VSCodeOption>
              <VSCodeOption value="add_function_grants">Add function grants</VSCodeOption>
              <VSCodeOption value="add_materialized_view_grants">Add materialized view grants</VSCodeOption>
              <VSCodeOption value="add_table_tag">Add table tag</VSCodeOption>
              <VSCodeOption value="add_view_tag">Add view tag</VSCodeOption>
              <VSCodeOption value="add_schema_tag">Add schema tag</VSCodeOption>
              {showCatalogTagOption && (
                <VSCodeOption value="add_catalog_tag">Add catalog tag</VSCodeOption>
              )}
            </VSCodeDropdown>
          </div>

          {isGrantOp(operationType) && (
            <>
              <div className="modal-field-group">
                <label htmlFor="bulk-grant-principal">Principal (user, group, or service principal)</label>
                <VSCodeTextField
                  id="bulk-grant-principal"
                  value={principal}
                  onInput={(e) => {
                    const target = e.target as HTMLInputElement;
                    setPrincipal(target.value ?? '');
                  }}
                  placeholder="e.g. data_engineers"
                  aria-label="Principal"
                />
              </div>
              <div className="modal-field-group">
                <label htmlFor="bulk-grant-privileges">
                  Privileges (comma-separated; use valid privileges for each object type, e.g. SELECT, MODIFY,
                  USE CATALOG)
                </label>
                <VSCodeTextField
                  id="bulk-grant-privileges"
                  value={privilegesStr}
                  onInput={(e) => {
                    const target = e.target as HTMLInputElement;
                    setPrivilegesStr(target.value ?? '');
                  }}
                  placeholder="e.g. SELECT, MODIFY"
                  aria-label="Privileges"
                />
              </div>
            </>
          )}

          {isTagOp(operationType) && (
            <>
              <div className="modal-field-group">
                <label htmlFor="bulk-tag-name">Tag name</label>
                <VSCodeTextField
                  id="bulk-tag-name"
                  value={tagName}
                  onInput={(e) => {
                    const target = e.target as HTMLInputElement;
                    setTagName(target.value ?? '');
                  }}
                  placeholder="e.g. domain"
                  aria-label="Tag name"
                />
              </div>
              <div className="modal-field-group">
                <label htmlFor="bulk-tag-value">Tag value</label>
                <VSCodeTextField
                  id="bulk-tag-value"
                  value={tagValue}
                  onInput={(e) => {
                    const target = e.target as HTMLInputElement;
                    setTagValue(target.value ?? '');
                  }}
                  placeholder="e.g. sales"
                  aria-label="Tag value"
                />
              </div>
            </>
          )}

          {opCount === 0 && (
            <p className="bulk-ops-empty" role="status">
              No objects in scope for this operation. Select a different operation or scope.
            </p>
          )}
          {opCount > 0 && (
            <p className="bulk-ops-count" role="status">
              {opCount} operation{opCount === 1 ? '' : 's'} will be applied.
            </p>
          )}
        </div>

        <div className="modal-buttons">
          <VSCodeButton appearance="secondary" onClick={onClose}>
            Cancel
          </VSCodeButton>
          <VSCodeButton onClick={handleApply} disabled={!canApply}>
            Apply
          </VSCodeButton>
        </div>
      </div>
    </div>
  );
};
