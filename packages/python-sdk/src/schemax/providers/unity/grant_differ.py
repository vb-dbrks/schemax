"""
Grant Differ — Cross-cutting grant comparison logic.

Compares grants on any securable object (catalog, schema, table, view, volume,
function, materialized view) and emits add_grant / revoke_grant operations.
"""

from typing import Any

from schemax.providers.base.operations import Operation

from .operation_builders import create_add_grant_op, create_revoke_grant_op


def diff_grants(
    target_type: str,
    target_id: str,
    old_grants: list[Any],
    new_grants: list[Any],
) -> list[Operation]:
    """Compare grants on a securable object. Emit add_grant/revoke_grant ops.

    Grants with empty principal are skipped (invalid for GRANT/REVOKE SQL).
    """
    ops: list[Operation] = []

    def normalize_grant(grant: Any) -> tuple[str, list[str]]:
        if isinstance(grant, dict):
            principal = (grant.get("principal") or "").strip()
            privs = grant.get("privileges") or []
            return (principal, list(privs) if isinstance(privs, list) else [])
        return ("", [])

    def valid_principal(principal: str) -> bool:
        """Explicitly reject empty principal (invalid for SQL; skip in diff)."""
        return bool(principal and principal.strip())

    def build_grant_map(grants: list[Any]) -> dict[str, list[str]]:
        grant_map: dict[str, list[str]] = {}
        for grant in grants:
            principal, privileges = normalize_grant(grant)
            if valid_principal(principal):
                grant_map[principal] = privileges
        return grant_map

    old_map = build_grant_map(old_grants or [])
    new_map = build_grant_map(new_grants or [])

    all_principals = set(old_map) | set(new_map)
    for principal in all_principals:
        ops.extend(
            _grant_delta_operations(
                target_type=target_type,
                target_id=target_id,
                principal=principal,
                old_map=old_map,
                new_map=new_map,
            )
        )

    return ops


def _grant_delta_operations(
    target_type: str,
    target_id: str,
    principal: str,
    old_map: dict[str, list[str]],
    new_map: dict[str, list[str]],
) -> list[Operation]:
    """Generate grant delta operations for one principal."""
    old_privs = set(old_map.get(principal, []))
    new_privs = set(new_map.get(principal, []))
    if principal not in old_map and new_privs:
        return [create_add_grant_op(target_type, target_id, principal, list(new_privs))]
    if principal not in new_map and old_privs:
        return [create_revoke_grant_op(target_type, target_id, principal, None)]
    return _changed_privilege_operations(
        target_type=target_type,
        target_id=target_id,
        principal=principal,
        removed=old_privs - new_privs,
        added=new_privs - old_privs,
    )


def _changed_privilege_operations(
    target_type: str,
    target_id: str,
    principal: str,
    removed: set[str],
    added: set[str],
) -> list[Operation]:
    """Generate revoke/add operations when principal remains but privileges changed."""
    ops: list[Operation] = []
    if removed:
        ops.append(create_revoke_grant_op(target_type, target_id, principal, list(removed)))
    if added:
        ops.append(create_add_grant_op(target_type, target_id, principal, list(added)))
    return ops
