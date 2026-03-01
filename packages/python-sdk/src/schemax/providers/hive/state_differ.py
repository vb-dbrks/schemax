"""State differ for Hive provider MVP."""

from typing import Any, cast

from schemax.providers.base.operations import Operation, create_operation
from schemax.providers.base.state_differ import StateDiffer


class HiveStateDiffer(StateDiffer):
    """Generate basic diff operations for Hive state."""

    def generate_diff_operations(self) -> list[Operation]:
        old_state = self._state_dict(self.old_state)
        new_state = self._state_dict(self.new_state)

        old_databases = self._id_map(old_state.get("databases", []))
        new_databases = self._id_map(new_state.get("databases", []))

        operations: list[Operation] = []

        for database_id, database in new_databases.items():
            if database_id not in old_databases:
                operations.append(
                    create_operation(
                        provider="hive",
                        op_type="add_database",
                        target=database_id,
                        payload={"name": database.get("name"), "comment": database.get("comment")},
                    )
                )

        for database_id in old_databases:
            if database_id not in new_databases:
                operations.append(
                    create_operation(
                        provider="hive",
                        op_type="drop_database",
                        target=database_id,
                        payload={},
                    )
                )

        return operations

    @staticmethod
    def _state_dict(state: Any) -> dict[str, Any]:
        if isinstance(state, dict):
            return cast(dict[str, Any], state)
        if hasattr(state, "model_dump"):
            return cast(dict[str, Any], state.model_dump(by_alias=True))
        return {}

    @staticmethod
    def _id_map(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {
            str(item.get("id")): item for item in items if isinstance(item, dict) and item.get("id")
        }
