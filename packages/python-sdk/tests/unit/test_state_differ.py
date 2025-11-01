"""
Unit tests for StateDiffer and UnityStateDiffer

Tests state comparison, diff generation, and rename detection.
"""

from datetime import UTC, datetime

from schematic.providers.base.operations import Operation
from schematic.providers.unity.state_differ import UnityStateDiffer


class TestUnityStateDiffer:
    """Test UnityStateDiffer for catalog/schema/table/column diffing"""

    def test_diff_added_catalog(self) -> None:
        """Should generate add_catalog operation for new catalog"""
        old_state = {"catalogs": []}
        new_state = {
            "catalogs": [
                {"id": "cat_1", "name": "bronze", "schemas": []},
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.add_catalog"
        assert ops[0].target == "cat_1"
        assert ops[0].payload["name"] == "bronze"

    def test_diff_removed_catalog(self) -> None:
        """Should generate drop_catalog operation for removed catalog"""
        old_state = {
            "catalogs": [
                {"id": "cat_1", "name": "bronze", "schemas": []},
            ]
        }
        new_state = {"catalogs": []}

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.drop_catalog"
        assert ops[0].target == "cat_1"

    def test_diff_renamed_catalog_with_history(self) -> None:
        """Should detect catalog rename from operation history"""
        old_state = {
            "catalogs": [
                {"id": "cat_1", "name": "bronze", "schemas": []},
            ]
        }
        new_state = {
            "catalogs": [
                {"id": "cat_1", "name": "silver", "schemas": []},
            ]
        }

        rename_op = Operation(
            id="op_1",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_catalog",
            target="cat_1",
            payload={"oldName": "bronze", "newName": "silver"},
        )

        differ = UnityStateDiffer(old_state, new_state, [], [rename_op])
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.rename_catalog"
        assert ops[0].payload["oldName"] == "bronze"
        assert ops[0].payload["newName"] == "silver"

    def test_diff_added_schema(self) -> None:
        """Should generate add_schema operation for new schema"""
        old_state = {
            "catalogs": [
                {"id": "cat_1", "name": "bronze", "schemas": []},
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {"id": "sch_1", "name": "sales", "tables": []},
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.add_schema"
        assert ops[0].target == "sch_1"
        assert ops[0].payload["name"] == "sales"
        assert ops[0].payload["catalogId"] == "cat_1"

    def test_diff_added_table(self) -> None:
        """Should generate add_table operation for new table"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {"id": "sch_1", "name": "sales", "tables": []},
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [],
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.add_table"
        assert ops[0].target == "tbl_1"
        assert ops[0].payload["name"] == "customers"
        assert ops[0].payload["schemaId"] == "sch_1"

    def test_diff_added_column(self) -> None:
        """Should generate add_column operation for new column"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                        {
                                            "id": "col_2",
                                            "name": "name",
                                            "type": "STRING",
                                            "nullable": True,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.add_column"
        assert ops[0].target == "col_2"
        assert ops[0].payload["name"] == "name"
        assert ops[0].payload["type"] == "STRING"
        assert ops[0].payload["tableId"] == "tbl_1"

    def test_diff_removed_column(self) -> None:
        """Should generate drop_column operation for removed column"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                        {
                                            "id": "col_2",
                                            "name": "name",
                                            "type": "STRING",
                                            "nullable": True,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.drop_column"
        assert ops[0].target == "col_2"
        assert ops[0].payload["tableId"] == "tbl_1"

    def test_diff_renamed_column_with_history(self) -> None:
        """Should detect column rename from operation history"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "user_id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "customer_id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        rename_op = Operation(
            id="op_1",
            ts=datetime.now(UTC).isoformat(),
            provider="unity",
            op="unity.rename_column",
            target="col_1",
            payload={"oldName": "user_id", "newName": "customer_id", "tableId": "tbl_1"},
        )

        differ = UnityStateDiffer(old_state, new_state, [], [rename_op])
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.rename_column"
        assert ops[0].payload["oldName"] == "user_id"
        assert ops[0].payload["newName"] == "customer_id"

    def test_diff_column_type_change(self) -> None:
        """Should generate change_column_type operation for type change"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "age",
                                            "type": "INT",
                                            "nullable": True,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "age",
                                            "type": "BIGINT",
                                            "nullable": True,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.change_column_type"
        assert ops[0].target == "col_1"
        assert ops[0].payload["newType"] == "BIGINT"

    def test_diff_column_nullable_change(self) -> None:
        """Should generate set_nullable operation for nullable change"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": True,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.set_nullable"
        assert ops[0].target == "col_1"
        assert ops[0].payload["nullable"] is False

    def test_diff_multiple_changes(self) -> None:
        """Should generate operations for multiple changes"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "customers",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "BIGINT",
                                            "nullable": False,
                                        },
                                        {
                                            "id": "col_2",
                                            "name": "name",
                                            "type": "STRING",
                                            "nullable": True,
                                        },
                                        {
                                            "id": "col_3",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": True,
                                        },
                                    ],
                                },
                            ],
                        },
                        {
                            "id": "sch_2",
                            "name": "marketing",
                            "tables": [],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 3  # 1 schema + 2 columns
        op_types = [op.op for op in ops]
        assert "unity.add_schema" in op_types
        assert op_types.count("unity.add_column") == 2

    def test_diff_no_changes(self) -> None:
        """Should generate no operations when states are identical"""
        state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(state, state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 0
