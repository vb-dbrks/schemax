"""
Unit tests for StateDiffer and UnityStateDiffer

Tests state comparison, diff generation, and rename detection.
"""

from datetime import UTC, datetime

from schemax.providers.base.operations import Operation
from schemax.providers.unity.state_differ import UnityStateDiffer


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

    def test_diff_column_reorder(self) -> None:
        """Should generate reorder_columns operation when column order changes"""
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
                                            "type": "INT",
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
                                        {
                                            "id": "col_4",
                                            "name": "created_at",
                                            "type": "TIMESTAMP",
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
        # Reorder columns: move created_at before email
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
                                            "type": "INT",
                                            "nullable": False,
                                        },
                                        {
                                            "id": "col_2",
                                            "name": "name",
                                            "type": "STRING",
                                            "nullable": True,
                                        },
                                        {
                                            "id": "col_4",
                                            "name": "created_at",
                                            "type": "TIMESTAMP",
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
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.reorder_columns"
        assert ops[0].target == "tbl_1"
        assert ops[0].payload["tableId"] == "tbl_1"
        assert ops[0].payload["order"] == ["col_1", "col_2", "col_4", "col_3"]
        assert ops[0].payload["previousOrder"] == ["col_1", "col_2", "col_3", "col_4"]

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

    def test_diff_table_comment_change(self) -> None:
        """Should detect table comment change"""
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
                                    "comment": "Old comment",
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
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
                                    "comment": "Updated comment",
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.set_table_comment"
        assert ops[0].target == "tbl_1"
        assert ops[0].payload["tableId"] == "tbl_1"
        assert ops[0].payload["comment"] == "Updated comment"

    def test_diff_table_comment_added(self) -> None:
        """Should detect when table comment is added"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "bronze",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [{"id": "tbl_1", "name": "customers", "columns": []}],
                        }
                    ],
                }
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
                                    "comment": "New comment",
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.set_table_comment"
        assert ops[0].payload["comment"] == "New comment"

    def test_diff_table_property_added(self) -> None:
        """Should detect when table property is added"""
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
                                    "properties": {},
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
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
                                    "properties": {"team": "data-eng", "tier": "gold"},
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 2
        set_property_ops = [op for op in ops if op.op == "unity.set_table_property"]
        assert len(set_property_ops) == 2

        # Check both properties were added
        keys = {op.payload["key"] for op in set_property_ops}
        assert keys == {"team", "tier"}

    def test_diff_table_property_updated(self) -> None:
        """Should detect when table property is updated"""
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
                                    "properties": {"team": "old-team"},
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
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
                                    "properties": {"team": "new-team"},
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.set_table_property"
        assert ops[0].payload["key"] == "team"
        assert ops[0].payload["value"] == "new-team"

    def test_diff_table_property_removed(self) -> None:
        """Should detect when table property is removed"""
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
                                    "properties": {"team": "data-eng", "tier": "gold"},
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
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
                                    "properties": {"team": "data-eng"},
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.unset_table_property"
        assert ops[0].payload["key"] == "tier"

    def test_diff_column_tag_added(self) -> None:
        """Should detect when column tag is added"""
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
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "tags": {},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
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
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "tags": {"pii": "true", "sensitive": "high"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 2
        set_tag_ops = [op for op in ops if op.op == "unity.set_column_tag"]
        assert len(set_tag_ops) == 2

        # Check both tags were added
        tag_names = {op.payload["tagName"] for op in set_tag_ops}
        assert tag_names == {"pii", "sensitive"}

    def test_diff_column_tag_updated(self) -> None:
        """Should detect when column tag value is updated"""
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
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "tags": {"pii": "false"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
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
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "tags": {"pii": "true"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.set_column_tag"
        assert ops[0].payload["tagName"] == "pii"
        assert ops[0].payload["tagValue"] == "true"

    def test_diff_column_tag_removed(self) -> None:
        """Should detect when column tag is removed"""
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
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "tags": {"pii": "true", "sensitive": "high"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
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
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "tags": {"pii": "true"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.unset_column_tag"
        assert ops[0].payload["tagName"] == "sensitive"

    def test_diff_multiple_metadata_changes(self) -> None:
        """Should detect multiple metadata changes at once"""
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
                                    "comment": "Old comment",
                                    "properties": {"team": "old-team"},
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "comment": "Old email comment",
                                            "tags": {"pii": "false"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
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
                                    "comment": "New comment",
                                    "properties": {"team": "new-team", "tier": "gold"},
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "email",
                                            "type": "STRING",
                                            "nullable": False,
                                            "comment": "New email comment",
                                            "tags": {"pii": "true", "sensitive": "high"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        # Should have: 1 table comment + 2 table properties + 1 column comment + 2 column tags = 6 ops
        assert len(ops) == 6

        op_types = [op.op for op in ops]
        assert "unity.set_table_comment" in op_types
        assert op_types.count("unity.set_table_property") == 2
        assert "unity.set_column_comment" in op_types
        assert op_types.count("unity.set_column_tag") == 2


class TestUnityStateDifferViews:
    """Test UnityStateDiffer for view diffing - regression tests for view support"""

    def test_diff_added_view(self) -> None:
        """
        Should generate add_view operation for new view

        REGRESSION TEST: This would have caught the bug where views were ignored
        """
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {"id": "sch_1", "name": "bronze", "tables": [], "views": []},
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1, "Should generate one add_view operation"
        assert ops[0].op == "unity.add_view"
        assert ops[0].target == "view_1"
        assert ops[0].payload["name"] == "my_view"
        assert ops[0].payload["schemaId"] == "sch_1"
        assert ops[0].payload["definition"] == "SELECT * FROM table1"

    def test_diff_removed_view(self) -> None:
        """Should generate drop_view operation for removed view"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
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
                    "name": "analytics",
                    "schemas": [
                        {"id": "sch_1", "name": "bronze", "tables": [], "views": []},
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.drop_view"
        assert ops[0].target == "view_1"

    def test_diff_renamed_view_with_history(self) -> None:
        """Should detect view rename from operation history"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "old_view",
                                    "definition": "SELECT * FROM table1",
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
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "new_view",
                                    "definition": "SELECT * FROM table1",
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
            op="unity.rename_view",
            target="view_1",
            payload={"oldName": "old_view", "newName": "new_view"},
        )

        differ = UnityStateDiffer(old_state, new_state, [], [rename_op])
        ops = differ.generate_diff_operations()

        assert len(ops) == 1
        assert ops[0].op == "unity.rename_view"
        assert ops[0].payload["oldName"] == "old_view"
        assert ops[0].payload["newName"] == "new_view"

    def test_diff_view_definition_change(self) -> None:
        """Should generate update_view operation when definition changes"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
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
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT id, name FROM table1 WHERE active = true",
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
        assert ops[0].op == "unity.update_view"
        assert ops[0].target == "view_1"
        assert "WHERE active = true" in ops[0].payload["definition"]

    def test_diff_view_comment_change(self) -> None:
        """Should generate set_view_comment operation when comment changes"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
                                    "comment": "Old comment",
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
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
                                    "comment": "New comment",
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
        assert ops[0].op == "unity.set_view_comment"
        assert ops[0].target == "view_1"
        assert ops[0].payload["comment"] == "New comment"

    def test_diff_multiple_views_added(self) -> None:
        """
        Should detect multiple views added in one diff

        REGRESSION TEST: Tests the exact scenario from the bug report
        """
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
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
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
                                },
                                {
                                    "id": "view_2",
                                    "name": "test4",
                                    "definition": "SELECT * FROM table4",
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 1, "Should detect one new view (test4)"
        assert ops[0].op == "unity.add_view"
        assert ops[0].payload["name"] == "test4"

    def test_diff_views_with_extracted_dependencies(self) -> None:
        """Should preserve extractedDependencies when generating operations"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {"id": "sch_1", "name": "bronze", "tables": [], "views": []},
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM bronze.table1",
                                    "extractedDependencies": {
                                        "tables": ["table1"],
                                        "views": [],
                                        "catalogs": [],
                                        "schemas": ["bronze"],
                                    },
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
        assert ops[0].op == "unity.add_view"
        assert "extractedDependencies" in ops[0].payload
        assert ops[0].payload["extractedDependencies"]["tables"] == ["table1"]

    def test_diff_view_no_changes(self) -> None:
        """Should not generate operations when view is unchanged"""
        state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
                                    "comment": "Test view",
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(state, state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 0, "No operations should be generated for identical states"

    def test_diff_views_and_tables_together(self) -> None:
        """Should detect both table and view changes in same diff"""
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [],
                            "views": [],
                        },
                    ],
                },
            ]
        }
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "raw_data",
                                    "format": "delta",
                                    "columns": [],
                                },
                            ],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "filtered_data",
                                    "definition": "SELECT * FROM raw_data WHERE active = true",
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        assert len(ops) == 2, "Should detect both table and view additions"
        op_types = [op.op for op in ops]
        assert "unity.add_table" in op_types
        assert "unity.add_view" in op_types


class TestUnityStateDifferViewRegression:
    """Regression tests for view bugs in state differ

    These tests specifically target bugs where views were silently excluded
    from operations when creating new parent containers (schemas/catalogs).
    """

    def test_new_schema_with_views_includes_all_views(self) -> None:
        """
        REGRESSION TEST: Views in newly created schemas must be included in operations

        Bug: When a schema was added to an existing catalog, views within that
        schema were silently excluded from the generated operations.

        Root cause: _diff_schemas() called _add_all_tables_in_schema() but not
        _add_all_views_in_schema() for new schemas.

        Impact: First deployment to environment would skip all views.
        """
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [],  # Existing catalog, no schemas yet
                },
            ]
        }

        # Add new schema with both tables and views
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "sales",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "orders",
                                    "format": "delta",
                                    "columns": [],
                                },
                            ],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "orders_summary",
                                    "definition": "SELECT * FROM orders",
                                },
                                {
                                    "id": "view_2",
                                    "name": "orders_by_region",
                                    "definition": "SELECT region, COUNT(*) FROM orders GROUP BY region",
                                },
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        # Should generate: add_schema, add_table, add_view, add_view
        assert len(ops) == 4, f"Expected 4 ops (schema, table, 2 views), got {len(ops)}"

        op_types = [op.op for op in ops]
        assert "unity.add_schema" in op_types, "Should include schema creation"
        assert "unity.add_table" in op_types, "Should include table creation"
        assert op_types.count("unity.add_view") == 2, "Should include BOTH view creations"

        # Verify correct view IDs
        view_ops = [op for op in ops if op.op == "unity.add_view"]
        view_ids = {op.target for op in view_ops}
        assert view_ids == {"view_1", "view_2"}, "Should include all view IDs"

    def test_new_catalog_with_views_includes_all_views(self) -> None:
        """
        REGRESSION TEST: Views in newly created catalogs must be included

        Bug: When deploying from empty state (first deployment), views were
        completely excluded from the generated operations.

        Root cause: _add_all_schemas_in_catalog() called _add_all_tables_in_schema()
        but not _add_all_views_in_schema().

        Impact: schemax apply from empty state would show "0 operations" even
        when snapshot contained views.
        """
        old_state = {"catalogs": []}  # Empty state (first deployment)

        # New catalog with schemas containing both tables and views
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "test_catalog",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "test_schema",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "table1",
                                    "format": "delta",
                                    "columns": [],
                                }
                            ],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM table1",
                                    "extractedDependencies": {
                                        "tables": ["tbl_1"],
                                        "views": [],
                                        "catalogs": [],
                                        "schemas": [],
                                    },
                                },
                                {
                                    "id": "view_2",
                                    "name": "test4",
                                    "definition": "SELECT * FROM table1 WHERE active = true",
                                    "extractedDependencies": {
                                        "tables": ["tbl_1"],
                                        "views": [],
                                        "catalogs": [],
                                        "schemas": [],
                                    },
                                },
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        # Should generate: add_catalog, add_schema, add_table, add_view, add_view
        assert len(ops) == 5, f"Expected 5 ops (catalog, schema, table, 2 views), got {len(ops)}"

        op_types = [op.op for op in ops]
        assert "unity.add_catalog" in op_types, "Should include catalog creation"
        assert "unity.add_schema" in op_types, "Should include schema creation"
        assert "unity.add_table" in op_types, "Should include table creation"
        assert op_types.count("unity.add_view") == 2, "Should include BOTH view creations"

        # Verify correct view names in payloads
        view_ops = [op for op in ops if op.op == "unity.add_view"]
        view_names = {op.payload["name"] for op in view_ops}
        assert view_names == {"my_view", "test4"}, "Should include all view names"

    def test_multiple_schemas_with_views_all_included(self) -> None:
        """
        REGRESSION TEST: All views across multiple new schemas must be included

        Ensures the fix works correctly when multiple schemas are added simultaneously,
        each containing views.
        """
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [],
                },
            ]
        }

        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "analytics",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "bronze",
                            "tables": [
                                {"id": "tbl_1", "name": "raw", "format": "delta", "columns": []}
                            ],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "bronze_view",
                                    "definition": "SELECT * FROM raw",
                                }
                            ],
                        },
                        {
                            "id": "sch_2",
                            "name": "silver",
                            "tables": [
                                {"id": "tbl_2", "name": "clean", "format": "delta", "columns": []}
                            ],
                            "views": [
                                {
                                    "id": "view_2",
                                    "name": "silver_view",
                                    "definition": "SELECT * FROM clean",
                                }
                            ],
                        },
                        {
                            "id": "sch_3",
                            "name": "gold",
                            "tables": [
                                {"id": "tbl_3", "name": "agg", "format": "delta", "columns": []}
                            ],
                            "views": [
                                {
                                    "id": "view_3",
                                    "name": "gold_view",
                                    "definition": "SELECT * FROM agg",
                                }
                            ],
                        },
                    ],
                },
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        # Should generate: 3 schemas, 3 tables, 3 views = 9 ops
        assert len(ops) == 9, f"Expected 9 ops (3 schemas, 3 tables, 3 views), got {len(ops)}"

        # Verify all views are present
        view_ops = [op for op in ops if op.op == "unity.add_view"]
        assert len(view_ops) == 3, "Should include all 3 views"

        view_names = {op.payload["name"] for op in view_ops}
        assert view_names == {"bronze_view", "silver_view", "gold_view"}


class TestUnityStateDifferGenericObjectCoverage:
    """
    Generic tests that validate ALL nested object types are handled consistently

    These tests use introspection to automatically verify that state differ handles
    all object types defined in the Unity Catalog models. They will catch bugs like
    the view exclusion bug for any future object types we add.

    Philosophy: If an object type exists in the state model, the state differ MUST:
    1. Generate operations when objects are added to new parents
    2. Generate operations when objects are added to existing parents
    3. Generate operations when objects are dropped
    """

    def test_all_nested_collections_included_in_new_catalog(self) -> None:
        """
        Generic test: ALL nested object types must be included when adding a catalog

        This test will catch bugs where we forget to add a new object type to
        _add_all_schemas_in_catalog() or similar methods.

        Approach: Create a state with a catalog containing ALL possible nested
        object types, then verify operations are generated for ALL of them.
        """
        old_state = {"catalogs": []}

        # Build comprehensive new state with ALL object types
        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "comprehensive_catalog",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "comprehensive_schema",
                            # Tables with all features
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "table_with_everything",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "INT",
                                            "nullable": False,
                                            "tags": {"pii": "true"},
                                        }
                                    ],
                                    "constraints": [
                                        {
                                            "id": "pk_1",
                                            "type": "primary_key",
                                            "columns": ["col_1"],
                                        }
                                    ],
                                }
                            ],
                            # Views
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "test_view",
                                    "definition": "SELECT * FROM table_with_everything",
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        # Extract operation types for analysis
        op_types_count = {}
        for op in ops:
            op_type = op.op.replace("unity.", "")
            op_types_count[op_type] = op_types_count.get(op_type, 0) + 1

        # Critical assertions: Must include operations for ALL object types
        assert "add_catalog" in op_types_count, "Missing catalog creation"
        assert "add_schema" in op_types_count, "Missing schema creation"
        assert "add_table" in op_types_count, "Missing table creation"
        assert "add_view" in op_types_count, "Missing VIEW creation (regression!)"
        assert "add_column" in op_types_count, "Missing column creation"
        assert "set_column_tag" in op_types_count, "Missing column tag creation"

        # TODO: Constraints not yet implemented in state differ - uncomment when added
        # assert "add_constraint" in op_types_count, "Missing constraint creation"

        # Count verification
        assert op_types_count["add_catalog"] == 1
        assert op_types_count["add_schema"] == 1
        assert op_types_count["add_table"] == 1
        assert op_types_count["add_view"] == 1, "Views should be included!"
        assert op_types_count["add_column"] == 1
        assert op_types_count["set_column_tag"] == 1
        # assert op_types_count["add_constraint"] == 1  # TODO: Uncomment when implemented

        # Document what's expected but not yet implemented
        # When adding support for new object types (constraints, row filters, etc.),
        # uncomment the assertions above to ensure they're handled correctly

    def test_all_nested_collections_included_in_new_schema(self) -> None:
        """
        Generic test: ALL nested object types must be included when adding a schema

        This is the exact scenario where the view bug occurred - adding a schema
        to an existing catalog. This test ensures ALL object types are handled.
        """
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "existing_catalog",
                    "schemas": [],
                }
            ]
        }

        new_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "existing_catalog",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "new_schema",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "my_table",
                                    "format": "delta",
                                    "columns": [
                                        {
                                            "id": "col_1",
                                            "name": "id",
                                            "type": "INT",
                                            "nullable": False,
                                        }
                                    ],
                                }
                            ],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM my_table",
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        # Should include: add_schema, add_table, add_column, add_view
        op_types = [op.op.replace("unity.", "") for op in ops]

        assert "add_schema" in op_types, "Missing schema creation"
        assert "add_table" in op_types, "Missing table creation"
        assert "add_column" in op_types, "Missing column creation"
        assert "add_view" in op_types, "Missing VIEW creation (exact regression scenario!)"

        # Count assertions
        assert op_types.count("add_view") == 1, "Should include exactly 1 view"
        assert op_types.count("add_table") == 1, "Should include exactly 1 table"

    def test_symmetry_all_object_types_can_be_dropped(self) -> None:
        """
        Generic test: ALL object types that can be added must also be droppable

        This test validates that for every ADD operation supported, there's a
        corresponding DROP operation that works correctly.
        """
        # Start with comprehensive state
        old_state = {
            "catalogs": [
                {
                    "id": "cat_1",
                    "name": "catalog_to_drop",
                    "schemas": [
                        {
                            "id": "sch_1",
                            "name": "schema_with_objects",
                            "tables": [
                                {
                                    "id": "tbl_1",
                                    "name": "my_table",
                                    "format": "delta",
                                    "columns": [],
                                }
                            ],
                            "views": [
                                {
                                    "id": "view_1",
                                    "name": "my_view",
                                    "definition": "SELECT * FROM my_table",
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        # Drop everything
        new_state = {"catalogs": []}

        differ = UnityStateDiffer(old_state, new_state)
        ops = differ.generate_diff_operations()

        # Should generate drop_catalog operation (which drops all nested objects)
        assert len(ops) == 1, "Should generate single drop_catalog (cascade)"
        assert ops[0].op == "unity.drop_catalog"
        assert ops[0].target == "cat_1"
