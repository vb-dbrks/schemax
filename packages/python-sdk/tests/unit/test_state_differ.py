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
