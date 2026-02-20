"""Live integration tests: snapshot rebase (placeholder, skipped)."""

import pytest

from tests.utils.live_databricks import require_live_command_tests


@pytest.mark.integration
def test_live_snapshot_rebase_explicit_skip() -> None:
    """Live snapshot rebase requires git-rebase divergence setup; not run in this matrix."""
    require_live_command_tests()
    pytest.skip("Live snapshot rebase requires git-rebase divergence setup; not run in this matrix")
