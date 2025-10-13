import pytest
from pathlib import Path

@pytest.fixture
def temp_workspace(tmp_path):
    """Create a temporary workspace directory"""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return workspace

@pytest.fixture
def schemax_dir(temp_workspace):
    """Create .schemax directory"""
    schemax = temp_workspace / ".schemax"
    schemax.mkdir()
    return schemax

