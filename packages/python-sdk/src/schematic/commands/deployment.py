"""
Deployment Recording Command

Manually records deployment metadata to project.json.
For automated deployment with execution, use the apply command instead.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

from rich.console import Console

from ..storage_v4 import load_current_state, read_project, write_deployment

console = Console()


class DeploymentRecordingError(Exception):
    """Raised when deployment recording fails"""

    pass


def record_deployment_to_environment(
    workspace: Path,
    environment: str,
    version: Optional[str] = None,
    mark_deployed: bool = False,
) -> dict:
    """Record a deployment to an environment (manual tracking)

    This function manually tracks a deployment record in project.json.
    For automated deployment with execution, use the apply command instead.

    Args:
        workspace: Path to Schematic workspace
        environment: Target environment name (dev/test/prod)
        version: Optional version to deploy (default: latest snapshot or 'changelog')
        mark_deployed: If True, mark deployment as successful immediately

    Returns:
        Deployment record dictionary

    Raises:
        DeploymentRecordingError: If recording fails
    """
    try:
        console.print(f"Recording deployment to [cyan]{environment}[/cyan]...")

        # Load project and changelog
        state, changelog_data, provider = load_current_state(workspace)
        project = read_project(workspace)

        # Determine version to deploy
        if not version:
            if project.get("latestSnapshot"):
                version = project["latestSnapshot"]
                console.print(f"Using latest snapshot: [cyan]{version}[/cyan]")
            else:
                version = "changelog"
                console.print("Using [cyan]changelog[/cyan] (no snapshots yet)")

        # Get operations that were applied
        if version == "changelog":
            ops_applied = [
                op.get("id", f"op_{i}") for i, op in enumerate(changelog_data.get("ops", []))
            ]
            snapshot_id = None
        else:
            # For snapshot deployments, we'd need to track ops since last deployment
            # For now, mark as snapshot-based deployment
            ops_applied = []
            snapshot_id = version

        # Create deployment record
        deployment = {
            "id": f"deploy_{uuid4().hex[:8]}",
            "environment": environment,
            "ts": datetime.utcnow().isoformat() + "Z",
            "deployedBy": "cli",
            "snapshotId": snapshot_id,
            "opsApplied": ops_applied,
            "schemaVersion": version,
            "status": "success" if mark_deployed else "pending",
        }

        # Write deployment
        write_deployment(workspace, deployment)

        console.print("[green]✓[/green] Deployment recorded")
        console.print(f"  Deployment ID: {deployment['id']}")
        console.print(f"  Environment: {deployment['environment']}")
        console.print(f"  Version: {deployment['schemaVersion']}")
        console.print(f"  Operations: {len(deployment['opsApplied'])}")
        console.print(f"  Status: {deployment['status']}")

        return deployment

    except FileNotFoundError as e:
        raise DeploymentRecordingError(f"Project files not found: {e}") from e
    except Exception as e:
        raise DeploymentRecordingError(f"Failed to record deployment: {e}") from e
