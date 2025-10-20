"""
Databricks Authentication Helpers

Provides authentication utilities for connecting to Databricks workspaces
using profiles from ~/.databrickscfg or environment variables.
"""

import os
from typing import Optional

from databricks.sdk import WorkspaceClient


class AuthenticationError(Exception):
    """Raised when authentication fails"""

    pass


def create_databricks_client(profile: Optional[str] = None) -> WorkspaceClient:
    """Create authenticated Databricks client

    Authentication priority:
    1. Profile from ~/.databrickscfg (if profile specified)
    2. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
    3. Default profile from ~/.databrickscfg

    Args:
        profile: Databricks profile name (e.g., "DEV", "PROD").
                If None, uses DEFAULT profile or environment variables.

    Returns:
        Authenticated WorkspaceClient instance

    Raises:
        AuthenticationError: If authentication fails or credentials not found

    Example:
        >>> client = create_databricks_client("DEV")
        >>> client.current_user.me()
    """
    try:
        # Create client with profile (databricks-sdk handles profile lookup)
        if profile:
            client = WorkspaceClient(profile=profile)
        else:
            # No profile specified - SDK will check env vars then DEFAULT profile
            client = WorkspaceClient()

        # Validate authentication by making a test API call
        validate_auth(client)

        return client

    except Exception as e:
        error_msg = _format_auth_error(e, profile)
        raise AuthenticationError(error_msg) from e


def validate_auth(client: WorkspaceClient) -> None:
    """Validate authentication by testing API call

    Args:
        client: WorkspaceClient to validate

    Raises:
        AuthenticationError: If authentication is invalid
    """
    try:
        # Test authentication with simple API call
        client.current_user.me()
    except Exception as e:
        raise AuthenticationError(f"Authentication validation failed: {e}") from e


def get_current_user(client: WorkspaceClient) -> str:
    """Get current authenticated user's email/username

    Args:
        client: Authenticated WorkspaceClient

    Returns:
        User email or username
    """
    try:
        user = client.current_user.me()
        return user.user_name or user.display_name or "unknown"
    except Exception:
        return "unknown"


def check_profile_exists(profile: str) -> bool:
    """Check if a Databricks profile exists in ~/.databrickscfg

    Args:
        profile: Profile name to check

    Returns:
        True if profile exists, False otherwise
    """
    config_path = os.path.expanduser("~/.databrickscfg")

    if not os.path.exists(config_path):
        return False

    try:
        with open(config_path, "r") as f:
            content = f.read()
            return f"[{profile}]" in content
    except Exception:
        return False


def get_workspace_url(client: WorkspaceClient) -> Optional[str]:
    """Get workspace URL from client configuration

    Args:
        client: WorkspaceClient instance

    Returns:
        Workspace URL or None
    """
    try:
        config = client.config
        return config.host
    except Exception:
        return None


def _format_auth_error(error: Exception, profile: Optional[str]) -> str:
    """Format authentication error with helpful troubleshooting info

    Args:
        error: Original exception
        profile: Profile that was attempted (if any)

    Returns:
        Formatted error message with troubleshooting steps
    """
    error_str = str(error)

    messages = ["Failed to authenticate with Databricks"]

    if profile:
        messages.append(f"Profile: {profile}")

        if not check_profile_exists(profile):
            messages.append(f"\n⚠️  Profile '{profile}' not found in ~/.databrickscfg")
            messages.append("\nTroubleshooting:")
            messages.append("  1. Check ~/.databrickscfg file exists")
            messages.append(f"  2. Verify [{profile}] section exists")
            messages.append("  3. Run 'databricks configure --profile {profile}'")
    else:
        messages.append("Profile: DEFAULT or environment variables")

        has_env_vars = bool(os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN"))
        has_default = check_profile_exists("DEFAULT")

        if not has_env_vars and not has_default:
            messages.append("\n⚠️  No authentication configured")
            messages.append("\nTroubleshooting:")
            messages.append("  Option 1 - Environment variables:")
            messages.append(
                "    export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com"
            )
            messages.append("    export DATABRICKS_TOKEN=dapi...")
            messages.append("  Option 2 - Profile:")
            messages.append("    databricks configure --profile DEFAULT")

    messages.append(f"\nError details: {error_str}")

    return "\n".join(messages)
