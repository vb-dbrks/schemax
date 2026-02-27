"""Load provider modules so they register with ProviderRegistry (side effect)."""

import schemax.providers  # noqa: F401


def ensure_providers_loaded() -> None:
    """Ensure provider modules are loaded; idempotent."""
    # Side effect: schemax.providers is imported at module load
    pass
