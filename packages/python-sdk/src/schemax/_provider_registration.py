"""Load provider modules so they register with ProviderRegistry (side effect)."""

import schemax.providers as _providers


def ensure_providers_loaded() -> None:
    """Ensure provider modules are loaded; idempotent."""
    _ = _providers
