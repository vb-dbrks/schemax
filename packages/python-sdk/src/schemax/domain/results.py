"""Typed workflow result envelopes used by CLI and SDK entrypoints."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class CommandResult:
    """Common command/service response payload."""

    success: bool
    code: str = "ok"
    message: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def as_json_dict(self) -> dict[str, Any]:
        """Return a stable machine-readable structure."""
        return {
            "success": self.success,
            "code": self.code,
            "message": self.message,
            "data": self.data,
        }
