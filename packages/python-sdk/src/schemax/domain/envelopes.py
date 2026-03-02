"""Typed command envelope builders shared by CLI JSON commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True, frozen=True)
class EnvelopeError:
    """Machine-readable error entry for command envelopes."""

    code: str
    message: str
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the error entry to JSON-ready dictionary."""
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details is not None:
            payload["details"] = self.details
        return payload


@dataclass(slots=True, frozen=True)
class EnvelopeMeta:
    """Execution metadata shared by all command envelopes."""

    duration_ms: int
    executed_command: str
    exit_code: int | None

    def to_dict(self) -> dict[str, Any]:
        """Serialize metadata to JSON-ready dictionary."""
        return {
            "durationMs": self.duration_ms,
            "executedCommand": self.executed_command,
            "exitCode": self.exit_code,
        }


@dataclass(slots=True, frozen=True)
class CommandEnvelope:
    """Standardized command envelope structure for CLI JSON output."""

    command: str
    status: str
    data: Any
    warnings: list[str]
    errors: list[EnvelopeError]
    meta: EnvelopeMeta
    schema_version: str = "1"

    def to_dict(self) -> dict[str, Any]:
        """Serialize command envelope to JSON-ready dictionary."""
        return {
            "schemaVersion": self.schema_version,
            "command": self.command,
            "status": self.status,
            "data": self.data,
            "warnings": self.warnings,
            "errors": [error.to_dict() for error in self.errors],
            "meta": self.meta.to_dict(),
        }


def build_success_envelope(
    *,
    command: str,
    data: Any,
    warnings: list[str] | None,
    meta: EnvelopeMeta,
) -> dict[str, Any]:
    """Build a success envelope with normalized metadata."""
    envelope = CommandEnvelope(
        command=command,
        status="success",
        data=data,
        warnings=warnings or [],
        errors=[],
        meta=meta,
    )
    return envelope.to_dict()


def build_error_envelope(
    *,
    command: str,
    data: Any,
    error: EnvelopeError,
    warnings: list[str] | None,
    meta: EnvelopeMeta,
) -> dict[str, Any]:
    """Build an error envelope with normalized metadata."""
    envelope = CommandEnvelope(
        command=command,
        status="error",
        data=data,
        warnings=warnings or [],
        errors=[error],
        meta=meta,
    )
    return envelope.to_dict()
