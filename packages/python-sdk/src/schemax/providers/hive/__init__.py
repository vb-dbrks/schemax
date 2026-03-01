"""Hive provider package exports."""

from .provider import HiveProvider

hive_provider = HiveProvider()

__all__ = ["HiveProvider", "hive_provider"]
