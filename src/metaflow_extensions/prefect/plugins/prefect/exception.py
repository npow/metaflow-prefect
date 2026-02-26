"""Exceptions raised by the Metaflow-Prefect integration."""

from __future__ import annotations

from metaflow.exception import MetaflowException


class PrefectException(MetaflowException):
    """Base error for the Prefect integration."""

    headline = "Prefect error"


class NotSupportedException(PrefectException):
    """Raised when a Metaflow feature is not yet supported by this integration."""

    headline = "Not supported"
