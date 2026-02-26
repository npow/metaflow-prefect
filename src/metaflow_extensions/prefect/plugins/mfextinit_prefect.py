"""Metaflow extension registration for the Prefect integration.

Metaflow discovers this file via the ``metaflow_extensions`` namespace package
mechanism.  The two descriptor lists tell Metaflow to:
  - add ``python flow.py prefect …`` CLI commands, and
  - make the ``--with=prefect_internal`` step decorator available so it can
    be auto-attached when a step runs inside a Prefect task.
"""

CLIS_DESC = [
    ("prefect", ".prefect.prefect_cli.cli"),
]

STEP_DECORATORS_DESC = [
    ("prefect_internal", ".prefect.prefect_decorator.PrefectInternalDecorator"),
]
