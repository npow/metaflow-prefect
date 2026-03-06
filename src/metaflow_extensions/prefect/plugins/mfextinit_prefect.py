"""Metaflow extension registration for the Prefect integration.

Metaflow discovers this file via the ``metaflow_extensions`` namespace package
mechanism.  The descriptor lists tell Metaflow to:
  - add ``python flow.py prefect …`` CLI commands,
  - make the ``--with=prefect_internal`` step decorator available so it can
    be auto-attached when a step runs inside a Prefect task, and
  - register ``PrefectDeployer`` so that ``Deployer(flow_file).prefect(...)``
    is available (enables ``--scheduler-type=prefect`` in UX tests).
"""

CLIS_DESC = [
    ("prefect", ".prefect.prefect_cli.cli"),
]

STEP_DECORATORS_DESC = [
    ("prefect_internal", ".prefect.prefect_decorator.PrefectInternalDecorator"),
]

DEPLOYER_IMPL_PROVIDERS_DESC = [
    ("prefect", ".prefect.prefect_deployer.PrefectDeployer"),
]
