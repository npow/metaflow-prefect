"""Metaflow Deployer plugin for Prefect.

Registers ``TYPE = "prefect"`` so that ``Deployer(flow_file).prefect(...)``
is available and the UX test suite can parametrise ``--scheduler-type=prefect``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Type

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from metaflow_extensions.prefect.plugins.prefect.prefect_deployer_objects import (
        PrefectDeployedFlow,
    )


class PrefectDeployer(DeployerImpl):
    """Deployer implementation for Prefect.

    Parameters
    ----------
    name : str, optional
        Prefect deployment name.  Defaults to the flow name.
    work_pool : str, optional
        Prefect work pool to use for this deployment.
    max_workers : int, optional
        Maximum parallel Prefect tasks (default 10).
    """

    TYPE: ClassVar[Optional[str]] = "prefect"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs) -> None:
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> Dict[str, str]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> Type["PrefectDeployedFlow"]:
        from .prefect_deployer_objects import PrefectDeployedFlow

        return PrefectDeployedFlow

    def create(self, **kwargs) -> "PrefectDeployedFlow":
        """Deploy this flow as a named Prefect deployment.

        Parameters
        ----------
        name : str
            Prefect deployment name.
        tags : list[str], optional
            Tags to attach to the deployment.
        work_pool : str, optional
            Prefect work pool name.
        paused : bool, optional
            Create the deployment in a paused state.
        max_workers : int, optional
            Maximum parallel Prefect tasks.
        deployer_attribute_file : str, optional
            Write deployment info JSON here (Metaflow Deployer API internal).

        Returns
        -------
        PrefectDeployedFlow
        """
        from .prefect_deployer_objects import PrefectDeployedFlow

        return self._create(PrefectDeployedFlow, **kwargs)
