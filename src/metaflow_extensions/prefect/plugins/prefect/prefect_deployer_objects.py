"""DeployedFlow and TriggeredRun objects for the Prefect Deployer plugin."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    pass


class PrefectTriggeredRun(TriggeredRun):
    """A Prefect flow run that was triggered via the Deployer API.

    Inherits ``.run`` from :class:`~metaflow.runner.deployer.TriggeredRun`, which polls
    Metaflow until the run with ``pathspec`` (``FlowName/prefect-<uuid>``) appears.
    """

    @property
    def prefect_ui(self) -> str | None:
        """URL to the Prefect UI for this flow run, if available."""
        # The pathspec is "FlowName/prefect-<uuid>"; extract the Prefect run UUID.
        try:
            _, run_id = self.pathspec.split("/")
            if run_id.startswith("prefect-"):
                prefect_run_id = run_id[len("prefect-"):]
                return f"http://localhost:4200/flow-runs/flow-run/{prefect_run_id}"
        except Exception:
            pass
        return None

    @property
    def run(self):
        """Retrieve the Run object, applying deployer env vars so local metadata works."""
        import os

        import metaflow
        from metaflow.exception import MetaflowNotFound

        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")

        old_meta = os.environ.get("METAFLOW_DEFAULT_METADATA")
        old_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        try:
            if meta_type:
                os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
                metaflow.metadata(meta_type)
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
            return metaflow.Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None
        except Exception:
            return None
        finally:
            if old_meta is None:
                os.environ.pop("METAFLOW_DEFAULT_METADATA", None)
            else:
                os.environ["METAFLOW_DEFAULT_METADATA"] = old_meta
            if old_sysroot is None:
                os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
            else:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old_sysroot

    @property
    def status(self) -> str | None:
        """Return a simple status string based on the underlying Metaflow run."""
        run = self.run
        if run is None:
            return "PENDING"
        if run.successful:
            return "SUCCEEDED"
        if run.finished:
            return "FAILED"
        return "RUNNING"


class PrefectDeployedFlow(DeployedFlow):
    """A Metaflow flow deployed as a named Prefect deployment."""

    TYPE: ClassVar[str | None] = "prefect"

    @property
    def id(self) -> str:
        """Deployment identifier encoding all info needed for ``from_deployment``."""
        import json
        return json.dumps({
            "name": self.name,
            "flow_name": self.flow_name,
            "flow_file": getattr(self.deployer, "flow_file", None),
        })

    @classmethod
    def from_deployment(cls, identifier: str, metadata: str | None = None) -> PrefectDeployedFlow:
        """Recover a PrefectDeployedFlow from a deployment name.

        Parameters
        ----------
        identifier : str
            Prefect deployment name (the value returned by ``deployed_flow.deployer.name``).
        metadata : str, optional
            Optional metadata string (unused, kept for API compatibility).

        Returns
        -------
        PrefectDeployedFlow
        """
        import asyncio
        import tempfile

        from metaflow.runner.deployer import Deployer, generate_fake_flow_file_contents

        deployment_name = identifier

        # Query Prefect to find the flow name for this deployment.
        async def _get_flow_info(name: str) -> tuple[str, str | None]:
            """Return (metaflow_class_name, project_name_or_None)."""
            try:
                from prefect.client.orchestration import get_client
                from prefect.client.schemas.filters import (
                    DeploymentFilter,
                    DeploymentFilterName,
                )

                async with get_client() as client:
                    deployments = await client.read_deployments(
                        deployment_filter=DeploymentFilter(
                            name=DeploymentFilterName(any_=[name])
                        )
                    )
                    if not deployments:
                        return name, None
                    deployment = deployments[0]
                    flow = await client.read_flow(deployment.flow_id)
                    # Prefect flow name may be "project.FlowName" — split out the parts.
                    prefect_flow_name = flow.name
                    if "." in prefect_flow_name:
                        project_name, mf_flow_name = prefect_flow_name.rsplit(".", 1)
                    else:
                        project_name = None
                        mf_flow_name = prefect_flow_name
                    return mf_flow_name, project_name
            except Exception:
                return name, None

        # Run async query; if already inside an event loop fall back gracefully.
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    flow_name, project_name = pool.submit(
                        asyncio.run, _get_flow_info(deployment_name)
                    ).result()
            else:
                flow_name, project_name = asyncio.run(_get_flow_info(deployment_name))
        except Exception:
            flow_name, project_name = deployment_name, None

        fake_flow_contents = generate_fake_flow_file_contents(
            flow_name=flow_name, param_info={}, project_name=project_name
        )

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
            tmp.write(fake_flow_contents)
            fake_flow_path = tmp.name

        deployer = Deployer(fake_flow_path).prefect()
        deployer.name = deployment_name
        deployer.flow_name = flow_name
        deployer.metadata = metadata or "{}"
        deployer.additional_info = {}
        return cls(deployer=deployer)

    def run(self, **kwargs) -> PrefectTriggeredRun:
        """Trigger a new run of this deployed flow.

        Parameters
        ----------
        **kwargs : Any
            Flow parameters as keyword arguments (e.g. ``message="hello"``).

        Returns
        -------
        PrefectTriggeredRun
        """
        # Convert kwargs to "key=value" strings for --run-param.
        run_params = tuple(f"{k}={v}" for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = dict(name=self.name, deployer_attribute_file=attribute_file_path)
            if run_params:
                trigger_kwargs["run_params"] = run_params
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(**trigger_kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return PrefectTriggeredRun(deployer=self.deployer, content=content)

        raise RuntimeError(
            f"Error triggering deployment {self.name!r} on Prefect for flow {self.deployer.flow_file!r}"
        )

    # Alias for backwards compatibility with test_utils.
    trigger = run
