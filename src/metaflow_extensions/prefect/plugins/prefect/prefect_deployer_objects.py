"""DeployedFlow and TriggeredRun objects for the Prefect Deployer plugin."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar, Optional

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    import metaflow
    import metaflow.runner.deployer_impl


class PrefectTriggeredRun(TriggeredRun):
    """A Prefect flow run that was triggered via the Deployer API.

    Inherits ``.run`` from :class:`~metaflow.runner.deployer.TriggeredRun`, which polls
    Metaflow until the run with ``pathspec`` (``FlowName/prefect-<uuid>``) appears.
    """

    @property
    def prefect_ui(self) -> Optional[str]:
        """URL to the Prefect UI for this flow run, if available."""
        # The pathspec is "FlowName/prefect-<uuid>"; extract the Prefect run UUID.
        try:
            _, run_id = self.pathspec.split("/")
            if run_id.startswith("prefect-"):
                prefect_run_id = run_id[len("prefect-"):]
                return "http://localhost:4200/flow-runs/flow-run/%s" % prefect_run_id
        except Exception:
            pass
        return None

    @property
    def status(self) -> Optional[str]:
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

    TYPE: ClassVar[Optional[str]] = "prefect"

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
        run_params = tuple("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(
                name=self.name,
                deployer_attribute_file=attribute_file_path,
                run_params=run_params,
            )

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
            "Error triggering deployment %r on Prefect for flow %r"
            % (self.name, self.deployer.flow_file)
        )
