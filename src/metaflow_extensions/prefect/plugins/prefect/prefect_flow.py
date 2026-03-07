"""PrefectFlow: orchestrates analysis + code-generation for one Metaflow flow.

This is the single entry point used by the CLI.  It takes the same constructor
arguments as the Metaflow Airflow integration class so the CLI code has a
familiar shape.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

from metaflow.util import get_username

from metaflow_extensions.prefect.plugins.prefect._codegen import generate_prefect_file
from metaflow_extensions.prefect.plugins.prefect._graph import analyze_graph
from metaflow_extensions.prefect.plugins.prefect._types import FlowSpec, PrefectFlowConfig

_log = logging.getLogger(__name__)


class PrefectFlow:
    """Compile a Metaflow flow into a runnable Prefect flow Python file.

    Usage::

        pf = PrefectFlow(name, graph, flow, ..., flow_file="myflow.py")
        source = pf.compile()
        with open("output.py", "w") as f:
            f.write(source)
    """

    def __init__(
        self,
        name: str,
        graph: Any,
        flow: Any,
        code_package_metadata: str,
        code_package_sha: str,
        code_package_url: str,
        metadata: Any,
        flow_datastore: Any,
        environment: Any,
        event_logger: Any,
        monitor: Any,
        tags: list[str] | None = None,
        namespace: str | None = None,
        username: str | None = None,
        max_workers: int = 10,
        description: str | None = None,
        flow_file: str | None = None,
        workflow_timeout: int | None = None,
        with_decorators: list[str] | None = None,
        origin_run_id: str | None = None,
    ) -> None:
        self._graph = graph
        self._flow = flow
        self._name = name
        self._tags = list(tags or [])
        self._namespace = namespace
        self._max_workers = max_workers
        self._flow_file = flow_file or os.path.abspath(os.path.realpath(__file__))
        env_type = getattr(environment, "TYPE", "local")
        event_logger_type = getattr(event_logger, "TYPE", "nullSidecarLogger")
        monitor_type = getattr(monitor, "TYPE", "nullSidecarMonitor")
        datastore_root = getattr(
            getattr(flow_datastore, "_storage_impl", None), "datastore_root", None
        )

        self._cfg = PrefectFlowConfig(
            flow_file=self._flow_file,
            datastore_type=flow_datastore.TYPE,
            metadata_type=metadata.TYPE,
            code_package_url=code_package_url or "",
            code_package_sha=code_package_sha or "",
            code_package_metadata=code_package_metadata or "",
            environment_type=str(env_type) if isinstance(env_type, str) else "local",
            event_logger_type=(
                str(event_logger_type)
                if isinstance(event_logger_type, str)
                else "nullSidecarLogger"
            ),
            monitor_type=(
                str(monitor_type) if isinstance(monitor_type, str) else "nullSidecarMonitor"
            ),
            datastore_root=str(datastore_root) if datastore_root is not None else None,
            username=username or get_username(),
            max_workers=max_workers,
            with_decorators=tuple(with_decorators or []),
            workflow_timeout=workflow_timeout,
            origin_run_id=origin_run_id,
        )

    def compile(self) -> str:
        """Return the full Python source of the generated Prefect flow file."""
        from dataclasses import replace

        spec: FlowSpec = analyze_graph(self._graph, self._flow)
        # Overlay CLI-supplied tags/namespace (they may differ from flow decorators).
        # Use dataclasses.replace so all other fields (triggers, etc.) are preserved.
        overrides: dict = {}
        if self._tags:
            overrides["tags"] = tuple(self._tags)
        if self._namespace is not None:
            overrides["namespace"] = self._namespace
        if overrides:
            spec = replace(spec, **overrides)
        cmd_templates = self._build_step_cmd_templates(spec)
        return generate_prefect_file(spec, self._cfg, cmd_templates=cmd_templates)

    def _build_step_cmd_templates(self, spec: FlowSpec) -> dict[str, tuple[str, ...]]:
        """Build per-step command templates using initialized decorator objects."""
        from metaflow.runtime import CLIArgs as RuntimeCLIArgs

        run_token = "__MF_RUN_ID__"
        task_token = "__MF_TASK_ID__"
        input_token = "__MF_INPUT_PATHS__"
        retry_token = "__MF_RETRY_COUNT__"
        max_retry_token = "__MF_MAX_USER_CODE_RETRIES__"
        split_token = "__MF_SPLIT_INDEX__"

        templates: dict[str, tuple[str, ...]] = {}
        for step in spec.steps:
            try:
                node = self._graph[step.name]
                task = type("_MFTask", (), {})()
                task.entrypoint = [sys.executable, "-u", self._cfg.flow_file]
                task.flow = self._flow
                task.step = step.name
                task.metadata_type = self._cfg.metadata_type
                task.environment_type = self._cfg.environment_type
                task.datastore_type = self._cfg.datastore_type
                task.event_logger_type = self._cfg.event_logger_type
                task.monitor_type = self._cfg.monitor_type
                # Don't bake the compile-time CWD path into step templates.
                # The task body always sets METAFLOW_DATASTORE_SYSROOT_LOCAL=~
                # in _extra_env, so steps and the init command both use ~.
                task.datastore_sysroot = None
                task.decos = list(node.decorators)
                task.run_id = run_token
                task.task_id = task_token
                task.input_paths = [input_token]
                task.split_index = split_token
                task.retries = retry_token
                task.user_code_retries = max_retry_token
                task.tags = list(spec.tags)
                task.ubf_context = None
                task.clone_run_id = self._cfg.origin_run_id
                task.is_cloned = self._cfg.origin_run_id is not None
                task.clone_origin = None

                for deco in task.decos:
                    if hasattr(deco, "package_metadata") and not getattr(
                        deco, "package_metadata", None
                    ):
                        deco.package_metadata = self._cfg.code_package_metadata
                    if hasattr(deco, "package_sha") and not getattr(deco, "package_sha", None):
                        deco.package_sha = self._cfg.code_package_sha
                    if hasattr(deco, "package_url") and not getattr(deco, "package_url", None):
                        deco.package_url = self._cfg.code_package_url

                args = RuntimeCLIArgs(task)
                with_opts = list(args.top_level_options.get("with") or [])
                if "prefect_internal" not in with_opts:
                    with_opts.append("prefect_internal")
                for deco in self._cfg.with_decorators:
                    if deco not in with_opts:
                        with_opts.append(deco)
                args.top_level_options["with"] = with_opts

                for deco in task.decos:
                    deco.runtime_step_cli(
                        args,
                        retry_count=0,
                        max_user_code_retries=step.max_user_code_retries,
                        ubf_context=None,
                    )

                args.command_options["run-id"] = run_token
                args.command_options["task-id"] = task_token
                args.command_options["input-paths"] = input_token
                args.command_options["retry-count"] = retry_token
                args.command_options["max-user-code-retries"] = max_retry_token
                args.command_options["tag"] = list(spec.tags)
                args.command_options["namespace"] = self._namespace or spec.namespace or ""
                args.command_options["split-index"] = split_token

                templates[step.name] = tuple(args.get_args())
            except Exception as exc:
                _log.debug(
                    "Could not build command template for step %r; "
                    "falling back to generic command: %s",
                    step.name,
                    exc,
                )
                continue

        return templates
