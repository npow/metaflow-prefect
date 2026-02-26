"""PrefectFlow: orchestrates analysis + code-generation for one Metaflow flow.

This is the single entry point used by the CLI.  It takes the same constructor
arguments as the Metaflow Airflow integration class so the CLI code has a
familiar shape.
"""

from __future__ import annotations

import os
from typing import Any

from metaflow.util import get_username

from metaflow_extensions.prefect.plugins.prefect._codegen import generate_prefect_file
from metaflow_extensions.prefect.plugins.prefect._graph import analyze_graph
from metaflow_extensions.prefect.plugins.prefect._types import FlowSpec, PrefectFlowConfig
from metaflow_extensions.prefect.plugins.prefect.exception import PrefectException


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
    ) -> None:
        self._graph = graph
        self._flow = flow
        self._name = name
        self._tags = list(tags or [])
        self._namespace = namespace
        self._max_workers = max_workers
        self._flow_file = flow_file or os.path.abspath(os.path.realpath(__file__))

        self._cfg = PrefectFlowConfig(
            flow_file=self._flow_file,
            datastore_type=flow_datastore.TYPE,
            metadata_type=metadata.TYPE,
            code_package_url=code_package_url or "",
            code_package_sha=code_package_sha or "",
            code_package_metadata=code_package_metadata or "",
            username=username or get_username(),
            max_workers=max_workers,
            with_decorators=tuple(with_decorators or []),
        )

    def compile(self) -> str:
        """Return the full Python source of the generated Prefect flow file."""
        spec: FlowSpec = analyze_graph(self._graph, self._flow)
        # Overlay CLI-supplied tags/namespace (they may differ from flow decorators)
        if self._tags or self._namespace:
            spec = FlowSpec(
                name=spec.name,
                steps=spec.steps,
                parameters=spec.parameters,
                description=spec.description,
                schedule_cron=spec.schedule_cron,
                tags=tuple(self._tags) if self._tags else spec.tags,
                namespace=self._namespace if self._namespace is not None else spec.namespace,
            )
        return generate_prefect_file(spec, self._cfg)
