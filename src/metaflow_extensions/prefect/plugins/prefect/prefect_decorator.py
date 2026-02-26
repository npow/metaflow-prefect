"""prefect_internal step decorator.

Auto-attached via ``--with=prefect_internal`` when Metaflow runs inside a
Prefect task.  Responsibilities:
  - Records Prefect flow-run-id and task-run-id in Metaflow metadata so runs
    are cross-referenced between the two systems.
  - Writes the foreach split-count to a side-car JSON file so the parent
    Prefect task can fan out the body steps dynamically.
"""

from __future__ import annotations

import json
import os
from typing import Any

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum

# ---------------------------------------------------------------------------
# Environment variables injected by the Prefect task runner at step launch.
# ---------------------------------------------------------------------------
ENV_FLOW_RUN_ID = "METAFLOW_PREFECT_FLOW_RUN_ID"
ENV_TASK_RUN_ID = "METAFLOW_PREFECT_TASK_RUN_ID"

# Path where foreach cardinality is written so the calling Prefect task can
# read it back and fan out the body tasks.
ENV_FOREACH_INFO_PATH = "METAFLOW_PREFECT_FOREACH_INFO_PATH"


class PrefectInternalDecorator(StepDecorator):
    """Internal decorator attached to every step running inside Prefect."""

    name = "prefect_internal"

    def task_pre_step(
        self,
        step_name: str,
        task_datastore: Any,
        metadata: Any,
        run_id: str,
        task_id: str,
        flow: Any,
        graph: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
        inputs: Any,
    ) -> None:
        meta: dict[str, str] = {}
        flow_run_id = os.environ.get(ENV_FLOW_RUN_ID)
        task_run_id = os.environ.get(ENV_TASK_RUN_ID)

        if flow_run_id:
            meta["prefect-flow-run-id"] = flow_run_id
        if task_run_id:
            meta["prefect-task-run-id"] = task_run_id

        entries = [
            MetaDatum(
                field=k,
                value=v,
                type=k,
                tags=["attempt_id:{0}".format(retry_count)],
            )
            for k, v in meta.items()
        ]
        metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self,
        step_name: str,
        flow: Any,
        graph: Any,
        is_task_ok: bool,
        retry_count: int,
        max_user_code_retries: int,
    ) -> None:
        if not is_task_ok:
            return

        if graph[step_name].type == "foreach":
            info_path = os.environ.get(ENV_FOREACH_INFO_PATH)
            if info_path:
                num_splits = getattr(flow, "_foreach_num_splits", None)
                if num_splits is not None:
                    with open(info_path, "w") as f:
                        json.dump({"num_splits": int(num_splits)}, f)
