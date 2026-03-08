"""Shared dataclasses and enums for the Metaflow-Prefect integration.

These types are the internal data model that ``_graph.py`` produces and
``_codegen.py`` consumes.  They carry no business logic — they are plain,
immutable value objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class NodeType(str, Enum):
    """Mirror of Metaflow's internal graph-node types."""

    START = "start"
    LINEAR = "linear"
    SPLIT = "split"
    SPLIT_SWITCH = "split-switch"
    JOIN = "join"
    FOREACH = "foreach"
    END = "end"


@dataclass(frozen=True)
class StepSpec:
    """Compiled specification for a single Metaflow step.

    Produced by ``_graph.analyze_graph`` and consumed by ``_codegen``.
    """

    name: str
    node_type: NodeType
    in_funcs: tuple[str, ...]       # upstream step names
    out_funcs: tuple[str, ...]      # downstream step names
    split_parents: tuple[str, ...]  # ancestors that opened the current fork
    max_user_code_retries: int = 0
    is_foreach_join: bool = False   # join that closes a foreach
    is_split_join: bool = False     # join that closes a static split
    condition_switch: str | None = None  # name of the split-switch step this step merges; non-None → is a condition merge
    timeout_seconds: int | None = None        # from @timeout(seconds=N)
    retry_delay_seconds: int | None = None    # from @retry(minutes_between_retries=N)
    env_vars: tuple[tuple[str, str], ...] = ()  # from @environment(vars={...})
    resource_cpu: int | None = None            # from @resources(cpu=N)
    resource_gpu: int | None = None            # from @resources(gpu=N)
    resource_memory: int | None = None         # from @resources(memory=N) in MB


@dataclass(frozen=True)
class ParameterSpec:
    """A single Metaflow flow parameter as seen at deploy time."""

    name: str
    default: object                    # evaluated default value; None when required=True
    description: str = ""
    type_name: str = "str"             # Python type name (str, int, float, bool)
    required: bool = False             # True when Parameter(required=True) and no default


@dataclass(frozen=True)
class TriggerSpec:
    """A custom-event trigger from @trigger(event=...)."""

    event_name: str
    parameter_map: tuple[tuple[str, str], ...] = ()  # (flow_param, event_field) pairs


@dataclass(frozen=True)
class TriggerOnFinishSpec:
    """A flow-completion trigger from @trigger_on_finish(flow=...)."""

    flow_name: str  # Prefect flow name to watch for completion


@dataclass(frozen=True)
class FlowSpec:
    """Fully-analysed description of a Metaflow flow, ready for code generation."""

    name: str
    steps: tuple[StepSpec, ...]        # topological order
    parameters: tuple[ParameterSpec, ...]
    description: str = ""
    schedule_cron: str | None = None
    tags: tuple[str, ...] = ()
    namespace: str | None = None
    project_name: str | None = None    # from @project(name=...) if present
    triggers: tuple[TriggerSpec, ...] = ()           # from @trigger
    trigger_on_finishes: tuple[TriggerOnFinishSpec, ...] = ()  # from @trigger_on_finish


@dataclass(frozen=True)
class PrefectFlowConfig:
    """User-supplied options for the generated Prefect flow."""

    flow_file: str                     # absolute path to the Metaflow .py file
    datastore_type: str = "local"
    metadata_type: str = "local"
    code_package_url: str = ""
    code_package_sha: str = ""
    code_package_metadata: str = ""
    environment_type: str = "local"
    event_logger_type: str = "nullSidecarLogger"
    monitor_type: str = "nullSidecarMonitor"
    datastore_root: str | None = None
    username: str = ""
    max_workers: int = 10
    with_decorators: tuple[str, ...] = ()  # extra --with=<deco> injected on every step
    workflow_timeout: int | None = None    # from --workflow-timeout (seconds)
    origin_run_id: str | None = None       # from --clone-run-id (resume support)
    flow_config_value: str | None = None   # JSON-serialised METAFLOW_FLOW_CONFIG_VALUE
