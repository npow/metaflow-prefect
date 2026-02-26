"""Shared dataclasses and enums for the Metaflow-Prefect integration.

These types are the internal data model that ``_graph.py`` produces and
``_codegen.py`` consumes.  They carry no business logic — they are plain,
immutable value objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class NodeType(str, Enum):
    """Mirror of Metaflow's internal graph-node types."""

    START = "start"
    LINEAR = "linear"
    SPLIT = "split"
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


@dataclass(frozen=True)
class ParameterSpec:
    """A single Metaflow flow parameter as seen at deploy time."""

    name: str
    default: object                    # evaluated default value
    description: str = ""
    type_name: str = "str"             # Python type name (str, int, float, bool)


@dataclass(frozen=True)
class FlowSpec:
    """Fully-analysed description of a Metaflow flow, ready for code generation."""

    name: str
    steps: tuple[StepSpec, ...]        # topological order
    parameters: tuple[ParameterSpec, ...]
    description: str = ""
    schedule_cron: str | None = None
    tags: tuple[str, ...] = field(default_factory=tuple)
    namespace: str | None = None


@dataclass(frozen=True)
class PrefectFlowConfig:
    """User-supplied options for the generated Prefect flow."""

    flow_file: str                     # absolute path to the Metaflow .py file
    datastore_type: str = "local"
    metadata_type: str = "local"
    code_package_url: str = ""
    code_package_sha: str = ""
    code_package_metadata: str = ""
    username: str = ""
    max_workers: int = 10
