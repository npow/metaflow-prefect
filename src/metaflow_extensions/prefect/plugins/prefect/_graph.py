"""Graph analysis utilities for the Metaflow-Prefect integration.

``analyze_graph`` is the only public function.  It walks the Metaflow DAG
and returns a ``FlowSpec`` containing an ordered list of ``StepSpec`` objects
that ``_codegen.py`` later turns into Python source.
"""

from __future__ import annotations

import warnings
from collections import deque
from typing import TYPE_CHECKING, Any

from metaflow.parameters import deploy_time_eval

from metaflow_extensions.prefect.plugins.prefect._types import (
    FlowSpec,
    NodeType,
    ParameterSpec,
    StepSpec,
    TriggerOnFinishSpec,
    TriggerSpec,
)
from metaflow_extensions.prefect.plugins.prefect.exception import NotSupportedException

if TYPE_CHECKING:
    # Only used for type-checker hints; not imported at runtime to avoid
    # pulling in heavy Metaflow internals.
    from metaflow.flowgraph import FlowGraph


def analyze_graph(
    graph: Any,  # metaflow.flowgraph.FlowGraph
    flow: Any,   # metaflow.FlowSpec subclass instance
) -> FlowSpec:
    """Convert a Metaflow ``FlowGraph`` into a ``FlowSpec``.

    Args:
        graph: A Metaflow ``FlowGraph`` (the ``._graph`` attribute on a flow).
        flow:  The Metaflow flow instance (used to read parameters and decorators).

    Returns:
        A ``FlowSpec`` with steps in topological order.

    Raises:
        NotSupportedException: For graph features not yet handled by this integration.
    """
    _validate(graph, flow)

    steps = _topological_order(graph)
    parameters = _extract_parameters(flow)
    schedule_cron = _extract_schedule(flow)
    tags_raw = getattr(flow, "_tags", None) or []
    project_name = _extract_project(flow)
    triggers = _extract_triggers(flow)
    trigger_on_finishes = _extract_trigger_on_finishes(flow)

    return FlowSpec(
        name=flow.name,
        steps=tuple(steps),
        parameters=tuple(parameters),
        description=(flow.__doc__ or "").strip(),
        schedule_cron=schedule_cron,
        tags=tuple(tags_raw),
        project_name=project_name,
        triggers=tuple(triggers),
        trigger_on_finishes=tuple(trigger_on_finishes),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate(graph: Any, flow: Any) -> None:
    """Raise NotSupportedException for features incompatible with Prefect."""
    # Step-level checks
    for node in graph:
        if node.parallel_foreach:
            raise NotSupportedException(
                "Deploying flows with @parallel to Prefect is not yet supported."
            )
        for deco in node.decorators:
            if deco.name == "batch":
                raise NotSupportedException(
                    "Step *%s* uses @batch which is not supported with Prefect. "
                    "Remove @batch or use --with=batch on the Prefect CLI instead." % node.name
                )
            if deco.name == "slurm":
                raise NotSupportedException(
                    "Step *%s* uses @slurm which is not supported with Prefect." % node.name
                )
            if deco.name == "condition":
                raise NotSupportedException(
                    "Step *%s* uses @condition which is not supported with Prefect. "
                    "Conditional branching via @condition produces incorrect generated "
                    "code and must be removed." % node.name
                )
            if deco.name == "resources":
                warnings.warn(
                    "Step *%s* uses @resources. Resource requirements are recorded as "
                    "Prefect task tags (resource:cpu=N, resource:gpu=N, etc.) but are "
                    "NOT enforced — configure matching resources on your Prefect work pool "
                    "to actually constrain execution." % node.name,
                    UserWarning,
                    stacklevel=2,
                )

    # @trigger and @trigger_on_finish are extracted and wired as Prefect automations
    # during deployment — no validation needed here.

    decos = getattr(flow._flow_decorators, "get", lambda *_: None)("exit_hook")
    if decos:
        raise NotSupportedException(
            "@exit_hook is not supported with Prefect deployments."
        )


def _max_user_code_retries(node: Any) -> int:
    """Return the maximum user-code retry count across all decorators on *node*."""
    max_retries = 0
    for deco in node.decorators:
        user_retries, _ = deco.step_task_retry_count()
        max_retries = max(max_retries, user_retries)
    return max_retries


def _step_retry_delay_seconds(node: Any) -> int | None:
    """Return retry delay in seconds from @retry(minutes_between_retries=N), or None."""
    for deco in node.decorators:
        if deco.name == "retry":
            mins = deco.attributes.get("minutes_between_retries", 0)
            if mins:
                return int(mins) * 60
    return None


def _step_timeout_seconds(node: Any) -> int | None:
    """Return timeout in seconds from @timeout(seconds=N) or @timeout(minutes=N), or None."""
    for deco in node.decorators:
        if deco.name == "timeout":
            secs = deco.attributes.get("seconds", 0) or 0
            mins = deco.attributes.get("minutes", 0) or 0
            hours = deco.attributes.get("hours", 0) or 0
            total = int(secs) + int(mins) * 60 + int(hours) * 3600
            if total > 0:
                return total
    return None


def _step_env_vars(node: Any) -> tuple[tuple[str, str], ...]:
    """Return (key, value) pairs from @environment(vars={...}), or empty tuple."""
    for deco in node.decorators:
        if deco.name == "environment":
            raw = deco.attributes.get("vars") or {}
            return tuple(sorted(raw.items()))
    return ()


def _step_resources(node: Any) -> tuple[int | None, int | None, int | None]:
    """Return (cpu, gpu, memory_mb) from @resources, or (None, None, None)."""
    for deco in node.decorators:
        if deco.name == "resources":
            cpu = deco.attributes.get("cpu")
            gpu = deco.attributes.get("gpu")
            memory = deco.attributes.get("memory")
            return (
                int(cpu) if cpu is not None else None,
                int(gpu) if gpu is not None else None,
                int(memory) if memory is not None else None,
            )
    return (None, None, None)


def _join_closes(graph: Any, node: Any, opener_type: str) -> bool:
    """True when *node* is a join step whose opening split is of *opener_type*."""
    return (
        node.type == "join"
        and bool(node.split_parents)
        and graph[node.split_parents[-1]].type == opener_type
    )


def _is_foreach_join(graph: Any, node: Any) -> bool:
    """True when *node* is a join step that closes a foreach."""
    return _join_closes(graph, node, "foreach")


def _is_split_join(graph: Any, node: Any) -> bool:
    """True when *node* is a join step that closes a static split."""
    return _join_closes(graph, node, "split")


def _topological_order(graph: Any) -> list[StepSpec]:
    """BFS from *start* yielding ``StepSpec`` objects in topological order."""
    visited: set[str] = set()
    result: list[StepSpec] = []
    queue: deque[str] = deque(["start"])

    while queue:
        name = queue.popleft()
        if name in visited:
            continue

        node = graph[name]

        # Only process *name* once all its predecessors have been processed.
        if any(p not in visited for p in node.in_funcs):
            queue.append(name)
            continue

        visited.add(name)

        resource_cpu, resource_gpu, resource_memory = _step_resources(node)
        # NodeType() raises ValueError for unknown types — fall back to LINEAR
        # so new Metaflow node types don't crash compilation.
        try:
            node_type = NodeType(node.type)
        except ValueError:
            node_type = NodeType.LINEAR
        spec = StepSpec(
            name=node.name,
            node_type=node_type,
            in_funcs=tuple(node.in_funcs),
            out_funcs=tuple(node.out_funcs),
            split_parents=tuple(node.split_parents),
            max_user_code_retries=_max_user_code_retries(node),
            is_foreach_join=_is_foreach_join(graph, node),
            is_split_join=_is_split_join(graph, node),
            timeout_seconds=_step_timeout_seconds(node),
            retry_delay_seconds=_step_retry_delay_seconds(node),
            env_vars=_step_env_vars(node),
            resource_cpu=resource_cpu,
            resource_gpu=resource_gpu,
            resource_memory=resource_memory,
        )
        result.append(spec)

        for child in node.out_funcs:
            if child not in visited:
                queue.append(child)

    return result


def _param_kwarg(param: Any, key: str) -> Any:
    """Read a kwarg from a Parameter, handling both stock and override-based subclasses.

    The open-source Metaflow Parameter stores values in ``param.kwargs``.
    Some extensions (e.g. nflx-metaflow) store them in ``param._override_kwargs``
    with ``param.kwargs`` left empty.  We check both.
    """
    value = param.kwargs.get(key)
    if value is None:
        value = getattr(param, "_override_kwargs", {}).get(key)
    return value


_TYPE_MAP: dict[str, str] = {
    "int": "int",
    "float": "float",
    "bool": "bool",
    "str": "str",
    "NoneType": "str",
}


def _extract_parameters(flow: Any) -> list[ParameterSpec]:
    """Pull parameters from the flow and evaluate their default values."""
    params: list[ParameterSpec] = []
    for _, param in flow._get_parameters():
        is_required = bool(_param_kwarg(param, "required"))
        raw_default = _param_kwarg(param, "default")

        if is_required and raw_default is None:
            # Infer type from the 'type' kwarg when there is no default.
            type_arg = _param_kwarg(param, "type")
            type_name = _TYPE_MAP.get(getattr(type_arg, "__name__", "NoneType"), "str")
            default = None
        else:
            default = deploy_time_eval(raw_default)
            type_name = _TYPE_MAP.get(type(default).__name__, "str")

        params.append(ParameterSpec(
            name=param.name,
            default=default,
            description=_param_kwarg(param, "help") or "",
            type_name=type_name,
            required=is_required and raw_default is None,
        ))
    return params


def _extract_schedule(flow: Any) -> str | None:
    """Return a cron string from an @schedule decorator, or None."""
    try:
        schedules = flow._flow_decorators.get("schedule")
    except Exception:
        return None
    if not schedules:
        return None
    s = schedules[0]
    if s.attributes.get("cron"):
        return s.attributes["cron"]
    if s.attributes.get("weekly"):
        return "0 0 * * 0"
    if s.attributes.get("hourly"):
        return "0 * * * *"
    if s.attributes.get("daily"):
        return "0 0 * * *"
    return None


def _extract_project(flow: Any) -> str | None:
    """Return the project name from @project(name=...), or None."""
    project_decos = getattr(flow._flow_decorators, "get", lambda *_: None)("project")
    if not project_decos:
        return None
    return project_decos[0].attributes.get("name") or None


def _extract_triggers(flow: Any) -> list[TriggerSpec]:
    """Return TriggerSpec entries from @trigger(event=...) or @trigger(events=[...])."""
    decos = getattr(flow._flow_decorators, "get", lambda *_: None)("trigger")
    if not decos:
        return []

    raw_triggers = getattr(decos[0], "triggers", None) or []
    result: list[TriggerSpec] = []
    for t in raw_triggers:
        if not isinstance(t, dict):
            continue
        name = t.get("name")
        if not name or not isinstance(name, str):
            warnings.warn(
                "@trigger entry has a non-string or deploy-time event name %r — "
                "skipping this trigger.  Evaluate the event name before deploying." % (name,),
                UserWarning,
                stacklevel=2,
            )
            continue
        raw_params = t.get("parameters") or {}
        param_map = tuple(sorted(raw_params.items())) if isinstance(raw_params, dict) else ()
        result.append(TriggerSpec(event_name=name, parameter_map=param_map))
    return result


def _extract_trigger_on_finishes(flow: Any) -> list[TriggerOnFinishSpec]:
    """Return TriggerOnFinishSpec entries from @trigger_on_finish(flow=...) or flows=[...]."""
    decos = getattr(flow._flow_decorators, "get", lambda *_: None)("trigger_on_finish")
    if not decos:
        return []

    raw_triggers = getattr(decos[0], "triggers", None) or []
    result: list[TriggerOnFinishSpec] = []
    for t in raw_triggers:
        if not isinstance(t, dict):
            continue
        # After _parse_fq_name, the dict has "flow" (plain name) and optionally "fq_name".
        flow_name = t.get("flow") or t.get("fq_name")
        if not flow_name or not isinstance(flow_name, str):
            warnings.warn(
                "@trigger_on_finish entry has a non-string or missing flow name %r — "
                "skipping this trigger." % (flow_name,),
                UserWarning,
                stacklevel=2,
            )
            continue
        result.append(TriggerOnFinishSpec(flow_name=flow_name))
    return result
