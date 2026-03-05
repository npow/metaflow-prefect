"""Python source-code generator for the Metaflow-Prefect integration.

``generate_prefect_file`` is the single public function.  It takes a
``FlowSpec`` plus a ``PrefectFlowConfig`` and returns the complete text of a
self-contained Python file that, when executed, runs the Metaflow flow as a
Prefect flow.

Design
------
Code generation is split into four concerns, each returning a plain string:

1. ``_build_header``  — compile-time constants (FLOW_NAME, DATASTORE_TYPE, …)
2. ``_HELPERS``       — runtime helper functions, embedded verbatim
3. ``_build_task``    — one ``@task`` function per Metaflow step
4. ``_build_flow``    — the top-level ``@flow`` function that wires steps together

Dynamic sections use the **lines-list** pattern: build a ``list[str]``, then
``"\\n".join(lines)``.  This keeps the shape of the generated code visible
when reading this file — no mental reconstruction from ``emit/indent/dedent``
calls required.
"""

from __future__ import annotations

import textwrap
from datetime import datetime
from typing import Sequence

from metaflow_extensions.prefect.plugins.prefect._types import (
    FlowSpec,
    NodeType,
    ParameterSpec,
    PrefectFlowConfig,
    StepSpec,
)

_INDENT = "    "

# ---------------------------------------------------------------------------
# Static helper code — pasted verbatim into every generated file.
#
# These functions reference module-level constants (FLOW_NAME, DATASTORE_TYPE,
# etc.) that are written by _build_header.  Because they are static they are
# far more readable as a single string than as dozens of cb.emit() calls.
# ---------------------------------------------------------------------------

_HELPERS = textwrap.dedent('''\
    # ---------------------------------------------------------------------------
    # Runtime helpers (embedded — no external imports needed)
    # ---------------------------------------------------------------------------

    def _read_foreach_num_splits(run_id: str, step_name: str, task_id: str) -> int:
        """Read foreach split count from the Metaflow datastore after step completes."""
        try:
            from metaflow.datastore import FlowDataStore
            from metaflow.plugins import DATASTORES
            _impl = next(d for d in DATASTORES if d.TYPE == DATASTORE_TYPE)
            _root = _impl.get_datastore_root_from_config(lambda *a: None)
            _fds = FlowDataStore(FLOW_NAME, None, storage_impl=_impl, ds_root=_root)
            _tds = _fds.get_task_datastore(run_id, step_name, task_id, attempt=0, mode="r")
            return int(_tds["_foreach_num_splits"])
        except Exception as _e:
            raise RuntimeError(
                f"Could not read foreach split count for {step_name}/{task_id}: {_e}"
            ) from _e


    def _run_cmd(cmd: list[str], extra_env: dict[str, str] | None = None) -> None:
        """Execute cmd as a subprocess, inheriting stdout/stderr."""
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        subprocess.run(cmd, env=env, check=True)


    def _mf_artifact_names(run_id: str, step_name: str, task_id: str) -> list[str]:
        """Return user-defined artifact names from the Metaflow datastore (no values loaded)."""
        try:
            from metaflow.datastore import FlowDataStore
            from metaflow.plugins import DATASTORES
            _impl = next(d for d in DATASTORES if d.TYPE == DATASTORE_TYPE)
            _root = _impl.get_datastore_root_from_config(lambda *a: None)
            _fds = FlowDataStore(FLOW_NAME, None, storage_impl=_impl, ds_root=_root)
            _tds = _fds.get_task_datastore(run_id, step_name, task_id, attempt=0, mode="r")
            _SKIP = {"name", "input"}  # Metaflow internal artifact names
            return [n for n in _tds if not n.startswith("_") and n not in _SKIP]
        except Exception:
            return []


    def _step_cmd(
        step_name: str,
        run_id: str,
        task_id: str,
        input_paths: str,
        retry_count: int = 0,
        max_user_code_retries: int = 0,
        split_index: int | None = None,
    ) -> list[str]:
        """Build the metaflow step command list."""
        cmd: list[str] = [
            sys.executable, FLOW_FILE,
            "--datastore", DATASTORE_TYPE,
            "--metadata", METADATA_TYPE,
            "--no-pylint",
            "--with=prefect_internal",
            "step", step_name,
            "--run-id", run_id,
            "--task-id", task_id,
            "--retry-count", str(retry_count),
            "--max-user-code-retries", str(max_user_code_retries),
            "--input-paths", input_paths,
        ]
        for _tag in TAGS:
            cmd += ["--tag", _tag]
        for _deco in WITH_DECORATORS:
            cmd += [f"--with={_deco}"]
        if NAMESPACE:
            cmd += ["--namespace", NAMESPACE]
        if split_index is not None:
            cmd += ["--split-index", str(split_index)]
        if CODE_PACKAGE_URL:
            cmd += ["--code-package-url", CODE_PACKAGE_URL]
        if CODE_PACKAGE_SHA:
            cmd += ["--code-package-sha", CODE_PACKAGE_SHA]
        if CODE_PACKAGE_METADATA:
            cmd += ["--code-package-metadata", CODE_PACKAGE_METADATA]
        return cmd
''')


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_prefect_file(spec: FlowSpec, cfg: PrefectFlowConfig) -> str:
    """Return the full Python source of a runnable Prefect flow file."""
    sections: list[str] = [
        _build_header(spec, cfg),
        _HELPERS,
    ]
    for step in spec.steps:
        sections.append(_build_task(step, spec, cfg))
    sections.append(_build_flow(spec, cfg))
    main_guard = "if __name__ == '__main__':\n" + _INDENT + "%s()" % _python_name(spec.name)
    sections.append(main_guard)
    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# Section 1 — header constants
# ---------------------------------------------------------------------------


def _build_header(spec: FlowSpec, cfg: PrefectFlowConfig) -> str:
    lines = [
        "# Generated by metaflow-prefect on %s" % datetime.now().isoformat(timespec="seconds"),
        "# Metaflow flow: %s" % spec.name,
        "# Do not edit this file — regenerate with: python %s prefect create <file>" % cfg.flow_file,
        "",
        "from __future__ import annotations",
        "",
        "import json",
        "import os",
        "import subprocess",
        "import sys",
        "import uuid",
        "from typing import Any",
        "",
        "from prefect import flow, task, get_run_logger",
        "from prefect.artifacts import create_markdown_artifact",
        "from prefect.context import get_run_context",
        "from prefect.task_runners import ThreadPoolTaskRunner",
        "",
        "# ---------------------------------------------------------------------------",
        "# Compile-time configuration",
        "# ---------------------------------------------------------------------------",
        "FLOW_FILE: str = %r" % cfg.flow_file,
        "FLOW_NAME: str = %r" % spec.name,
        "DATASTORE_TYPE: str = %r" % cfg.datastore_type,
        "METADATA_TYPE: str = %r" % cfg.metadata_type,
        "USERNAME: str = %r" % cfg.username,
        "TAGS: list[str] = %r" % list(spec.tags),
        "NAMESPACE: str | None = %r" % spec.namespace,
        "SCHEDULE_CRON: str | None = %r" % spec.schedule_cron,
        "WITH_DECORATORS: list[str] = %r" % list(cfg.with_decorators),
        "CODE_PACKAGE_URL: str = %r" % cfg.code_package_url,
        "CODE_PACKAGE_SHA: str = %r" % cfg.code_package_sha,
        "CODE_PACKAGE_METADATA: str = %r" % cfg.code_package_metadata,
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section 3 — per-step @task functions
# ---------------------------------------------------------------------------


def _build_task(step: StepSpec, spec: FlowSpec, cfg: PrefectFlowConfig) -> str:
    """Return the full source of the @task function for one Metaflow step."""
    body = _task_body_lines(step, spec)
    indented_body = [_INDENT + line if line else "" for line in body]
    return "\n".join([
        _task_decorator(step),
        "def %s(%s) -> %s:" % (
            _task_fn(step.name),
            _task_signature(step, spec),
            _task_return_type(step),
        ),
    ] + indented_body)


def _task_decorator(step: StepSpec) -> str:
    parts = ['name="%s"' % step.name, "retries=%d" % step.max_user_code_retries]
    if step.timeout_seconds is not None:
        parts.append("timeout_seconds=%d" % step.timeout_seconds)
    if step.retry_delay_seconds is not None:
        parts.append("retry_delay_seconds=%d" % step.retry_delay_seconds)
    return "@task(%s)" % ", ".join(parts)


def _task_signature(step: StepSpec, spec: FlowSpec) -> str:
    """Return the parameter list string for the @task function."""
    is_start = step.name == "start"
    foreach_body_steps = {
        s.out_funcs[0]
        for s in spec.steps
        if s.node_type == NodeType.FOREACH and s.out_funcs
    }
    if is_start:
        return "run_id: str, parameters: dict[str, Any]"
    if step.is_foreach_join:
        return "run_id: str, parent_step: str, task_ids: list[str]"
    if step.is_split_join:
        return "run_id: str, parent_task_ids: dict[str, str]"
    if step.name in foreach_body_steps:
        return "run_id: str, prev_task_id: str, split_index: int = 0"
    return "run_id: str, prev_task_id: str"


def _task_return_type(step: StepSpec) -> str:
    if step.node_type == NodeType.FOREACH:
        return "tuple[str, int]"
    return "str"


def _task_body_lines(step: StepSpec, spec: FlowSpec) -> list[str]:
    """Return the unindented body lines for a @task function."""
    is_start = step.name == "start"
    foreach_body_steps = {
        s.out_funcs[0]
        for s in spec.steps
        if s.node_type == NodeType.FOREACH and s.out_funcs
    }
    is_foreach_body = step.name in foreach_body_steps

    lines: list[str] = []
    lines.append("logger = get_run_logger()")

    # Resource hint comment — users must configure this at the Prefect work pool.
    resource_parts = []
    if step.resource_cpu is not None:
        resource_parts.append("cpu=%d" % step.resource_cpu)
    if step.resource_gpu is not None:
        resource_parts.append("gpu=%d" % step.resource_gpu)
    if step.resource_memory is not None:
        resource_parts.append("memory=%d MB" % step.resource_memory)
    if resource_parts:
        lines.append(
            "# NOTE: @resources(%s) — configure matching resources at the Prefect work pool."
            % ", ".join(resource_parts)
        )

    lines.append("task_id: str = uuid.uuid4().hex[:16]")
    lines.append("_extra_env: dict[str, str] = {}")
    lines += _ctx_inject_lines()

    if step.env_vars:
        lines.append("_extra_env.update(%r)" % dict(step.env_vars))

    # For the start step, the init command runs first and defines param_task_id,
    # which input_paths then references.  For all other steps the assignment is safe.
    if is_start:
        lines += _start_init_lines(spec)

    lines.append(_input_paths_line(step, spec))

    lines.append('logger.info(f"Metaflow step \'%s\' task_id={task_id}")' % step.name)
    lines.append("cmd = _step_cmd(")
    lines.append(_INDENT + "%r, run_id, task_id, input_paths," % step.name)
    lines.append(_INDENT + "max_user_code_retries=%d," % step.max_user_code_retries)
    if is_foreach_body:
        lines.append(_INDENT + "split_index=split_index,")
    lines.append(")")
    lines.append("_run_cmd(cmd, extra_env=_extra_env)")
    lines += _artifact_lines(step)

    if step.node_type == NodeType.FOREACH:
        lines.append("num_splits: int = _read_foreach_num_splits(run_id, %r, task_id)" % step.name)
        lines.append("return task_id, num_splits")
    else:
        lines.append("return task_id")

    return lines


def _input_paths_line(step: StepSpec, spec: FlowSpec) -> str:
    """Return the ``input_paths: str = ...`` assignment for a task body."""
    is_start = step.name == "start"
    if is_start:
        # _start_init_lines() will have already set param_task_id.
        return 'input_paths: str = f"{run_id}/_parameters/{param_task_id}"'
    if step.is_foreach_join:
        return (
            'input_paths: str = ",".join('
            'f"{run_id}/{parent_step}/{tid}" for tid in task_ids)'
        )
    if step.is_split_join:
        path_exprs = ", ".join(
            'f"{{run_id}}/%s/{{parent_task_ids[%r]}}"' % (p, p)
            for p in step.in_funcs
        )
        return 'input_paths: str = ",".join([%s])' % path_exprs
    parent = step.in_funcs[0] if step.in_funcs else "start"
    return 'input_paths: str = f"{run_id}/%s/{prev_task_id}"' % parent


def _ctx_inject_lines() -> list[str]:
    """Lines that inject the Prefect run context IDs into the subprocess env."""
    return [
        "try:",
        _INDENT + "_ctx = get_run_context()",
        _INDENT + '_extra_env["METAFLOW_PREFECT_FLOW_RUN_ID"] = str(_ctx.flow_run.id)',
        _INDENT + '_extra_env["METAFLOW_PREFECT_TASK_RUN_ID"] = str(_ctx.task_run.id)',
        "except Exception:",
        _INDENT + "pass",
    ]


def _start_init_lines(spec: FlowSpec) -> list[str]:
    """Lines that run the Metaflow ``init`` command to register flow parameters."""
    return [
        "# --- _parameters init task ---",
        "param_task_id: str = uuid.uuid4().hex[:16]",
        "init_cmd: list[str] = [",
        _INDENT + "sys.executable, FLOW_FILE,",
        _INDENT + '"--datastore", DATASTORE_TYPE,',
        _INDENT + '"--metadata", METADATA_TYPE,',
        _INDENT + '"--no-pylint",',
        _INDENT + '"init",',
        _INDENT + '"--run-id", run_id,',
        _INDENT + '"--task-id", param_task_id,',
        "]",
        "for _tag in TAGS:",
        _INDENT + 'init_cmd += ["--tag", _tag]',
        "init_env: dict[str, str] = os.environ.copy()",
        "if parameters:",
        _INDENT + 'init_env["METAFLOW_PARAMETERS"] = json.dumps(parameters)',
        "subprocess.run(init_cmd, env=init_env, check=True)",
    ]


def _artifact_lines(step: StepSpec) -> list[str]:
    """Lines that publish a Prefect markdown artifact listing Metaflow artifacts."""
    artifact_key = step.name.replace("_", "-")
    return [
        "_art_names = _mf_artifact_names(run_id, %r, task_id)" % step.name,
        '_md = f"## `%s` — {run_id}\\n\\n"' % step.name,
        "if _art_names:",
        _INDENT + "for _n in _art_names:",
        _INDENT * 2 + '_md += f"`{_n}`\\n"',
        _INDENT * 2 + '_md += "```python\\n"',
        _INDENT * 2 + "_md += f\"Task('{FLOW_NAME}/{run_id}/%s/{task_id}')['{_n}'].data\\n\"" % step.name,
        _INDENT * 2 + '_md += "```\\n\\n"',
        "else:",
        _INDENT + '_md += "*(no user artifacts)*\\n"',
        "create_markdown_artifact(key=%r, markdown=_md)" % artifact_key,
    ]


# ---------------------------------------------------------------------------
# Section 4 — @flow function
# ---------------------------------------------------------------------------


def _build_flow(spec: FlowSpec, cfg: PrefectFlowConfig) -> str:
    """Return the full source of the @flow function."""
    flow_name = _python_name(spec.name)
    decorator = _flow_decorator(spec, cfg)
    sig = _flow_signature(spec.parameters)

    body = _flow_body_lines(spec)
    indented_body = [_INDENT + line if line else "" for line in body]

    return "\n".join([
        decorator,
        "def %s(%s) -> None:" % (flow_name, sig),
    ] + indented_body)


def _flow_decorator(spec: FlowSpec, cfg: PrefectFlowConfig) -> str:
    parts = [
        "name=%r" % spec.name,
        "description=%r" % (spec.description or spec.name),
        "task_runner=ThreadPoolTaskRunner(max_workers=%d)" % cfg.max_workers,
    ]
    if cfg.workflow_timeout is not None:
        parts.append("timeout_seconds=%d" % cfg.workflow_timeout)
    return "@flow(%s)" % ", ".join(parts)


def _flow_body_lines(spec: FlowSpec) -> list[str]:
    """Return the unindented body lines for the @flow function."""
    lines: list[str] = []

    # Derive the Metaflow run_id from Prefect's flow-run UUID.
    lines.append("ctx = get_run_context()")
    lines.append('run_id: str = f"prefect-{ctx.flow_run.id}"')

    # Collect Metaflow parameters into a dict for the init command.
    if spec.parameters:
        param_items = ", ".join('%r: %s' % (p.name, p.name) for p in spec.parameters)
        lines.append("parameters: dict[str, Any] = {%s}" % param_items)
    else:
        lines.append("parameters: dict[str, Any] = {}")

    lines.append("")
    lines += _flow_wiring_lines(spec)
    return lines


def _flow_wiring_lines(spec: FlowSpec) -> list[str]:
    """Return lines that call each @task in topological order."""
    lines: list[str] = []

    # task_id_vars maps step_name → Python variable name holding its task_id.
    task_id_vars: dict[str, str] = {}

    # Map foreach step name → its immediate body step name.
    foreach_body: dict[str, str] = {
        s.name: s.out_funcs[0]
        for s in spec.steps
        if s.node_type == NodeType.FOREACH and s.out_funcs
    }

    for step in spec.steps:
        tid_var = "_tid_%s" % step.name
        is_start = step.name == "start"

        if is_start and step.node_type == NodeType.FOREACH:
            # start that fans out immediately: call it, then submit all body tasks.
            lines.append(
                "%s_pair: tuple[str, int] = %s(run_id, parameters)"
                % (tid_var, _task_fn(step.name))
            )
            lines.append("%s: str = %s_pair[0]" % (tid_var, tid_var))
            lines.append("%s_nsplits: int = %s_pair[1]" % (tid_var, tid_var))
            lines += _submit_foreach_body(tid_var, foreach_body[step.name])
            task_id_vars[step.name] = tid_var

        elif is_start:
            lines.append(
                "%s: str = %s(run_id, parameters)" % (tid_var, _task_fn(step.name))
            )
            task_id_vars[step.name] = tid_var

        elif step.is_foreach_join:
            body_step_name = foreach_body[_foreach_parent(step, spec)]
            body_var = "_tid_%s_list" % body_step_name
            lines.append(
                "%s: str = %s(run_id, %r, %s)"
                % (tid_var, _task_fn(step.name), body_step_name, body_var)
            )
            task_id_vars[step.name] = tid_var

        elif step.is_split_join:
            parent_ids = "{%s}" % ", ".join(
                "%r: %s" % (p, task_id_vars[p]) for p in step.in_funcs
            )
            lines.append(
                "%s: str = %s(run_id, %s)" % (tid_var, _task_fn(step.name), parent_ids)
            )
            task_id_vars[step.name] = tid_var

        elif step.node_type == NodeType.FOREACH:
            parent_var = task_id_vars[step.in_funcs[0]]
            lines.append(
                "%s_pair: tuple[str, int] = %s(run_id, %s)"
                % (tid_var, _task_fn(step.name), parent_var)
            )
            lines.append("%s: str = %s_pair[0]" % (tid_var, tid_var))
            lines.append("%s_nsplits: int = %s_pair[1]" % (tid_var, tid_var))
            lines += _submit_foreach_body(tid_var, foreach_body[step.name])
            task_id_vars[step.name] = tid_var

        elif step.name in foreach_body.values():
            # Already emitted inside the foreach block above; just register the var.
            task_id_vars[step.name] = "_tid_%s_list" % step.name

        else:
            # Linear, split branch, or end step.
            parent_var = task_id_vars[step.in_funcs[0]]
            lines.append(
                "%s: str = %s(run_id, %s)" % (tid_var, _task_fn(step.name), parent_var)
            )
            task_id_vars[step.name] = tid_var

    return lines


def _submit_foreach_body(foreach_tid_var: str, body_step_name: str) -> list[str]:
    """Lines that .submit() foreach body tasks and collect their results."""
    futures_var = "_futures_%s" % body_step_name
    results_var = "_tid_%s_list" % body_step_name
    return [
        "%s = [%s.submit(run_id, %s, split_index=_i) for _i in range(%s_nsplits)]"
        % (futures_var, _task_fn(body_step_name), foreach_tid_var, foreach_tid_var),
        "%s: list[str] = [_f.result() for _f in %s]" % (results_var, futures_var),
    ]


def _foreach_parent(join_step: StepSpec, spec: FlowSpec) -> str:
    """Return the name of the foreach step that this join closes."""
    parent_name = join_step.split_parents[-1]
    return next(s.name for s in spec.steps if s.name == parent_name)


# ---------------------------------------------------------------------------
# Small utilities shared by tasks and flow
# ---------------------------------------------------------------------------


def _task_fn(step_name: str) -> str:
    return "_step_%s" % step_name


def _python_name(flow_name: str) -> str:
    """Convert CamelCase flow name to snake_case."""
    result = []
    for i, ch in enumerate(flow_name):
        if ch.isupper() and i > 0:
            result.append("_")
        result.append(ch.lower())
    return "".join(result)


def _flow_signature(params: Sequence[ParameterSpec]) -> str:
    """Build the function parameter string from flow parameters.

    Required params (no default) are emitted before optional ones so the
    generated signature is always valid Python.
    """
    def _param_str(p: ParameterSpec) -> str:
        if p.required:
            return '%s: %s' % (p.name, p.type_name)
        if isinstance(p.default, str):
            return '%s: str = %r' % (p.name, p.default)
        if isinstance(p.default, bool):
            return '%s: bool = %r' % (p.name, p.default)  # bool before int
        if isinstance(p.default, int):
            return '%s: int = %r' % (p.name, p.default)
        if isinstance(p.default, float):
            return '%s: float = %r' % (p.name, p.default)
        return '%s: Any = %r' % (p.name, p.default)

    required = [_param_str(p) for p in params if p.required]
    optional = [_param_str(p) for p in params if not p.required]
    return ", ".join(required + optional)
