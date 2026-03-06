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


    def _fallback_step_cmd(
        step_name: str,
        run_id: str,
        task_id: str,
        input_paths: str,
        retry_count: int = 0,
        max_user_code_retries: int = 0,
        split_index: int | None = None,
    ) -> list[str]:
        """Conservative fallback command when decorator-aware resolution fails."""
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


    def _materialize_cmd(
        template: tuple[str, ...],
        run_id: str,
        task_id: str,
        input_paths: str,
        retry_count: int,
        max_user_code_retries: int,
        split_index: int | None,
    ) -> list[str]:
        """Apply runtime values to a compile-time command template."""
        run_token = "__MF_RUN_ID__"
        task_token = "__MF_TASK_ID__"
        input_token = "__MF_INPUT_PATHS__"
        retry_token = "__MF_RETRY_COUNT__"
        max_retry_token = "__MF_MAX_USER_CODE_RETRIES__"
        split_token = "__MF_SPLIT_INDEX__"
        out: list[str] = []
        i = 0
        while i < len(template):
            tok = template[i]
            if tok == "--split-index" and i + 1 < len(template) and template[i + 1] == split_token:
                if split_index is None:
                    i += 2
                    continue
                out.append(tok)
                out.append(str(split_index))
                i += 2
                continue
            if tok == run_token:
                out.append(run_id)
            elif tok == task_token:
                out.append(task_id)
            elif tok == input_token:
                out.append(input_paths)
            elif tok == retry_token:
                out.append(str(retry_count))
            elif tok == max_retry_token:
                out.append(str(max_user_code_retries))
            elif tok == split_token:
                if split_index is not None:
                    out.append(str(split_index))
            else:
                out.append(tok)
            i += 1
        return out


    def _step_cmd(
        step_name: str,
        run_id: str,
        task_id: str,
        input_paths: str,
        retry_count: int = 0,
        max_user_code_retries: int = 0,
        split_index: int | None = None,
    ) -> list[str]:
        """Build a Metaflow step command honoring compile-time decorator state."""
        template = STEP_CMD_TEMPLATES.get(step_name)
        if template:
            return _materialize_cmd(
                template, run_id, task_id, input_paths, retry_count,
                max_user_code_retries, split_index,
            )
        return _fallback_step_cmd(
            step_name, run_id, task_id, input_paths,
            retry_count=retry_count,
            max_user_code_retries=max_user_code_retries,
            split_index=split_index,
        )
''')


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_prefect_file(
    spec: FlowSpec,
    cfg: PrefectFlowConfig,
    cmd_templates: dict[str, tuple[str, ...]] | None = None,
) -> str:
    """Return the full Python source of a runnable Prefect flow file."""
    sections: list[str] = [
        _build_header(spec, cfg, cmd_templates or {}),
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


def _build_header(
    spec: FlowSpec,
    cfg: PrefectFlowConfig,
    cmd_templates: dict[str, tuple[str, ...]] | None = None,
) -> str:
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
        "ENVIRONMENT_TYPE: str = %r" % cfg.environment_type,
        "EVENT_LOGGER_TYPE: str = %r" % cfg.event_logger_type,
        "MONITOR_TYPE: str = %r" % cfg.monitor_type,
        "DATASTORE_ROOT: str | None = %r" % cfg.datastore_root,
        "CODE_PACKAGE_URL: str = %r" % cfg.code_package_url,
        "CODE_PACKAGE_SHA: str = %r" % cfg.code_package_sha,
        "CODE_PACKAGE_METADATA: str = %r" % cfg.code_package_metadata,
        "USERNAME: str = %r" % cfg.username,
        "TAGS: list[str] = %r" % list(spec.tags),
        "NAMESPACE: str | None = %r" % spec.namespace,
        "SCHEDULE_CRON: str | None = %r" % spec.schedule_cron,
        "WITH_DECORATORS: list[str] = %r" % list(cfg.with_decorators),
        "STEP_CMD_TEMPLATES: dict[str, tuple[str, ...]] = %r" % dict(cmd_templates or {}),
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
    resource_tags = []
    if step.resource_cpu is not None:
        resource_tags.append("resource:cpu=%d" % step.resource_cpu)
    if step.resource_memory is not None:
        resource_tags.append("resource:memory=%d" % step.resource_memory)
    if step.resource_gpu is not None:
        resource_tags.append("resource:gpu=%d" % step.resource_gpu)
    if resource_tags:
        parts.append("tags=%r" % resource_tags)
        if step.resource_gpu is not None:
            parts.append('task_run_concurrency_tags=["gpu"]')
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
    # Ensure local metadata writes to $HOME/.metaflow/ regardless of CWD
    # (Prefect workers may run from a temp directory).
    lines.append('_extra_env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = os.path.expanduser("~")')
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
        'init_env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = os.path.expanduser("~")',
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

    # Build chains for all outermost foreach steps (supports arbitrary nesting depth).
    chains = _foreach_chains(spec)

    # Steps whose code is generated inside chain blocks — skip in main iteration.
    nested_skip: set[str] = set()
    for outer, chain in chains.items():
        for _foreach_name, body_name, join_name in chain:
            nested_skip.add(body_name)
            nested_skip.add(join_name)
        nested_skip.discard(outer)         # outermost foreach called at top level
        nested_skip.discard(chain[0][2])   # outermost join handled by is_foreach_join

    for step in spec.steps:
        tid_var = "_tid_%s" % step.name
        is_start = step.name == "start"

        if step.name in nested_skip:
            # Handled inside a chain block; task_id_vars already populated.
            pass

        elif is_start and step.node_type == NodeType.FOREACH:
            lines.append(
                "%s_pair: tuple[str, int] = %s(run_id, parameters)"
                % (tid_var, _task_fn(step.name))
            )
            lines.append("%s: str = %s_pair[0]" % (tid_var, tid_var))
            lines.append("%s_nsplits: int = %s_pair[1]" % (tid_var, tid_var))
            chain_lines, result_var, result_step = _chain_wiring_lines(
                chains[step.name], tid_var, tid_var + "_nsplits"
            )
            lines += chain_lines
            task_id_vars[result_step] = result_var
            task_id_vars[step.name] = tid_var

        elif is_start:
            lines.append(
                "%s: str = %s(run_id, parameters)" % (tid_var, _task_fn(step.name))
            )
            task_id_vars[step.name] = tid_var

        elif step.is_foreach_join:
            # in_funcs[0] is the direct predecessor at every nesting depth.
            parent_step_name = step.in_funcs[0]
            body_var = task_id_vars[parent_step_name]
            lines.append(
                "%s: str = %s(run_id, %r, %s)"
                % (tid_var, _task_fn(step.name), parent_step_name, body_var)
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
            chain_lines, result_var, result_step = _chain_wiring_lines(
                chains[step.name], tid_var, tid_var + "_nsplits"
            )
            lines += chain_lines
            task_id_vars[result_step] = result_var
            task_id_vars[step.name] = tid_var

        else:
            # Linear, split branch, or end step.
            parent_var = task_id_vars[step.in_funcs[0]]
            lines.append(
                "%s: str = %s(run_id, %s)" % (tid_var, _task_fn(step.name), parent_var)
            )
            task_id_vars[step.name] = tid_var

    return lines


def _foreach_chains(
    spec: FlowSpec,
) -> dict[str, list[tuple[str, str, str]]]:
    """Return {outermost_foreach: chain} for every outermost foreach in the flow.

    Each chain is a list of ``(foreach_name, body_name, join_name)`` tuples ordered
    outermost-first.  The chain ends when ``body_name`` is NOT itself a foreach step,
    so ``len(chain) == 1`` for a simple (non-nested) foreach.
    """
    # foreach step name → immediate body step name
    foreach_body: dict[str, str] = {
        s.name: s.out_funcs[0]
        for s in spec.steps
        if s.node_type == NodeType.FOREACH and s.out_funcs
    }
    # Foreach steps that are themselves the body of another foreach (nested)
    nested_foreach = {body for body in foreach_body.values() if body in foreach_body}
    # Outermost = foreach steps not nested inside another foreach
    outermost = [name for name in foreach_body if name not in nested_foreach]

    chains: dict[str, list[tuple[str, str, str]]] = {}
    for outer in outermost:
        chain: list[tuple[str, str, str]] = []
        current = outer
        while True:
            body = foreach_body[current]
            # The join for `current` is the is_foreach_join step whose split_parents[-1] == current
            join = next(
                s.name
                for s in spec.steps
                if s.is_foreach_join and s.split_parents and s.split_parents[-1] == current
            )
            chain.append((current, body, join))
            if body in foreach_body:
                current = body
            else:
                break
        chains[outer] = chain

    return chains


def _chain_wiring_lines(
    chain: list[tuple[str, str, str]],
    foreach_tid_var: str,
    foreach_nsplits_var: str,
    depth: int = 0,
    indent: str = "",
) -> tuple[list[str], str, str]:
    """Generate wiring lines for a foreach chain at arbitrary nesting depth.

    Parameters
    ----------
    chain : list of (foreach_name, body_name, join_name)
        Chain from current level down to innermost.  ``chain[0]`` is the level
        whose foreach task has already been called (task_id in ``foreach_tid_var``).
    foreach_tid_var, foreach_nsplits_var :
        Python variable names holding the task_id / nsplits of ``chain[0][0]``.
    depth : int
        Recursion depth — used to pick a unique loop-index variable.
    indent : str
        Indentation prefix for generated lines (grows by ``_INDENT`` per level).

    Returns
    -------
    lines : list[str]
        Lines to append to the flow body.
    result_var : str
        Name of the Python variable that holds the list of task IDs consumed by the
        join step that closes ``chain[0]`` (i.e. ``chain[0][2]``).
    result_step : str
        The step name whose task IDs populate ``result_var`` — passed as
        ``parent_step`` to the join function.
    """
    _IDX = ["_i", "_j", "_k", "_l", "_m", "_n", "_o", "_p"]
    idx = _IDX[depth] if depth < len(_IDX) else "_idx_%d" % depth

    _foreach_name, body_name, join_name = chain[0]

    if len(chain) == 1:
        # Innermost level: body_name is a regular (non-foreach) step.
        futures_var = "_futures_%s" % body_name
        result_var = "_tid_%s_list" % body_name
        lines = [
            indent + "%s = [%s.submit(run_id, %s, split_index=%s) for %s in range(%s)]"
            % (futures_var, _task_fn(body_name), foreach_tid_var, idx, idx, foreach_nsplits_var),
            indent + "%s: list[str] = [_f.result() for _f in %s]" % (result_var, futures_var),
        ]
        return lines, result_var, body_name

    # Nested: body_name is itself a foreach step — recurse.
    inner_chain = chain[1:]
    inner_join_name = inner_chain[0][2]

    body_futures_var = "_futures_%s" % body_name
    body_pairs_var = "_pairs_%s" % body_name
    body_tid_item = "_%s_tid" % body_name
    body_nsplits_item = "_%s_nsplits" % body_name
    outer_result_var = "_tid_%s_list" % join_name   # list of inner_join task IDs
    inner_join_single_var = "_%s_tid" % inner_join_name

    lines: list[str] = [
        indent + "%s = [%s.submit(run_id, %s, split_index=%s) for %s in range(%s)]"
        % (body_futures_var, _task_fn(body_name), foreach_tid_var, idx, idx, foreach_nsplits_var),
        indent + "%s = [_f.result() for _f in %s]" % (body_pairs_var, body_futures_var),
        indent + "%s: list[str] = []" % outer_result_var,
        indent + "for %s, %s in %s:" % (body_tid_item, body_nsplits_item, body_pairs_var),
    ]

    inner_lines, inner_result_var, inner_result_step = _chain_wiring_lines(
        inner_chain,
        body_tid_item,
        body_nsplits_item,
        depth + 1,
        indent + _INDENT,
    )
    lines += inner_lines

    lines.append(
        indent + _INDENT + "%s = %s(run_id, %r, %s)"
        % (inner_join_single_var, _task_fn(inner_join_name), inner_result_step, inner_result_var)
    )
    lines.append(
        indent + _INDENT + "%s.append(%s)" % (outer_result_var, inner_join_single_var)
    )

    return lines, outer_result_var, inner_join_name


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
