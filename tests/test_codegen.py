"""Tests for metaflow_extensions.prefect.plugins.prefect._codegen."""
from __future__ import annotations

import ast
from typing import Any

import pytest

from metaflow_extensions.prefect.plugins.prefect._codegen import (
    _flow_signature,
    _python_name,
    _task_fn,
    generate_prefect_file,
)
from metaflow_extensions.prefect.plugins.prefect._graph import analyze_graph
from metaflow_extensions.prefect.plugins.prefect._types import (
    FlowSpec,
    ParameterSpec,
    PrefectFlowConfig,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(**kwargs: Any) -> PrefectFlowConfig:
    defaults = dict(
        flow_file="/flows/myflow.py",
        datastore_type="local",
        metadata_type="local",
        username="tester",
    )
    defaults.update(kwargs)
    return PrefectFlowConfig(**defaults)


def _parse(src: str) -> ast.Module:
    """Assert *src* is valid Python and return the parsed AST."""
    return ast.parse(src, mode="exec")


def _top_level_names(tree: ast.Module) -> set[str]:
    """Collect names of all top-level function definitions."""
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            isinstance(parent, ast.Module)
            for parent in ast.walk(tree)
            if hasattr(parent, "body") and node in getattr(parent, "body", [])
        )
    }


def _fn_names_at_module_level(tree: ast.Module) -> set[str]:
    """Return names of all FunctionDefs that are direct children of the Module."""
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }


def _decorator_names(funcdef: ast.FunctionDef) -> list[str]:
    """Return decorator name strings for a FunctionDef."""
    names = []
    for dec in funcdef.decorator_list:
        if isinstance(dec, ast.Name):
            names.append(dec.id)
        elif isinstance(dec, ast.Call):
            func = dec.func
            if isinstance(func, ast.Name):
                names.append(func.id)
            elif isinstance(func, ast.Attribute):
                names.append(func.attr)
    return names


def _find_fn(tree: ast.Module, name: str) -> ast.FunctionDef | None:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


class TestTaskFn:
    def test_basic(self) -> None:
        assert _task_fn("start") == "_step_start"
        assert _task_fn("my_step") == "_step_my_step"


class TestPythonName:
    def test_camel_case(self) -> None:
        assert _python_name("SimpleFlow") == "simple_flow"
        assert _python_name("MyAwesomeFlow") == "my_awesome_flow"

    def test_single_word(self) -> None:
        assert _python_name("Flow") == "flow"

    def test_already_lower(self) -> None:
        assert _python_name("flow") == "flow"


class TestFlowSignature:
    def test_empty(self) -> None:
        assert _flow_signature([]) == ""

    def test_str_param(self) -> None:
        p = ParameterSpec(name="msg", default="hello", type_name="str")
        sig = _flow_signature([p])
        assert sig == "msg: str = 'hello'"

    def test_int_param(self) -> None:
        p = ParameterSpec(name="count", default=3, type_name="int")
        sig = _flow_signature([p])
        assert sig == "count: int = 3"

    def test_float_param(self) -> None:
        p = ParameterSpec(name="rate", default=0.5, type_name="float")
        sig = _flow_signature([p])
        assert sig == "rate: float = 0.5"

    def test_bool_param(self) -> None:
        p = ParameterSpec(name="flag", default=True, type_name="bool")
        sig = _flow_signature([p])
        assert sig == "flag: bool = True"

    def test_none_default(self) -> None:
        p = ParameterSpec(name="opt", default=None)
        sig = _flow_signature([p])
        assert "opt" in sig

    def test_required_param_no_default_in_sig(self) -> None:
        p = ParameterSpec(name="msg", default=None, type_name="str", required=True)
        sig = _flow_signature([p])
        assert "msg: str" in sig
        assert "=" not in sig

    def test_multiple_params(self) -> None:
        params = [
            ParameterSpec(name="msg", default="hi", type_name="str"),
            ParameterSpec(name="n", default=5, type_name="int"),
        ]
        sig = _flow_signature(params)
        assert "msg: str = 'hi'" in sig
        assert "n: int = 5" in sig
        assert sig.index("msg") < sig.index("n")


# ---------------------------------------------------------------------------
# generate_prefect_file — structural checks
# ---------------------------------------------------------------------------


class TestGeneratePrefectFileSimple:
    """Checks on the generated file for a simple linear flow."""

    @pytest.fixture
    def src(self, simple_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)  # raises SyntaxError if invalid

    def test_header_comment(self, src: str) -> None:
        assert "# Generated by metaflow-prefect" in src
        assert "SimpleFlow" in src

    def test_imports_present(self, src: str) -> None:
        assert "from prefect import flow, task, get_run_logger" in src
        assert "import subprocess" in src

    def test_task_functions_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        assert "_step_start" in fns
        assert "_step_process" in fns
        assert "_step_end" in fns

    def test_flow_function_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        assert "simple_flow" in fns

    def test_task_decorator_on_steps(self, src: str) -> None:
        tree = _parse(src)
        for fn_name in ("_step_start", "_step_process", "_step_end"):
            fn = _find_fn(tree, fn_name)
            assert fn is not None
            assert "task" in _decorator_names(fn), f"@task missing on {fn_name}"

    def test_flow_decorator_on_flow(self, src: str) -> None:
        tree = _parse(src)
        fn = _find_fn(tree, "simple_flow")
        assert fn is not None
        assert "flow" in _decorator_names(fn)

    def test_main_guard(self, src: str) -> None:
        assert "if __name__ == '__main__':" in src
        assert "simple_flow()" in src

    def test_helper_functions_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        assert "_read_foreach_num_splits" in fns
        assert "_run_cmd" in fns
        assert "_step_cmd" in fns
        # Old temp-file helpers must NOT appear.
        assert "_foreach_info_path" not in fns
        assert "_read_foreach_info" not in fns

    def test_config_constants(self, src: str) -> None:
        assert "FLOW_FILE" in src
        assert "DATASTORE_TYPE" in src
        assert "METADATA_TYPE" in src
        assert "CODE_PACKAGE_URL" in src
        assert "CODE_PACKAGE_SHA" in src
        assert "CODE_PACKAGE_METADATA" in src
        assert "ORIGIN_RUN_ID" in src

    def test_thread_pool_task_runner_imported(self, src: str) -> None:
        assert "ThreadPoolTaskRunner" in src

    def test_thread_pool_task_runner_in_flow_decorator(self, src: str) -> None:
        flow_deco_line = next(
            (line for line in src.splitlines() if line.startswith("@flow(")), None
        )
        assert flow_deco_line is not None
        assert "ThreadPoolTaskRunner" in flow_deco_line

    def test_max_workers_in_flow_decorator(self, src: str) -> None:
        flow_deco_line = next(
            (line for line in src.splitlines() if line.startswith("@flow(")), None
        )
        assert flow_deco_line is not None
        assert "max_workers=" in flow_deco_line


class TestGeneratePrefectFileBranch:
    """Checks on the generated file for a split/join flow."""

    @pytest.fixture
    def src(self, branch_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = branch_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_all_task_functions_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        for name in ("_step_start", "_step_branch_a", "_step_branch_b", "_step_join", "_step_end"):
            assert name in fns, f"{name} missing from generated file"

    def test_join_signature_uses_dict(self, src: str) -> None:
        """The split-join step should accept parent_task_ids: dict."""
        tree = _parse(src)
        fn = _find_fn(tree, "_step_join")
        assert fn is not None
        arg_names = [a.arg for a in fn.args.args]
        assert "parent_task_ids" in arg_names

    def test_flow_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        assert "branch_flow" in fns


class TestGeneratePrefectFileForeach:
    """Checks on the generated file for a foreach flow."""

    @pytest.fixture
    def src(self, foreach_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = foreach_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_foreach_step_returns_tuple(self, src: str) -> None:
        tree = _parse(src)
        fn = _find_fn(tree, "_step_start")
        assert fn is not None
        # Return annotation should mention tuple
        if fn.returns:
            annotation_src = ast.unparse(fn.returns)
            assert "tuple" in annotation_src.lower()

    def test_foreach_join_accepts_task_ids_list(self, src: str) -> None:
        tree = _parse(src)
        fn = _find_fn(tree, "_step_join_step")
        assert fn is not None
        arg_names = [a.arg for a in fn.args.args]
        assert "task_ids" in arg_names

    def test_list_comprehension_in_flow(self, src: str) -> None:
        """The foreach body should be called in a list comprehension."""
        assert "for _i in range(" in src

    def test_flow_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        assert "foreach_flow" in fns


class TestGeneratePrefectFileParams:
    """Checks on the generated file for a parametrised flow."""

    @pytest.fixture
    def src(self, param_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = param_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_params_in_flow_signature(self, src: str) -> None:
        tree = _parse(src)
        fn = _find_fn(tree, "param_flow")
        assert fn is not None
        arg_names = [a.arg for a in fn.args.args]
        assert "message" in arg_names
        assert "count" in arg_names

    def test_defaults_in_flow_signature(self, src: str) -> None:
        """Default values should appear in the generated signature."""
        assert "'hello'" in src
        assert "= 3" in src

    def test_parameters_dict_populated(self, src: str) -> None:
        """The flow body should build a parameters dict."""
        assert "'message': message" in src or '"message": message' in src


class TestGeneratePrefectFileConfig:
    """Checks that PrefectFlowConfig values are emitted correctly."""

    def _spec(self, simple_flow_graph: tuple[Any, Any]) -> FlowSpec:
        graph, flow = simple_flow_graph
        return analyze_graph(graph, flow)

    def test_datastore_type_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg(datastore_type="s3"))
        assert "'s3'" in src or '"s3"' in src

    def test_metadata_type_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg(metadata_type="service"))
        assert "'service'" in src or '"service"' in src

    def test_flow_file_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg(flow_file="/custom/path/flow.py"))
        assert "/custom/path/flow.py" in src

    def test_tags_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        # Tags are embedded from spec.tags; a flow without tags should have []
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg())
        assert "TAGS" in src

    def test_with_decorators_constant_present(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """WITH_DECORATORS constant should always appear in the generated file."""
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg())
        assert "WITH_DECORATORS" in src

    def test_with_decorators_empty_by_default(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg())
        assert "WITH_DECORATORS: list[str] = []" in src

    def test_with_decorators_values_emitted(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg(with_decorators=("sandbox", "resources:cpu=4")))
        assert "'sandbox'" in src
        assert "'resources:cpu=4'" in src

    def test_with_decorators_forwarded_in_step_cmd(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """The generated _step_cmd should loop over WITH_DECORATORS and emit --with flags."""
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg(with_decorators=("sandbox",)))
        assert "for _deco in WITH_DECORATORS" in src
        assert '"--with={_deco}"' in src or "'--with={_deco}'" in src or "f\"--with={_deco}\"" in src

    def test_workflow_timeout_in_flow_decorator(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg(workflow_timeout=3600))
        assert "timeout_seconds=3600" in src

    def test_no_workflow_timeout_by_default(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg())
        # timeout_seconds should NOT appear in the @flow decorator when not set
        flow_deco_line = next(
            (line for line in src.splitlines() if line.startswith("@flow(")), None
        )
        assert flow_deco_line is not None
        assert "timeout_seconds" not in flow_deco_line

    def test_code_package_url_propagated(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(
            spec, _make_cfg(code_package_url="s3://bucket/code.tgz")
        )
        assert "s3://bucket/code.tgz" in src
        assert "--code-package-url" in src

    def test_code_package_empty_by_default(self, simple_flow_graph: tuple[Any, Any]) -> None:
        spec = self._spec(simple_flow_graph)
        src = generate_prefect_file(spec, _make_cfg())
        assert "CODE_PACKAGE_URL: str = ''" in src


class TestDecoratorCodegen:
    """Codegen output for @retry, @timeout, @environment decorated steps."""

    @pytest.fixture
    def src(self, decorator_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = decorator_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_timeout_in_task_decorator(self, src: str) -> None:
        """@timeout(seconds=300) → timeout_seconds=300 on @task."""
        assert "timeout_seconds=300" in src

    def test_retry_delay_in_task_decorator(self, src: str) -> None:
        """@retry(minutes_between_retries=1) → retry_delay_seconds=60 on @task."""
        assert "retry_delay_seconds=60" in src

    def test_retry_count_in_task_decorator(self, src: str) -> None:
        assert "retries=2" in src

    def test_env_vars_in_task_body(self, src: str) -> None:
        """@environment vars appear as _extra_env.update({...}) in the task body."""
        assert "_extra_env.update(" in src
        assert "MY_VAR" in src
        assert "hello" in src

    def test_timeout_minutes_converted(self, src: str) -> None:
        """@timeout(minutes=5) → timeout_seconds=300."""
        # Both 300 values (start=300s, end=5*60=300) should appear
        assert src.count("timeout_seconds=300") == 2

    def test_no_env_vars_on_plain_step(self, src: str) -> None:
        """Steps without @environment must not emit _extra_env.update."""
        # The end step has no @environment decorator.
        # Count occurrences — only start has it, so exactly one update call.
        assert src.count("_extra_env.update(") == 1


class TestResourcesComment:
    """@resources values appear as a comment in the generated task body."""

    @pytest.fixture
    def src(self, resources_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = resources_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_resources_comment_present(self, src: str) -> None:
        assert "# NOTE: @resources(" in src
        assert "cpu=4" in src
        assert "memory=8192 MB" in src

    def test_gpu_comment_present(self, src: str) -> None:
        assert "gpu=1" in src


class TestRequiredParamCodegen:
    """Required parameters produce no default in the generated flow signature."""

    @pytest.fixture
    def src(self, required_param_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = required_param_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_required_param_has_no_default(self, src: str) -> None:
        tree = _parse(src)
        fn = _find_fn(tree, "required_param_flow")
        assert fn is not None
        arg_names = [a.arg for a in fn.args.args]
        assert "message" in arg_names
        # The default list excludes args without defaults; required params appear
        # before optional ones in the signature.
        defaults = fn.args.defaults
        args = fn.args.args
        # args with defaults are the LAST len(defaults) args
        required_args = args[: len(args) - len(defaults)]
        required_names = {a.arg for a in required_args}
        assert "message" in required_names

    def test_optional_param_retains_default(self, src: str) -> None:
        assert "count: int = 5" in src


class TestForeachConcurrency:
    """Foreach body tasks use .submit() for concurrent execution."""

    @pytest.fixture
    def src(self, foreach_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = foreach_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_submit_used_for_body(self, src: str) -> None:
        assert ".submit(" in src

    def test_result_collected(self, src: str) -> None:
        assert ".result()" in src

    def test_no_direct_body_call_in_comprehension(self, src: str) -> None:
        """The foreach body should NOT be called directly (only via .submit)."""
        import re
        # Direct call pattern: _step_foreach_step(run_id, ...split_index
        direct = re.search(r"_step_foreach_step\(run_id,.*split_index", src)
        assert direct is None, "foreach body called directly instead of via .submit()"


class TestNestedForeachCodegen:
    """Generated code for 2-level nested foreach flows."""

    @pytest.fixture
    def src(self, nested_foreach_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = nested_foreach_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_all_task_functions_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        for name in (
            "_step_start", "_step_outer_step", "_step_inner_step",
            "_step_inner_join", "_step_outer_join", "_step_end",
        ):
            assert name in fns, f"{name} missing from generated file"

    def test_outer_step_returns_tuple(self, src: str) -> None:
        """outer_step is a foreach — must return (task_id, num_splits)."""
        tree = _parse(src)
        fn = _find_fn(tree, "_step_outer_step")
        assert fn is not None
        if fn.returns:
            annotation_src = ast.unparse(fn.returns)
            assert "tuple" in annotation_src.lower()

    def test_inner_foreach_submitted_in_comprehension(self, src: str) -> None:
        """outer_step (inner foreach) should be .submit()'d for each outer split."""
        assert "_step_outer_step.submit(" in src

    def test_inner_body_submitted_in_loop(self, src: str) -> None:
        """inner_step should be .submit()'d inside a for loop over outer splits."""
        assert "_step_inner_step.submit(" in src

    def test_inner_join_called_per_outer_split(self, src: str) -> None:
        """inner_join is called inside the per-outer-split loop."""
        assert "_step_inner_join(" in src
        # The call site (not the def) must be indented (inside the for loop).
        call_lines = [
            line for line in src.splitlines()
            if "_step_inner_join(" in line and not line.lstrip().startswith("def ")
        ]
        assert call_lines, "No call to _step_inner_join found"
        assert call_lines[0].startswith("    "), (
            "inner_join call should be indented (inside for loop)"
        )

    def test_outer_join_called_with_inner_join_list(self, src: str) -> None:
        """outer_join receives the accumulated list of inner_join task IDs."""
        assert "_step_outer_join(" in src
        # Variable holds the list of inner_join task IDs accumulated across outer splits.
        assert "_tid_outer_join_list" in src

    def test_for_loop_over_outer_pairs(self, src: str) -> None:
        """Generated code contains a for loop iterating over outer foreach results."""
        assert "for _outer_step_tid, _outer_step_nsplits in _pairs_outer_step:" in src


class TestResourceTags:
    """@resources values are emitted as tags= on @task."""

    @pytest.fixture
    def src(self, resources_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = resources_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_cpu_tag_in_task_decorator(self, src: str) -> None:
        """resource:cpu tag present on step with @resources(cpu=4)."""
        assert "resource:cpu=4" in src

    def test_memory_tag_in_task_decorator(self, src: str) -> None:
        """resource:memory tag present on step with @resources(memory=8192)."""
        assert "resource:memory=8192" in src

    def test_gpu_tag_in_task_decorator(self, src: str) -> None:
        """resource:gpu tag present on step with @resources(gpu=1)."""
        assert "resource:gpu=1" in src

    def test_tags_kwarg_in_task_decorator(self, src: str) -> None:
        """tags= kwarg appears in @task() for steps with resources."""
        assert "tags=" in src

    def test_gpu_concurrency_tag(self, src: str) -> None:
        """task_run_concurrency_tags added for GPU steps."""
        assert 'task_run_concurrency_tags=["gpu"]' in src

    def test_no_concurrency_tag_without_gpu(self, src: str) -> None:
        """Steps without GPU must NOT get task_run_concurrency_tags."""
        # start has cpu+memory but no gpu — find its @task decorator line
        tree = _parse(src)
        fn = _find_fn(tree, "_step_start")
        assert fn is not None
        # Reconstruct the decorator source
        start_src = ast.unparse(fn)
        assert "task_run_concurrency_tags" not in start_src


class TestTripleForeachCodegen:
    """Generated code for 3-level nested foreach flows (arbitrary depth)."""

    @pytest.fixture
    def src(self, triple_foreach_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = triple_foreach_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_all_task_functions_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        for name in (
            "_step_start", "_step_outer_step", "_step_middle_step",
            "_step_inner_step", "_step_inner_join", "_step_middle_join",
            "_step_outer_join", "_step_end",
        ):
            assert name in fns, f"{name} missing from generated file"

    def test_three_foreach_steps_return_tuple(self, src: str) -> None:
        """start, outer_step, middle_step are all foreach — must return tuple."""
        tree = _parse(src)
        for fn_name in ("_step_start", "_step_outer_step", "_step_middle_step"):
            fn = _find_fn(tree, fn_name)
            assert fn is not None
            if fn.returns:
                assert "tuple" in ast.unparse(fn.returns).lower(), (
                    f"{fn_name} should return tuple"
                )

    def test_middle_step_submitted_in_comprehension(self, src: str) -> None:
        """middle_step (2nd level foreach) is .submit()'d inside the outer loop."""
        assert "_step_middle_step.submit(" in src

    def test_inner_step_submitted(self, src: str) -> None:
        """inner_step (3rd level body) is .submit()'d."""
        assert "_step_inner_step.submit(" in src

    def test_two_for_loops_generated(self, src: str) -> None:
        """Three levels of nesting require two nested for loops in the flow body."""
        flow_body_lines = [
            line for line in src.splitlines()
            if line.lstrip().startswith("for ") and "_tid" in line
        ]
        assert len(flow_body_lines) >= 2, (
            f"Expected at least 2 for loops for 3-level nested foreach, "
            f"got {len(flow_body_lines)}: {flow_body_lines}"
        )

    def test_all_joins_called(self, src: str) -> None:
        """All three join steps are invoked in the generated code."""
        for fn in ("_step_inner_join(", "_step_middle_join(", "_step_outer_join("):
            assert fn in src, f"{fn} not found in generated source"

    def test_inner_join_innermost_indented(self, src: str) -> None:
        """inner_join call is the most-deeply indented join (inside 2 for loops)."""
        call_lines = [
            line for line in src.splitlines()
            if "_step_inner_join(" in line and not line.lstrip().startswith("def ")
        ]
        assert call_lines, "No call to _step_inner_join found"
        # Must be indented at least 8 spaces (inside 2 loops at 4-space indent each)
        assert len(call_lines[0]) - len(call_lines[0].lstrip()) >= 8, (
            f"inner_join call should be doubly-indented, got: {call_lines[0]!r}"
        )


class TestMidForeachCodegen:
    """Code-generation for a flow where the foreach is NOT the start step.

    This covers the ``elif step.node_type == NodeType.FOREACH`` wiring branch
    in ``_flow_wiring_lines`` (lines 630-643 of _codegen.py).
    """

    @pytest.fixture(scope="class")
    def src(self, mid_foreach_flow_graph: tuple) -> str:
        from metaflow_extensions.prefect.plugins.prefect._codegen import generate_prefect_file
        from metaflow_extensions.prefect.plugins.prefect._graph import analyze_graph
        from metaflow_extensions.prefect.plugins.prefect._types import PrefectFlowConfig

        graph, flow = mid_foreach_flow_graph
        spec = analyze_graph(graph, flow)
        cfg = PrefectFlowConfig(flow_file="/tmp/mid_foreach_flow.py")
        return generate_prefect_file(spec, cfg)

    def test_compiles_without_error(self, src: str) -> None:
        """Generated source is valid Python."""
        import ast
        ast.parse(src)

    def test_foreach_mid_returns_tuple(self, src: str) -> None:
        """The foreach_mid @task returns tuple[str, int] since it fans out."""
        import ast
        tree = ast.parse(src)
        fns = {
            n.name: n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef)
        }
        fn = fns.get("_step_foreach_mid")
        assert fn is not None, "_step_foreach_mid not found"
        assert fn.returns is not None
        assert "tuple" in ast.unparse(fn.returns).lower()

    def test_start_step_returns_str(self, src: str) -> None:
        """The start @task returns str (it's a plain linear step)."""
        import ast
        tree = ast.parse(src)
        fns = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        fn = fns.get("_step_start")
        assert fn is not None
        assert fn.returns is not None
        assert ast.unparse(fn.returns) == "str"

    def test_foreach_mid_wiring_uses_pair_variable(self, src: str) -> None:
        """Flow body uses _tid_foreach_mid_pair to unpack task_id and nsplits."""
        assert "_tid_foreach_mid_pair" in src

    def test_body_submitted_in_list_comprehension(self, src: str) -> None:
        """body tasks are submitted via list comprehension (.submit)."""
        assert "_step_body.submit" in src

    def test_all_steps_have_task_functions(self, src: str) -> None:
        """Every step in the flow has a corresponding @task function."""
        import ast
        tree = ast.parse(src)
        fns = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        for step in ("_step_start", "_step_foreach_mid", "_step_body", "_step_join", "_step_end"):
            assert step in fns, f"{step} missing from generated file"


class TestConditionFlowCodegen:
    """Generated code for a conditional (split-switch) flow."""

    @pytest.fixture(scope="class")
    def src(self, condition_flow_graph: tuple[Any, Any]) -> str:
        graph, flow = condition_flow_graph
        spec = analyze_graph(graph, flow)
        return generate_prefect_file(spec, _make_cfg())

    def test_is_valid_python(self, src: str) -> None:
        _parse(src)

    def test_all_task_functions_present(self, src: str) -> None:
        tree = _parse(src)
        fns = _fn_names_at_module_level(tree)
        for name in (
            "_step_start", "_step_high_branch", "_step_low_branch", "_step_join", "_step_end"
        ):
            assert name in fns, f"{name} missing from generated file"

    def test_start_returns_tuple_str_str(self, src: str) -> None:
        """The split-switch start step must return tuple[str, str]."""
        tree = _parse(src)
        fn = _find_fn(tree, "_step_start")
        assert fn is not None
        if fn.returns:
            annotation = ast.unparse(fn.returns)
            assert "tuple" in annotation.lower()
            assert "str" in annotation

    def test_condition_helper_present(self, src: str) -> None:
        """_read_condition_branch helper is embedded in the generated file."""
        assert "_read_condition_branch" in src

    def test_condition_helper_reads_transition(self, src: str) -> None:
        """The helper reads the '_transition' artifact from the datastore."""
        assert "_transition" in src

    def test_join_signature_uses_branch_args(self, src: str) -> None:
        """The condition join @task accepts branch_name and branch_task_id."""
        tree = _parse(src)
        fn = _find_fn(tree, "_step_join")
        assert fn is not None
        arg_names = [a.arg for a in fn.args.args]
        assert "branch_name" in arg_names
        assert "branch_task_id" in arg_names

    def test_join_input_paths_uses_branch_vars(self, src: str) -> None:
        """input_paths for the condition join uses branch_name and branch_task_id."""
        assert "branch_name" in src
        assert "branch_task_id" in src

    def test_flow_emits_if_elif_for_branches(self, src: str) -> None:
        """The flow body has an if/elif block routing to the taken branch."""
        assert "_tid_start_branch ==" in src or "_tid_start_branch ==" in src
        # Check for branch-routing pattern
        assert "if _tid_start_branch ==" in src or "if _tid_start_branch ==" in src

    def test_flow_emits_taken_variable(self, src: str) -> None:
        """The flow body sets a _tid_start_taken variable for the taken branch's task_id."""
        assert "_tid_start_taken" in src

    def test_flow_calls_join_with_branch_vars(self, src: str) -> None:
        """The join call passes branch and taken variables."""
        assert "_step_join(run_id, _tid_start_branch, _tid_start_taken)" in src

    def test_branch_steps_not_called_unconditionally(self, src: str) -> None:
        """Branch steps are only called inside the if/elif block, not unconditionally."""
        # No line should call _step_high_branch or _step_low_branch at the top level
        # (i.e., with no leading whitespace before the call).
        lines = src.splitlines()
        for line in lines:
            stripped = line.lstrip()
            if stripped.startswith("_step_high_branch(") or stripped.startswith("_step_low_branch("):
                # Must be indented (inside if/elif block)
                assert line != stripped, (
                    f"Branch step called unconditionally (no indent): {line!r}"
                )

    def test_else_raises_runtime_error(self, src: str) -> None:
        """The flow emits an else clause that raises RuntimeError for unknown branches."""
        assert "raise RuntimeError" in src
        assert "Unexpected condition branch" in src
