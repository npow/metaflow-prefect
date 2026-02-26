"""Tests for metaflow_extensions.prefect.plugins.prefect._codegen."""
from __future__ import annotations

import ast
from typing import Any

import pytest

from metaflow_extensions.prefect.plugins.prefect._codegen import (
    _CB,
    _flow_signature,
    _python_name,
    _task_fn,
    generate_prefect_file,
)
from metaflow_extensions.prefect.plugins.prefect._graph import analyze_graph
from metaflow_extensions.prefect.plugins.prefect._types import (
    FlowSpec,
    NodeType,
    ParameterSpec,
    PrefectFlowConfig,
    StepSpec,
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
# _CB tests
# ---------------------------------------------------------------------------


class TestCB:
    def test_empty_build(self) -> None:
        cb = _CB()
        assert cb.build() == ""

    def test_single_line(self) -> None:
        cb = _CB()
        cb.emit("hello")
        assert cb.build() == "hello"

    def test_blank_line(self) -> None:
        cb = _CB()
        cb.emit("a")
        cb.emit()
        cb.emit("b")
        assert cb.build() == "a\n\nb"

    def test_indent_dedent(self) -> None:
        cb = _CB()
        cb.emit("def f():")
        cb.indent()
        cb.emit("pass")
        cb.dedent()
        cb.emit("x = 1")
        assert cb.build() == "def f():\n    pass\nx = 1"

    def test_nested_indent(self) -> None:
        cb = _CB()
        cb.indent().emit("level1")
        cb.indent().emit("level2")
        cb.dedent().dedent().emit("level0")
        assert cb.build() == "    level1\n        level2\nlevel0"

    def test_dedent_below_zero_clamps(self) -> None:
        cb = _CB()
        cb.dedent()  # should not go negative
        cb.emit("x")
        assert cb.build() == "x"

    def test_chaining(self) -> None:
        cb = _CB()
        result = cb.emit("a").emit("b").build()
        assert result == "a\nb"


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
        assert "_foreach_info_path" in fns
        assert "_read_foreach_info" in fns
        assert "_run_cmd" in fns
        assert "_step_cmd" in fns

    def test_config_constants(self, src: str) -> None:
        assert "FLOW_FILE" in src
        assert "DATASTORE_TYPE" in src
        assert "METADATA_TYPE" in src


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
