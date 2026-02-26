"""Tests for metaflow_extensions.prefect.plugins.prefect._graph."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from metaflow_extensions.prefect.plugins.prefect._graph import analyze_graph
from metaflow_extensions.prefect.plugins.prefect._types import FlowSpec, NodeType
from metaflow_extensions.prefect.plugins.prefect.exception import NotSupportedException


class TestAnalyzeGraphSimple:
    """analyze_graph with the simple linear flow."""

    def test_returns_flow_spec(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """analyze_graph returns a FlowSpec."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        assert isinstance(spec, FlowSpec)

    def test_flow_name(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """FlowSpec.name matches the flow class name."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        assert spec.name == "SimpleFlow"

    def test_topological_order_starts_with_start(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """First step in topological order is always 'start'."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        assert spec.steps[0].name == "start"

    def test_topological_order_ends_with_end(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """Last step in topological order is always 'end'."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        assert spec.steps[-1].name == "end"

    def test_all_steps_present(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """All three steps (start, process, end) appear in the spec."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        names = {s.name for s in spec.steps}
        assert names == {"start", "process", "end"}

    def test_start_node_type(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """The start step has NodeType.START."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        start = next(s for s in spec.steps if s.name == "start")
        assert start.node_type == NodeType.START

    def test_end_node_type(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """The end step has NodeType.END."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        end = next(s for s in spec.steps if s.name == "end")
        assert end.node_type == NodeType.END

    def test_no_parameters(self, simple_flow_graph: tuple[Any, Any]) -> None:
        """SimpleFlow has no declared parameters."""
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        assert spec.parameters == ()


class TestAnalyzeGraphBranch:
    """analyze_graph with the branch/join flow."""

    def test_split_node_type(self, branch_flow_graph: tuple[Any, Any]) -> None:
        """The start step has type SPLIT when it fans out."""
        graph, flow = branch_flow_graph
        spec = analyze_graph(graph, flow)
        start = next(s for s in spec.steps if s.name == "start")
        # In Metaflow, a step that calls self.next(a, b) has type "split"
        assert start.node_type == NodeType.SPLIT

    def test_join_flags(self, branch_flow_graph: tuple[Any, Any]) -> None:
        """The join step is detected as a split-join (not a foreach-join)."""
        graph, flow = branch_flow_graph
        spec = analyze_graph(graph, flow)
        join = next(s for s in spec.steps if s.name == "join")
        assert join.is_split_join is True
        assert join.is_foreach_join is False

    def test_join_in_funcs(self, branch_flow_graph: tuple[Any, Any]) -> None:
        """The join step lists both branches as in_funcs."""
        graph, flow = branch_flow_graph
        spec = analyze_graph(graph, flow)
        join = next(s for s in spec.steps if s.name == "join")
        assert set(join.in_funcs) == {"branch_a", "branch_b"}

    def test_all_steps_present(self, branch_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = branch_flow_graph
        spec = analyze_graph(graph, flow)
        names = {s.name for s in spec.steps}
        assert names == {"start", "branch_a", "branch_b", "join", "end"}


class TestAnalyzeGraphForeach:
    """analyze_graph with the foreach flow."""

    def test_foreach_step_type(self, foreach_flow_graph: tuple[Any, Any]) -> None:
        """The start step has type FOREACH when it fans out with foreach=."""
        graph, flow = foreach_flow_graph
        spec = analyze_graph(graph, flow)
        start = next(s for s in spec.steps if s.name == "start")
        assert start.node_type == NodeType.FOREACH

    def test_foreach_join_flag(self, foreach_flow_graph: tuple[Any, Any]) -> None:
        """The join step that closes a foreach is detected correctly."""
        graph, flow = foreach_flow_graph
        spec = analyze_graph(graph, flow)
        join = next(s for s in spec.steps if s.name == "join_step")
        assert join.is_foreach_join is True
        assert join.is_split_join is False


class TestAnalyzeGraphParams:
    """analyze_graph with the parametrised flow."""

    def test_parameters_extracted(self, param_flow_graph: tuple[Any, Any]) -> None:
        """Parameters are present in the FlowSpec."""
        graph, flow = param_flow_graph
        spec = analyze_graph(graph, flow)
        param_names = {p.name for p in spec.parameters}
        assert "message" in param_names
        assert "count" in param_names

    def test_parameter_defaults(self, param_flow_graph: tuple[Any, Any]) -> None:
        """Default values are evaluated at analysis time."""
        graph, flow = param_flow_graph
        spec = analyze_graph(graph, flow)
        msg_param = next(p for p in spec.parameters if p.name == "message")
        count_param = next(p for p in spec.parameters if p.name == "count")
        assert msg_param.default == "hello"
        assert count_param.default == 3


def _make_mock_graph(parallel_foreach: bool = False, deco_names: list[str] | None = None) -> Any:
    """Return a (graph, flow) mock pair for validation tests."""
    from unittest.mock import MagicMock

    deco_names = deco_names or []
    decos = []
    for dname in deco_names:
        d = MagicMock()
        d.name = dname
        decos.append(d)

    node = MagicMock()
    node.name = "start"
    node.parallel_foreach = parallel_foreach
    node.decorators = decos
    node.in_funcs = []
    node.out_funcs = []
    node.split_parents = []
    node.type = "start"

    graph = MagicMock()
    graph.__iter__ = MagicMock(return_value=iter([node]))
    graph.__getitem__ = MagicMock(return_value=node)

    flow = MagicMock()
    flow.name = "BadFlow"
    flow._get_parameters = MagicMock(return_value=[])
    flow._flow_decorators = {}

    return graph, flow


class TestValidation:
    """analyze_graph raises for unsupported features."""

    def test_parallel_foreach_raises(self) -> None:
        graph, flow = _make_mock_graph(parallel_foreach=True)
        with pytest.raises(NotSupportedException):
            analyze_graph(graph, flow)

    def test_batch_raises(self) -> None:
        graph, flow = _make_mock_graph(deco_names=["batch"])
        with pytest.raises(NotSupportedException, match="@batch"):
            analyze_graph(graph, flow)

    def test_slurm_raises(self) -> None:
        graph, flow = _make_mock_graph(deco_names=["slurm"])
        with pytest.raises(NotSupportedException, match="@slurm"):
            analyze_graph(graph, flow)

    def test_trigger_raises(self) -> None:
        graph, flow = _make_mock_graph()
        trigger = MagicMock()
        flow._flow_decorators = {"trigger": [trigger]}
        with pytest.raises(NotSupportedException, match="@trigger"):
            analyze_graph(graph, flow)

    def test_trigger_on_finish_raises(self) -> None:
        graph, flow = _make_mock_graph()
        t = MagicMock()
        flow._flow_decorators = {"trigger_on_finish": [t]}
        with pytest.raises(NotSupportedException, match="@trigger_on_finish"):
            analyze_graph(graph, flow)

    def test_exit_hook_raises(self) -> None:
        graph, flow = _make_mock_graph()
        e = MagicMock()
        flow._flow_decorators = {"exit_hook": [e]}
        with pytest.raises(NotSupportedException, match="@exit_hook"):
            analyze_graph(graph, flow)


class TestDecoratorExtraction:
    """StepSpec fields populated from @retry, @timeout, @environment."""

    def test_timeout_seconds_from_timeout_deco(self, decorator_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = decorator_flow_graph
        spec = analyze_graph(graph, flow)
        start = next(s for s in spec.steps if s.name == "start")
        assert start.timeout_seconds == 300

    def test_timeout_minutes_from_timeout_deco(self, decorator_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = decorator_flow_graph
        spec = analyze_graph(graph, flow)
        end = next(s for s in spec.steps if s.name == "end")
        assert end.timeout_seconds == 300  # 5 * 60

    def test_retry_delay_seconds(self, decorator_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = decorator_flow_graph
        spec = analyze_graph(graph, flow)
        start = next(s for s in spec.steps if s.name == "start")
        assert start.retry_delay_seconds == 60  # 1 minute

    def test_retry_count(self, decorator_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = decorator_flow_graph
        spec = analyze_graph(graph, flow)
        start = next(s for s in spec.steps if s.name == "start")
        assert start.max_user_code_retries == 2

    def test_env_vars_extracted(self, decorator_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = decorator_flow_graph
        spec = analyze_graph(graph, flow)
        start = next(s for s in spec.steps if s.name == "start")
        env = dict(start.env_vars)
        assert env.get("MY_VAR") == "hello"
        assert env.get("OTHER") == "world"

    def test_no_env_vars_on_plain_step(self, decorator_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = decorator_flow_graph
        spec = analyze_graph(graph, flow)
        end = next(s for s in spec.steps if s.name == "end")
        assert end.env_vars == ()

    def test_no_timeout_on_plain_step(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        for step in spec.steps:
            assert step.timeout_seconds is None

    def test_no_retry_delay_on_plain_step(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        spec = analyze_graph(graph, flow)
        for step in spec.steps:
            assert step.retry_delay_seconds is None
