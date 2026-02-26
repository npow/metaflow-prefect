"""Tests for metaflow_extensions.prefect.plugins.prefect.prefect_flow."""
from __future__ import annotations

import ast
from typing import Any
from unittest.mock import MagicMock

import pytest

from metaflow_extensions.prefect.plugins.prefect.prefect_flow import PrefectFlow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_prefect_flow(
    graph: Any,
    flow: Any,
    *,
    flow_file: str = "/flows/myflow.py",
    tags: list[str] | None = None,
    namespace: str | None = None,
    datastore_type: str = "local",
    metadata_type: str = "local",
) -> PrefectFlow:
    mock_datastore = MagicMock()
    mock_datastore.TYPE = datastore_type
    mock_metadata = MagicMock()
    mock_metadata.TYPE = metadata_type

    return PrefectFlow(
        name=getattr(flow, "name", "TestFlow"),
        graph=graph,
        flow=flow,
        code_package_metadata="",
        code_package_sha="",
        code_package_url="",
        metadata=mock_metadata,
        flow_datastore=mock_datastore,
        environment=MagicMock(),
        event_logger=MagicMock(),
        monitor=MagicMock(),
        tags=tags,
        namespace=namespace,
        flow_file=flow_file,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestPrefectFlowConstruction:
    def test_can_construct(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        pf = _make_prefect_flow(graph, flow)
        assert pf is not None

    def test_stores_tags(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        pf = _make_prefect_flow(graph, flow, tags=["env:prod", "team:data"])
        assert pf._tags == ["env:prod", "team:data"]

    def test_stores_namespace(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        pf = _make_prefect_flow(graph, flow, namespace="production")
        assert pf._namespace == "production"

    def test_none_tags_becomes_empty_list(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        pf = _make_prefect_flow(graph, flow, tags=None)
        assert pf._tags == []


# ---------------------------------------------------------------------------
# compile() — output validity
# ---------------------------------------------------------------------------


class TestPrefectFlowCompile:
    def test_returns_string(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow).compile()
        assert isinstance(src, str)

    def test_valid_python(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow).compile()
        ast.parse(src, mode="exec")  # raises SyntaxError if invalid

    def test_valid_python_branch(self, branch_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = branch_flow_graph
        src = _make_prefect_flow(graph, flow).compile()
        ast.parse(src, mode="exec")

    def test_valid_python_foreach(self, foreach_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = foreach_flow_graph
        src = _make_prefect_flow(graph, flow).compile()
        ast.parse(src, mode="exec")

    def test_valid_python_params(self, param_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = param_flow_graph
        src = _make_prefect_flow(graph, flow).compile()
        ast.parse(src, mode="exec")


# ---------------------------------------------------------------------------
# compile() — tag and namespace overlay
# ---------------------------------------------------------------------------


class TestPrefectFlowTagsNamespace:
    def test_tags_appear_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow, tags=["my-tag"]).compile()
        assert "my-tag" in src

    def test_namespace_appears_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow, namespace="staging").compile()
        assert "staging" in src

    def test_no_tags_yields_empty_list(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow, tags=None).compile()
        assert "TAGS: list[str] = []" in src


# ---------------------------------------------------------------------------
# compile() — datastore / metadata config
# ---------------------------------------------------------------------------


class TestPrefectFlowConfig:
    def test_datastore_type_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow, datastore_type="s3").compile()
        assert "'s3'" in src or '"s3"' in src

    def test_metadata_type_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow, metadata_type="service").compile()
        assert "'service'" in src or '"service"' in src

    def test_flow_file_in_output(self, simple_flow_graph: tuple[Any, Any]) -> None:
        graph, flow = simple_flow_graph
        src = _make_prefect_flow(graph, flow, flow_file="/my/custom/flow.py").compile()
        assert "/my/custom/flow.py" in src
