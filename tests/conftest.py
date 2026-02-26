"""Shared pytest fixtures for the metaflow-prefect test suite."""
from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FLOWS_DIR = Path(__file__).parent / "flows"


def _load_flow_graph(flow_file: str) -> tuple[Any, Any]:
    """Import *flow_file*, instantiate the flow, return (graph, flow_instance).

    We do this by dynamically importing the module and calling the FlowSpec
    subclass under ``sys.argv = [flow_file, "--help"]`` (suppressed).
    """
    import importlib.util
    import io
    from contextlib import redirect_stderr, redirect_stdout

    spec = importlib.util.spec_from_file_location("_test_flow", flow_file)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]

    old_argv = sys.argv[:]
    sys.argv = [flow_file, "--help"]
    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            try:
                spec.loader.exec_module(mod)  # type: ignore[union-attr]
            except SystemExit:
                pass
    finally:
        sys.argv = old_argv

    # Find the FlowSpec subclass in the module.
    # The FlowSpec metaclass populates cls._graph and cls._flow_state at class
    # definition time — we can access these attributes directly on the class.
    from metaflow import FlowSpec

    for attr in dir(mod):
        cls = getattr(mod, attr)
        if (
            isinstance(cls, type)
            and issubclass(cls, FlowSpec)
            and cls is not FlowSpec
        ):
            # cls._graph is a class-level attribute set by FlowSpec.__init_subclass__
            graph = cls._graph

            # _flow_decorators is an instance property backed by cls._flow_state,
            # so we create a minimal proxy that satisfies analyze_graph's interface.
            class _FlowProxy:
                name = cls.__name__
                _graph = cls._graph

                def _get_parameters(self):  # type: ignore[override]
                    return cls._get_parameters()

                @property
                def _flow_decorators(self):  # type: ignore[override]
                    return cls._flow_decorators.fget(self)  # type: ignore[attr-defined]

                # Make the proxy look up attributes on cls for decorator access
                def __getattr__(self, item):  # type: ignore[misc]
                    return getattr(cls, item)

            # Bind _flow_state so the property getter works
            _FlowProxy._flow_state = cls._flow_state  # type: ignore[attr-defined]

            return graph, _FlowProxy()

    raise ValueError("No FlowSpec subclass found in %s" % flow_file)


# ---------------------------------------------------------------------------
# Flow fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def simple_flow_graph() -> tuple[Any, Any]:
    """(graph, flow) for SimpleFlow."""
    return _load_flow_graph(str(FLOWS_DIR / "simple_flow.py"))


@pytest.fixture(scope="session")
def branch_flow_graph() -> tuple[Any, Any]:
    """(graph, flow) for BranchFlow."""
    return _load_flow_graph(str(FLOWS_DIR / "branch_flow.py"))


@pytest.fixture(scope="session")
def foreach_flow_graph() -> tuple[Any, Any]:
    """(graph, flow) for ForeachFlow."""
    return _load_flow_graph(str(FLOWS_DIR / "foreach_flow.py"))


@pytest.fixture(scope="session")
def param_flow_graph() -> tuple[Any, Any]:
    """(graph, flow) for ParamFlow."""
    return _load_flow_graph(str(FLOWS_DIR / "param_flow.py"))


@pytest.fixture(scope="session")
def decorator_flow_graph() -> tuple[Any, Any]:
    """(graph, flow) for DecoratorFlow (@retry, @timeout, @environment)."""
    return _load_flow_graph(str(FLOWS_DIR / "decorator_flow.py"))


# ---------------------------------------------------------------------------
# Mock Metaflow infrastructure objects
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_datastore() -> MagicMock:
    """A mock Metaflow datastore with TYPE='local'."""
    ds = MagicMock()
    ds.TYPE = "local"
    return ds


@pytest.fixture
def mock_metadata() -> MagicMock:
    """A mock Metaflow metadata provider with TYPE='local'."""
    md = MagicMock()
    md.TYPE = "local"
    return md


@pytest.fixture
def mock_environment() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_prefect_flow_config(tmp_path: Path) -> Any:
    """A PrefectFlowConfig pointing at a temp flow file."""
    from metaflow_extensions.prefect.plugins.prefect._types import PrefectFlowConfig

    return PrefectFlowConfig(
        flow_file=str(tmp_path / "myflow.py"),
        datastore_type="local",
        metadata_type="local",
        username="testuser",
    )
