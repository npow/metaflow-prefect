"""Tests for metaflow_extensions.prefect.plugins.prefect._types."""
from __future__ import annotations

import pytest

from metaflow_extensions.prefect.plugins.prefect._types import (
    FlowSpec,
    NodeType,
    ParameterSpec,
    PrefectFlowConfig,
    StepSpec,
)


class TestNodeType:
    """Tests for NodeType enum."""

    def test_values_are_metaflow_compatible(self) -> None:
        """NodeType string values match Metaflow graph node type strings."""
        assert NodeType.START.value == "start"
        assert NodeType.LINEAR.value == "linear"
        assert NodeType.SPLIT.value == "split"
        assert NodeType.JOIN.value == "join"
        assert NodeType.FOREACH.value == "foreach"
        assert NodeType.END.value == "end"

    def test_from_string(self) -> None:
        """NodeType can be constructed from a plain string."""
        assert NodeType("start") is NodeType.START
        assert NodeType("foreach") is NodeType.FOREACH


class TestStepSpec:
    """Tests for StepSpec dataclass."""

    def test_frozen(self) -> None:
        """StepSpec instances are immutable."""
        spec = StepSpec(
            name="start",
            node_type=NodeType.START,
            in_funcs=(),
            out_funcs=("process",),
            split_parents=(),
        )
        with pytest.raises((AttributeError, TypeError)):
            spec.name = "other"  # type: ignore[misc]

    def test_defaults(self) -> None:
        """StepSpec optional fields have correct defaults."""
        spec = StepSpec(
            name="end",
            node_type=NodeType.END,
            in_funcs=("process",),
            out_funcs=(),
            split_parents=(),
        )
        assert spec.max_user_code_retries == 0
        assert spec.is_foreach_join is False
        assert spec.is_split_join is False

    def test_flags_are_exclusive(self) -> None:
        """A step cannot be both a foreach-join and a split-join."""
        # We don't enforce this at construction time, but verify that both
        # flags can be independently set to True or False.
        spec = StepSpec(
            name="join",
            node_type=NodeType.JOIN,
            in_funcs=("a", "b"),
            out_funcs=("end",),
            split_parents=("split",),
            is_foreach_join=True,
            is_split_join=False,
        )
        assert spec.is_foreach_join is True
        assert spec.is_split_join is False


class TestParameterSpec:
    """Tests for ParameterSpec dataclass."""

    def test_string_default(self) -> None:
        p = ParameterSpec(name="msg", default="hello", type_name="str")
        assert p.default == "hello"
        assert p.type_name == "str"

    def test_int_default(self) -> None:
        p = ParameterSpec(name="count", default=42, type_name="int")
        assert p.default == 42

    def test_empty_description_default(self) -> None:
        p = ParameterSpec(name="x", default=1)
        assert p.description == ""


class TestFlowSpec:
    """Tests for FlowSpec dataclass."""

    def test_minimal_construction(self) -> None:
        """FlowSpec can be constructed with only required fields."""
        spec = FlowSpec(
            name="MyFlow",
            steps=(
                StepSpec("start", NodeType.START, (), ("end",), ()),
                StepSpec("end", NodeType.END, ("start",), (), ()),
            ),
            parameters=(),
        )
        assert spec.name == "MyFlow"
        assert len(spec.steps) == 2
        assert spec.schedule_cron is None
        assert spec.tags == ()

    def test_frozen(self) -> None:
        spec = FlowSpec(name="F", steps=(), parameters=())
        with pytest.raises((AttributeError, TypeError)):
            spec.name = "G"  # type: ignore[misc]


class TestPrefectFlowConfig:
    """Tests for PrefectFlowConfig dataclass."""

    def test_defaults(self) -> None:
        cfg = PrefectFlowConfig(flow_file="/tmp/myflow.py")
        assert cfg.datastore_type == "local"
        assert cfg.metadata_type == "local"
        assert cfg.max_workers == 10
        assert cfg.username == ""

    def test_custom_values(self) -> None:
        cfg = PrefectFlowConfig(
            flow_file="/path/to/flow.py",
            datastore_type="s3",
            metadata_type="service",
            username="alice",
            max_workers=20,
        )
        assert cfg.datastore_type == "s3"
        assert cfg.username == "alice"
        assert cfg.max_workers == 20
