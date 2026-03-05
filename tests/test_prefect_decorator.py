"""Tests for metaflow_extensions.prefect.plugins.prefect.prefect_decorator."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from metaflow_extensions.prefect.plugins.prefect.prefect_decorator import (
    ENV_FLOW_RUN_ID,
    ENV_TASK_RUN_ID,
    PrefectInternalDecorator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_decorator() -> PrefectInternalDecorator:
    return PrefectInternalDecorator()


def _call_pre_step(
    deco: PrefectInternalDecorator,
    metadata: Any,
    step_name: str = "start",
    run_id: str = "prefect-abc123",
    task_id: str = "task001",
    retry_count: int = 0,
) -> None:
    deco.task_pre_step(
        step_name=step_name,
        task_datastore=MagicMock(),
        metadata=metadata,
        run_id=run_id,
        task_id=task_id,
        flow=MagicMock(),
        graph=MagicMock(),
        retry_count=retry_count,
        max_user_code_retries=0,
        ubf_context=None,
        inputs=None,
    )


# ---------------------------------------------------------------------------
# Basic properties
# ---------------------------------------------------------------------------


class TestPrefectInternalDecoratorBasics:
    def test_name(self) -> None:
        assert PrefectInternalDecorator.name == "prefect_internal"

    def test_can_instantiate(self) -> None:
        deco = _make_decorator()
        assert deco is not None

    def test_no_foreach_info_path_exported(self) -> None:
        """The side-car temp-file mechanism has been removed."""
        import metaflow_extensions.prefect.plugins.prefect.prefect_decorator as mod
        assert not hasattr(mod, "ENV_FOREACH_INFO_PATH")


# ---------------------------------------------------------------------------
# task_pre_step — metadata registration
# ---------------------------------------------------------------------------


class TestTaskPreStep:
    def test_registers_flow_run_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENV_FLOW_RUN_ID, "flow-run-999")
        monkeypatch.delenv(ENV_TASK_RUN_ID, raising=False)

        deco = _make_decorator()
        metadata = MagicMock()
        _call_pre_step(deco, metadata)

        metadata.register_metadata.assert_called_once()
        entries = metadata.register_metadata.call_args[0][3]
        fields = {e.field: e.value for e in entries}
        assert fields["prefect-flow-run-id"] == "flow-run-999"

    def test_registers_task_run_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENV_TASK_RUN_ID, "task-run-42")
        monkeypatch.delenv(ENV_FLOW_RUN_ID, raising=False)

        deco = _make_decorator()
        metadata = MagicMock()
        _call_pre_step(deco, metadata)

        entries = metadata.register_metadata.call_args[0][3]
        fields = {e.field: e.value for e in entries}
        assert fields["prefect-task-run-id"] == "task-run-42"

    def test_registers_both_ids(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENV_FLOW_RUN_ID, "flow-run-1")
        monkeypatch.setenv(ENV_TASK_RUN_ID, "task-run-2")

        deco = _make_decorator()
        metadata = MagicMock()
        _call_pre_step(deco, metadata)

        entries = metadata.register_metadata.call_args[0][3]
        fields = {e.field: e.value for e in entries}
        assert fields["prefect-flow-run-id"] == "flow-run-1"
        assert fields["prefect-task-run-id"] == "task-run-2"

    def test_no_env_vars_registers_no_metadata(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(ENV_FLOW_RUN_ID, raising=False)
        monkeypatch.delenv(ENV_TASK_RUN_ID, raising=False)

        deco = _make_decorator()
        metadata = MagicMock()
        _call_pre_step(deco, metadata)

        metadata.register_metadata.assert_called_once()
        entries = metadata.register_metadata.call_args[0][3]
        assert entries == []

    def test_metadata_includes_retry_tag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENV_FLOW_RUN_ID, "flow-run-x")
        monkeypatch.delenv(ENV_TASK_RUN_ID, raising=False)

        deco = _make_decorator()
        metadata = MagicMock()
        _call_pre_step(deco, metadata, retry_count=2)

        entries = metadata.register_metadata.call_args[0][3]
        assert any("attempt_id:2" in e.tags[0] for e in entries)
