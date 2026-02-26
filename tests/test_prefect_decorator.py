"""Tests for metaflow_extensions.prefect.plugins.prefect.prefect_decorator."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from metaflow_extensions.prefect.plugins.prefect.prefect_decorator import (
    ENV_FLOW_RUN_ID,
    ENV_FOREACH_INFO_PATH,
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


def _call_task_finished(
    deco: PrefectInternalDecorator,
    flow: Any,
    graph: Any,
    step_name: str = "start",
    is_task_ok: bool = True,
) -> None:
    deco.task_finished(
        step_name=step_name,
        flow=flow,
        graph=graph,
        is_task_ok=is_task_ok,
        retry_count=0,
        max_user_code_retries=0,
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


# ---------------------------------------------------------------------------
# task_finished — foreach info file
# ---------------------------------------------------------------------------


class TestTaskFinished:
    def test_writes_foreach_info_when_foreach_step(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        info_path = str(tmp_path / "foreach_info.json")
        monkeypatch.setenv(ENV_FOREACH_INFO_PATH, info_path)

        deco = _make_decorator()
        flow = MagicMock()
        flow._foreach_num_splits = 5
        graph = MagicMock()
        graph.__getitem__ = MagicMock(return_value=MagicMock(type="foreach"))

        _call_task_finished(deco, flow, graph, step_name="start")

        assert os.path.exists(info_path)
        with open(info_path) as f:
            data = json.load(f)
        assert data["num_splits"] == 5

    def test_does_not_write_when_not_foreach(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        info_path = str(tmp_path / "foreach_info.json")
        monkeypatch.setenv(ENV_FOREACH_INFO_PATH, info_path)

        deco = _make_decorator()
        flow = MagicMock()
        flow._foreach_num_splits = 3
        graph = MagicMock()
        graph.__getitem__ = MagicMock(return_value=MagicMock(type="linear"))

        _call_task_finished(deco, flow, graph, step_name="process")

        assert not os.path.exists(info_path)

    def test_does_not_write_when_task_failed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        info_path = str(tmp_path / "foreach_info.json")
        monkeypatch.setenv(ENV_FOREACH_INFO_PATH, info_path)

        deco = _make_decorator()
        flow = MagicMock()
        flow._foreach_num_splits = 4
        graph = MagicMock()
        graph.__getitem__ = MagicMock(return_value=MagicMock(type="foreach"))

        _call_task_finished(deco, flow, graph, step_name="start", is_task_ok=False)

        assert not os.path.exists(info_path)

    def test_does_not_write_when_no_env_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(ENV_FOREACH_INFO_PATH, raising=False)

        deco = _make_decorator()
        flow = MagicMock()
        flow._foreach_num_splits = 2
        graph = MagicMock()
        graph.__getitem__ = MagicMock(return_value=MagicMock(type="foreach"))

        # Should not raise even if path is not set
        _call_task_finished(deco, flow, graph, step_name="start")

    def test_does_not_write_when_num_splits_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        info_path = str(tmp_path / "foreach_info.json")
        monkeypatch.setenv(ENV_FOREACH_INFO_PATH, info_path)

        deco = _make_decorator()
        flow = MagicMock(spec=[])  # No _foreach_num_splits attribute
        graph = MagicMock()
        graph.__getitem__ = MagicMock(return_value=MagicMock(type="foreach"))

        _call_task_finished(deco, flow, graph, step_name="start")

        assert not os.path.exists(info_path)
