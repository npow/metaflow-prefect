"""Tests for prefect_deployer_objects and prefect_deployer."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# PrefectTriggeredRun
# ---------------------------------------------------------------------------


def _make_triggered_run(pathspec: str) -> "PrefectTriggeredRun":
    from metaflow_extensions.prefect.plugins.prefect.prefect_deployer_objects import (
        PrefectTriggeredRun,
    )

    deployer = MagicMock()
    deployer.env_vars = {}
    run = object.__new__(PrefectTriggeredRun)
    run.__dict__["deployer"] = deployer
    run.__dict__["pathspec"] = pathspec
    run.__dict__["content"] = "{}"
    return run


class TestPrefectTriggeredRunPrefectUI:
    def test_returns_url_for_valid_pathspec(self) -> None:
        run = _make_triggered_run("MyFlow/prefect-abc123")
        assert run.prefect_ui == "http://localhost:4200/flow-runs/flow-run/abc123"

    def test_returns_none_when_run_id_not_prefect_prefix(self) -> None:
        run = _make_triggered_run("MyFlow/argo-abc123")
        assert run.prefect_ui is None

    def test_returns_none_for_malformed_pathspec(self) -> None:
        run = _make_triggered_run("no-slash-here")
        assert run.prefect_ui is None


class TestPrefectTriggeredRunStatus:
    def test_pending_when_run_is_none(self) -> None:
        run = _make_triggered_run("MyFlow/prefect-abc")
        with patch.object(type(run), "run", new_callable=lambda: property(lambda self: None)):
            assert run.status == "PENDING"

    def test_succeeded_when_run_successful(self) -> None:
        run = _make_triggered_run("MyFlow/prefect-abc")
        mock_run = MagicMock()
        mock_run.successful = True
        mock_run.finished = True
        with patch.object(type(run), "run", new_callable=lambda: property(lambda self: mock_run)):
            assert run.status == "SUCCEEDED"

    def test_failed_when_run_finished_not_successful(self) -> None:
        run = _make_triggered_run("MyFlow/prefect-abc")
        mock_run = MagicMock()
        mock_run.successful = False
        mock_run.finished = True
        with patch.object(type(run), "run", new_callable=lambda: property(lambda self: mock_run)):
            assert run.status == "FAILED"

    def test_running_when_run_not_finished(self) -> None:
        run = _make_triggered_run("MyFlow/prefect-abc")
        mock_run = MagicMock()
        mock_run.successful = False
        mock_run.finished = False
        with patch.object(type(run), "run", new_callable=lambda: property(lambda self: mock_run)):
            assert run.status == "RUNNING"


class TestPrefectTriggeredRunRun:
    def test_returns_none_when_run_not_found(self) -> None:
        from metaflow.exception import MetaflowNotFound

        run = _make_triggered_run("MyFlow/prefect-abc")
        with patch("metaflow.Run", side_effect=MetaflowNotFound("not found")):
            with patch("metaflow.metadata"):
                assert run.run is None

    def test_restores_env_vars_after_call(self) -> None:
        """Environment variables are restored even when Run() raises."""
        import os

        from metaflow.exception import MetaflowNotFound

        run = _make_triggered_run("MyFlow/prefect-abc")
        run.__dict__["deployer"].env_vars = {"METAFLOW_DEFAULT_METADATA": "local"}

        original = os.environ.copy()
        with patch("metaflow.Run", side_effect=MetaflowNotFound("not found")):
            with patch("metaflow.metadata"):
                run.run
        # Env should be restored to its original state
        assert os.environ.get("METAFLOW_DEFAULT_METADATA") == original.get(
            "METAFLOW_DEFAULT_METADATA"
        )


# ---------------------------------------------------------------------------
# PrefectDeployedFlow
# ---------------------------------------------------------------------------


def _make_deployed_flow(name: str = "my-deploy", flow_name: str = "MyFlow") -> "PrefectDeployedFlow":
    from metaflow_extensions.prefect.plugins.prefect.prefect_deployer_objects import (
        PrefectDeployedFlow,
    )

    deployer = MagicMock()
    deployer.flow_file = "/path/to/myflow.py"
    deployed = object.__new__(PrefectDeployedFlow)
    deployed.__dict__["deployer"] = deployer
    deployed.__dict__["name"] = name
    deployed.__dict__["flow_name"] = flow_name
    deployed.__dict__["metadata"] = "{}"
    return deployed


class TestPrefectDeployedFlowId:
    def test_id_is_valid_json(self) -> None:
        flow = _make_deployed_flow()
        parsed = json.loads(flow.id)
        assert parsed["name"] == "my-deploy"
        assert parsed["flow_name"] == "MyFlow"

    def test_id_contains_flow_file(self) -> None:
        flow = _make_deployed_flow()
        parsed = json.loads(flow.id)
        assert parsed["flow_file"] == "/path/to/myflow.py"

    def test_id_roundtrips_through_from_deployment(self) -> None:
        from metaflow_extensions.prefect.plugins.prefect.prefect_deployer_objects import (
            PrefectDeployedFlow,
        )

        original = _make_deployed_flow(name="roundtrip-deploy", flow_name="RoundtripFlow")
        identifier = original.id

        with patch(
            "metaflow_extensions.prefect.plugins.prefect.prefect_deployer.PrefectDeployer"
        ) as MockDeployer:
            mock_deployer_instance = MagicMock()
            MockDeployer.return_value = mock_deployer_instance
            recovered = PrefectDeployedFlow.from_deployment(identifier)

        assert recovered.deployer.name == "roundtrip-deploy"
        assert recovered.deployer.flow_name == "RoundtripFlow"


class TestPrefectDeployedFlowFromDeployment:
    def test_from_deployment_sets_name_and_flow_name(self) -> None:
        from metaflow_extensions.prefect.plugins.prefect.prefect_deployer_objects import (
            PrefectDeployedFlow,
        )

        identifier = json.dumps({
            "name": "test-deploy",
            "flow_name": "TestFlow",
            "flow_file": "/tmp/testflow.py",
        })

        with patch(
            "metaflow_extensions.prefect.plugins.prefect.prefect_deployer.PrefectDeployer"
        ) as MockDeployer:
            mock_deployer_instance = MagicMock()
            MockDeployer.return_value = mock_deployer_instance
            result = PrefectDeployedFlow.from_deployment(identifier)

        assert result.deployer.name == "test-deploy"
        assert result.deployer.flow_name == "TestFlow"

    def test_from_deployment_passes_metadata(self) -> None:
        from metaflow_extensions.prefect.plugins.prefect.prefect_deployer_objects import (
            PrefectDeployedFlow,
        )

        identifier = json.dumps({
            "name": "d",
            "flow_name": "F",
            "flow_file": "/tmp/f.py",
        })
        with patch(
            "metaflow_extensions.prefect.plugins.prefect.prefect_deployer.PrefectDeployer"
        ):
            result = PrefectDeployedFlow.from_deployment(identifier, metadata='{"k": "v"}')
        assert result.deployer.metadata == '{"k": "v"}'

    def test_from_deployment_defaults_metadata_to_empty_dict(self) -> None:
        from metaflow_extensions.prefect.plugins.prefect.prefect_deployer_objects import (
            PrefectDeployedFlow,
        )

        identifier = json.dumps({
            "name": "d",
            "flow_name": "F",
            "flow_file": "/tmp/f.py",
        })
        with patch(
            "metaflow_extensions.prefect.plugins.prefect.prefect_deployer.PrefectDeployer"
        ):
            result = PrefectDeployedFlow.from_deployment(identifier)
        assert result.deployer.metadata == "{}"


# ---------------------------------------------------------------------------
# prefect_cli internals
# ---------------------------------------------------------------------------


class TestExecFlowFile:
    def test_calls_flow_function(self, tmp_path) -> None:
        """_exec_flow_file imports the file and calls the @flow entry point."""
        from metaflow_extensions.prefect.plugins.prefect.prefect_cli import _exec_flow_file

        sentinel = tmp_path / "sentinel.txt"
        flow_file = tmp_path / "myflow.py"
        flow_file.write_text(
            f"def my_flow():\n    open({str(sentinel)!r}, 'w').close()\n"
        )

        _exec_flow_file(str(flow_file), "MyFlow")
        assert sentinel.exists()

    def test_unknown_flow_name_raises(self, tmp_path) -> None:
        """_exec_flow_file raises AttributeError when the flow function doesn't exist."""
        from metaflow_extensions.prefect.plugins.prefect.prefect_cli import _exec_flow_file

        flow_file = tmp_path / "empty.py"
        flow_file.write_text("x = 1\n")

        with pytest.raises(AttributeError):
            _exec_flow_file(str(flow_file), "NonExistentFlow")
