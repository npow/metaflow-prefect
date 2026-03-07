"""End-to-end tests: execute Metaflow flows through a real Prefect runtime.

Two test classes:

* ``TestPrefectRun``
    Uses ``python flow.py prefect run``.  Compiles the flow and runs it via
    Prefect locally (ephemeral API — no server required).  Verifies both the
    subprocess exit code and Metaflow artifact values in the local datastore.

* ``TestPrefectDeployment``
    Full lifecycle: ``prefect create`` → trigger via Prefect API → poll until
    completion → verify Metaflow artifacts.  Requires a running Prefect server
    (``PREFECT_API_URL``) and a process work pool (``PREFECT_E2E_WORK_POOL``,
    default ``e2e-pool``).

Run locally (unit-test mode, no server):
    pytest -m e2e -k TestPrefectRun -v

Run in CI with server:
    PREFECT_API_URL=http://127.0.0.1:4200/api \\
    PREFECT_E2E_WORK_POOL=e2e-pool \\
    pytest -m e2e -v
"""
from __future__ import annotations

import asyncio
import datetime
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

FLOWS_DIR = Path(__file__).parent / "flows"
PYTHON = sys.executable
WORK_POOL = os.environ.get("PREFECT_E2E_WORK_POOL", "e2e-pool")


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------


def _env() -> dict[str, str]:
    """Subprocess environment with local Metaflow datastore/metadata."""
    e = dict(os.environ)
    e.setdefault("METAFLOW_DEFAULT_METADATA", "local")
    e.setdefault("METAFLOW_DEFAULT_DATASTORE", "local")
    e.setdefault("METAFLOW_DEFAULT_ENVIRONMENT", "local")
    return e


def _run_flow(
    flow_file: Path, *args: str, timeout: int = 300
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [PYTHON, str(flow_file), "--no-pylint", *args],
        capture_output=True,
        text=True,
        env=_env(),
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Metaflow artifact helpers
# ---------------------------------------------------------------------------


def _mf_run_after(flow_class_name: str, after: datetime.datetime) -> Any:
    """Return the most recent Metaflow run created after *after*, or None."""
    from metaflow import Flow  # type: ignore[import]

    for run in Flow(flow_class_name):
        created = run.created_at
        # Normalise to UTC-aware for comparison.
        if created.tzinfo is None:
            created = created.replace(tzinfo=datetime.timezone.utc)
        if created >= after:
            return run
    return None


def _mf_run_by_id(flow_class_name: str, run_id: str) -> Any:
    """Return the Metaflow Run for the given run_id."""
    from metaflow import Run  # type: ignore[import]

    return Run(f"{flow_class_name}/{run_id}")


# ---------------------------------------------------------------------------
# Prefect deployment helpers
# ---------------------------------------------------------------------------


async def _trigger_and_wait(
    flow_name: str,
    deployment_name: str,
    *,
    parameters: dict[str, Any] | None = None,
    timeout: int = 300,
) -> str:
    """Trigger a named Prefect deployment and wait for a terminal state.

    Returns the Prefect flow-run UUID as a string.  Raises ``AssertionError``
    if the run fails, or ``TimeoutError`` if it does not finish in time.
    """
    from prefect.client.orchestration import get_client  # type: ignore[import]
    from prefect.client.schemas.filters import (  # type: ignore[import]
        DeploymentFilter,
        DeploymentFilterName,
        FlowFilter,
        FlowFilterName,
    )

    async with get_client() as client:
        deployments = await client.read_deployments(
            flow_filter=FlowFilter(name=FlowFilterName(any_=[flow_name])),
            deployment_filter=DeploymentFilter(
                name=DeploymentFilterName(any_=[deployment_name])
            ),
        )
        if not deployments:
            raise AssertionError(
                f"No deployment {deployment_name!r} found for flow {flow_name!r}"
            )

        flow_run = await client.create_flow_run_from_deployment(
            deployments[0].id,
            parameters=parameters or None,
        )
        flow_run_id = str(flow_run.id)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            run = await client.read_flow_run(flow_run.id)
            if run.state.is_final():
                if not run.state.is_completed():
                    raise AssertionError(
                        f"Flow run {flow_run_id} ended in state {run.state.name}"
                    )
                return flow_run_id
            await asyncio.sleep(3)

    raise TimeoutError(
        f"Prefect flow run {flow_run_id!r} did not complete in {timeout}s"
    )


# ---------------------------------------------------------------------------
# TestPrefectRun — 'prefect run' (no server required)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestPrefectRun:
    """'python flow.py prefect run' compiles and runs the flow via Prefect locally."""

    def test_simple_flow(self) -> None:
        t0 = datetime.datetime.now(datetime.timezone.utc)
        r = _run_flow(FLOWS_DIR / "simple_flow.py", "prefect", "run")
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        mf_run = _mf_run_after("SimpleFlow", t0)
        assert mf_run is not None, "No Metaflow run found after test start"
        assert mf_run.successful
        assert mf_run["process"].task.data.result == 84  # value=42, result=42*2

    def test_branch_flow(self) -> None:
        t0 = datetime.datetime.now(datetime.timezone.utc)
        r = _run_flow(FLOWS_DIR / "branch_flow.py", "prefect", "run")
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        mf_run = _mf_run_after("BranchFlow", t0)
        assert mf_run is not None
        assert mf_run.successful
        # branch_a: result_a=11, branch_b: result_b=12, join: result=23
        assert mf_run["join"].task.data.result == 23

    def test_foreach_flow(self) -> None:
        t0 = datetime.datetime.now(datetime.timezone.utc)
        r = _run_flow(FLOWS_DIR / "foreach_flow.py", "prefect", "run")
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        mf_run = _mf_run_after("ForeachFlow", t0)
        assert mf_run is not None
        assert mf_run.successful
        # items=[1,2,3], each *10 → results=[10,20,30]
        assert sorted(mf_run["join_step"].task.data.results) == [10, 20, 30]

    def test_param_flow_defaults(self) -> None:
        t0 = datetime.datetime.now(datetime.timezone.utc)
        r = _run_flow(FLOWS_DIR / "param_flow.py", "prefect", "run")
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        mf_run = _mf_run_after("ParamFlow", t0)
        assert mf_run is not None
        assert mf_run.successful
        # default: message="hello", count=3 → output="hellohellohello"
        assert mf_run["start"].task.data.output == "hellohellohello"


# ---------------------------------------------------------------------------
# TestPrefectDeployment — full lifecycle (requires server + worker)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestPrefectDeployment:
    """Create deployment → trigger run → wait → verify Metaflow artifacts."""

    @pytest.fixture(autouse=True)
    def _require_server(self) -> None:
        if not os.environ.get("PREFECT_API_URL"):
            pytest.skip("PREFECT_API_URL not set — deployment tests require a Prefect server")

    async def test_simple_flow(self) -> None:
        r = _run_flow(
            FLOWS_DIR / "simple_flow.py",
            "prefect", "create", "--name", "e2e-simple", "--work-pool", WORK_POOL,
        )
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        flow_run_id = await _trigger_and_wait("SimpleFlow", "e2e-simple")

        mf_run = _mf_run_by_id("SimpleFlow", f"prefect-{flow_run_id}")
        assert mf_run.successful
        assert mf_run["process"].task.data.result == 84

    async def test_branch_flow(self) -> None:
        r = _run_flow(
            FLOWS_DIR / "branch_flow.py",
            "prefect", "create", "--name", "e2e-branch", "--work-pool", WORK_POOL,
        )
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        flow_run_id = await _trigger_and_wait("BranchFlow", "e2e-branch")

        mf_run = _mf_run_by_id("BranchFlow", f"prefect-{flow_run_id}")
        assert mf_run.successful
        assert mf_run["join"].task.data.result == 23

    async def test_foreach_flow(self) -> None:
        r = _run_flow(
            FLOWS_DIR / "foreach_flow.py",
            "prefect", "create", "--name", "e2e-foreach", "--work-pool", WORK_POOL,
        )
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        flow_run_id = await _trigger_and_wait("ForeachFlow", "e2e-foreach")

        mf_run = _mf_run_by_id("ForeachFlow", f"prefect-{flow_run_id}")
        assert mf_run.successful
        assert sorted(mf_run["join_step"].task.data.results) == [10, 20, 30]

    async def test_param_flow(self) -> None:
        r = _run_flow(
            FLOWS_DIR / "param_flow.py",
            "prefect", "create", "--name", "e2e-params", "--work-pool", WORK_POOL,
        )
        assert r.returncode == 0, f"STDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"

        # Trigger with default parameters (no override).
        flow_run_id = await _trigger_and_wait("ParamFlow", "e2e-params")

        mf_run = _mf_run_by_id("ParamFlow", f"prefect-{flow_run_id}")
        assert mf_run.successful
        # default: message="hello", count=3 → output="hellohellohello"
        assert mf_run["start"].task.data.output == "hellohellohello"
