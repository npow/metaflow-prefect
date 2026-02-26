"""Metaflow CLI extension: ``python myflow.py prefect <command>``.

Commands
--------
create  Compile the flow to a Prefect flow Python file.
run     Compile and immediately run the flow via Prefect (local execution).
deploy  Register the flow as a named Prefect deployment on the active server.
"""

from __future__ import annotations

import asyncio
import os
import sys

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.package import MetaflowPackage
from metaflow.util import get_username

from metaflow_extensions.prefect.plugins.prefect.exception import (
    NotSupportedException,
    PrefectException,
)
from metaflow_extensions.prefect.plugins.prefect.prefect_flow import PrefectFlow


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    pass


@cli.group(help="Commands for deploying Metaflow flows to Prefect.")
@click.pass_obj
def prefect(obj: object) -> None:  # type: ignore[override]
    pass


# ---------------------------------------------------------------------------
# prefect create
# ---------------------------------------------------------------------------


@prefect.command(help="Compile this flow to a Prefect flow Python file.")
@click.argument("output_file", required=True)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate Metaflow run objects with this tag (repeatable).",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    help="Override the Metaflow namespace for the run.",
)
@click.option(
    "--max-workers",
    default=10,
    show_default=True,
    help="Maximum number of concurrent Prefect tasks.",
)
@click.pass_obj
def create(
    obj: object,
    output_file: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
) -> None:
    if os.path.abspath(sys.argv[0]) == os.path.abspath(output_file):
        raise MetaflowException(
            "Output file name cannot be the same as the flow file name."
        )

    _make_flow_and_write(obj, output_file, tags, user_namespace, max_workers)

    # type: ignore — obj is the Metaflow CLI context object
    obj.echo(  # type: ignore[attr-defined]
        "Prefect flow file written to *{out}*.\n"
        "Run it with:  python {out}\n"
        "Or deploy it: python {flow} prefect deploy --name my-deployment".format(
            out=output_file,
            flow=sys.argv[0],
        ),
        bold=True,
    )


# ---------------------------------------------------------------------------
# prefect run
# ---------------------------------------------------------------------------


@prefect.command(help="Compile and immediately run the flow via Prefect (locally).")
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Tag for the Metaflow run (repeatable).",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
)
@click.option("--max-workers", default=10, show_default=True)
@click.pass_obj
def run(
    obj: object,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
) -> None:
    import importlib.util
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
        tmp_path = tmp.name

    try:
        _make_flow_and_write(obj, tmp_path, tags, user_namespace, max_workers)
        spec = importlib.util.spec_from_file_location("_mf_prefect_flow", tmp_path)
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        # Call the flow's main entry-point (the @flow decorated function).
        flow_fn_name = _python_name(obj.flow.name)  # type: ignore[attr-defined]
        getattr(mod, flow_fn_name)()
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# prefect deploy
# ---------------------------------------------------------------------------


@prefect.command(help="Register this flow as a named Prefect deployment.")
@click.option("--name", required=True, help="Prefect deployment name.")
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
)
@click.option("--namespace", "user_namespace", default=None)
@click.option("--max-workers", default=10, show_default=True)
@click.option(
    "--work-pool",
    default=None,
    help="Prefect work pool name (required for server-side deployments).",
)
@click.option(
    "--paused",
    is_flag=True,
    default=False,
    help="Create the deployment in a paused state.",
)
@click.pass_obj
def deploy(
    obj: object,
    name: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
    work_pool: str | None,
    paused: bool,
) -> None:
    import importlib.util
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
        tmp_path = tmp.name

    try:
        _make_flow_and_write(obj, tmp_path, tags, user_namespace, max_workers)
        spec = importlib.util.spec_from_file_location("_mf_prefect_flow", tmp_path)
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        flow_fn_name = _python_name(obj.flow.name)  # type: ignore[attr-defined]
        prefect_flow_fn = getattr(mod, flow_fn_name)

        schedule_cron = _get_schedule_cron(obj.flow)  # type: ignore[attr-defined]

        asyncio.run(
            _register_deployment(
                prefect_flow_fn,
                name=name,
                cron=schedule_cron,
                work_pool=work_pool,
                paused=paused,
                tags=list(tags),
                obj=obj,
            )
        )
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_flow_and_write(
    obj: object,
    output_file: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
) -> None:
    pf = PrefectFlow(
        name=_resolve_name(obj),  # type: ignore[arg-type]
        graph=obj.graph,          # type: ignore[attr-defined]
        flow=obj.flow,            # type: ignore[attr-defined]
        code_package_metadata="",
        code_package_sha="",
        code_package_url="",
        metadata=obj.metadata,                      # type: ignore[attr-defined]
        flow_datastore=obj.flow_datastore,          # type: ignore[attr-defined]
        environment=obj.environment,                # type: ignore[attr-defined]
        event_logger=obj.event_logger,              # type: ignore[attr-defined]
        monitor=obj.monitor,                        # type: ignore[attr-defined]
        tags=list(tags),
        namespace=user_namespace,
        username=get_username(),
        max_workers=max_workers,
        description=obj.flow.__doc__,               # type: ignore[attr-defined]
        flow_file=os.path.abspath(sys.argv[0]),
    )
    source = pf.compile()
    with open(output_file, "w") as f:
        f.write(source)


async def _register_deployment(
    prefect_flow_fn: object,
    name: str,
    cron: str | None,
    work_pool: str | None,
    paused: bool,
    tags: list[str],
    obj: object,
) -> None:
    try:
        from prefect.client.orchestration import get_client
    except ImportError:
        raise PrefectException(
            "prefect is required for deployment. "
            "Install it with: pip install metaflow-prefect"
        ) from None

    deployment = prefect_flow_fn.to_deployment(  # type: ignore[attr-defined]
        name=name,
        cron=cron,
        paused=paused,
        tags=tags,
        work_pool_name=work_pool,
    )
    deployment_id = await deployment.apply()
    obj.echo(  # type: ignore[attr-defined]
        "Deployment *{name}* registered with id *{id}*.".format(
            name=name, id=deployment_id
        ),
        bold=True,
    )


def _resolve_name(obj: object) -> str:
    import re

    name = obj.flow.name  # type: ignore[attr-defined]
    # Keep only alphanumerics, hyphens, underscores, dots
    if re.search(r"[^a-zA-Z0-9_\-\.]", name):
        raise MetaflowException(
            "Flow name '%s' contains characters not allowed in a Prefect flow name." % name
        )
    return name


def _get_schedule_cron(flow: object) -> str | None:
    schedules = flow._flow_decorators.get("schedule")  # type: ignore[attr-defined]
    if not schedules:
        return None
    s = schedules[0]
    if s.attributes.get("cron"):
        return s.attributes["cron"]
    if s.attributes.get("weekly"):
        return "0 0 * * 0"
    if s.attributes.get("hourly"):
        return "0 * * * *"
    if s.attributes.get("daily"):
        return "0 0 * * *"
    return None


def _python_name(flow_name: str) -> str:
    result: list[str] = []
    for i, ch in enumerate(flow_name):
        if ch.isupper() and i > 0:
            result.append("_")
        result.append(ch.lower())
    return "".join(result)
