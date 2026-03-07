"""Metaflow CLI extension: ``python myflow.py prefect <command>``.

Commands
--------
compile  Compile the flow to a Prefect flow Python file.
run      Compile and immediately run the flow via Prefect (local execution).
resume   Re-run a failed flow, reusing outputs from steps that already succeeded.
create   Register the flow as a named Prefect deployment on the active server.
trigger  Trigger a run for a previously deployed Prefect deployment.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import tempfile

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.package import MetaflowPackage
from metaflow.util import get_username

from metaflow_extensions.prefect.plugins.prefect._codegen import _python_name
from metaflow_extensions.prefect.plugins.prefect._graph import analyze_graph
from metaflow_extensions.prefect.plugins.prefect._types import FlowSpec
from metaflow_extensions.prefect.plugins.prefect.exception import PrefectException
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
# prefect compile
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
@click.option(
    "--with",
    "with_decorators",
    multiple=True,
    default=None,
    help="Inject a decorator on every step (repeatable), e.g. --with=sandbox.",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Flow-level timeout in seconds.",
)
@click.pass_obj
def compile(
    obj: object,
    output_file: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
) -> None:
    if os.path.abspath(sys.argv[0]) == os.path.abspath(output_file):
        raise MetaflowException(
            "Output file name cannot be the same as the flow file name."
        )

    _make_flow_and_write(obj, output_file, tags, user_namespace, max_workers, with_decorators, workflow_timeout)

    # type: ignore — obj is the Metaflow CLI context object
    obj.echo(  # type: ignore[attr-defined]
        "Prefect flow file written to *{out}*.\n"
        "Run it with:  python {out}\n"
        "Or deploy it: python {flow} prefect create --name my-deployment".format(
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
@click.option("--with", "with_decorators", multiple=True, default=None,
              help="Inject a decorator on every step (repeatable).")
@click.option("--workflow-timeout", default=None, type=int,
              help="Flow-level timeout in seconds.")
@click.pass_obj
def run(
    obj: object,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
) -> None:
    _compile_and_run_locally(obj, tags, user_namespace, max_workers, with_decorators, workflow_timeout)


# ---------------------------------------------------------------------------
# prefect resume
# ---------------------------------------------------------------------------


@prefect.command(help="Resume a failed Metaflow run via Prefect (locally).")
@click.option(
    "--clone-run-id",
    required=True,
    help="Metaflow run ID to resume from (e.g. prefect-<uuid>).",
)
@click.option("--tag", "tags", multiple=True, default=None,
              help="Tag for the Metaflow run (repeatable).")
@click.option("--namespace", "user_namespace", default=None)
@click.option("--max-workers", default=10, show_default=True)
@click.option("--with", "with_decorators", multiple=True, default=None,
              help="Inject a decorator on every step (repeatable).")
@click.option("--workflow-timeout", default=None, type=int,
              help="Flow-level timeout in seconds.")
@click.pass_obj
def resume(
    obj: object,
    clone_run_id: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
) -> None:
    _compile_and_run_locally(
        obj, tags, user_namespace, max_workers, with_decorators, workflow_timeout,
        origin_run_id=clone_run_id,
    )


# ---------------------------------------------------------------------------
# prefect create
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
@click.option("--with", "with_decorators", multiple=True, default=None,
              help="Inject a decorator on every step (repeatable).")
@click.option("--workflow-timeout", default=None, type=int,
              help="Flow-level timeout in seconds.")
@click.option("--deployer-attribute-file", default=None, hidden=True,
              help="Write deployment info JSON here (used by Metaflow Deployer API).")
@click.pass_obj
def create(
    obj: object,
    name: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
    work_pool: str | None,
    paused: bool,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    deployer_attribute_file: str | None,
) -> None:
    # Write to a permanent file named after the flow so the Prefect worker
    # can reload it later.  The temp-file approach breaks because to_deployment()
    # records the file path and the worker needs it at execution time.
    flow_file_name = "%s_prefect.py" % obj.flow.name.lower()  # type: ignore[attr-defined]
    # Write next to the original flow file so the path is stable and the
    # Prefect worker can find it regardless of CWD (os.getcwd() may be a temp dir).
    flow_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    flow_file_path = os.path.join(flow_dir, flow_file_name)

    mf_spec = analyze_graph(obj.graph, obj.flow)  # type: ignore[attr-defined]

    _make_flow_and_write(obj, flow_file_path, tags, user_namespace, max_workers, with_decorators, workflow_timeout)

    mod_spec = importlib.util.spec_from_file_location("_mf_prefect_flow", flow_file_path)
    mod = importlib.util.module_from_spec(mod_spec)  # type: ignore[arg-type]
    sys.modules[mod_spec.name] = mod  # Make module importable for Prefect deployment introspection.
    mod_spec.loader.exec_module(mod)  # type: ignore[union-attr]
    prefect_flow_fn = getattr(mod, _python_name(obj.flow.name))  # type: ignore[attr-defined]

    asyncio.run(
        _register_deployment(
            prefect_flow_fn,
            name=name,
            cron=mf_spec.schedule_cron,
            work_pool=work_pool,
            paused=paused,
            tags=list(tags),
            obj=obj,
            flow_spec=mf_spec,
        )
    )

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": name,
                    "flow_name": obj.flow.name,  # type: ignore[attr-defined]
                    "metadata": "{}",
                },
                f,
            )


# ---------------------------------------------------------------------------
# prefect trigger
# ---------------------------------------------------------------------------


@prefect.command(help="Trigger a run for a previously deployed Prefect deployment.")
@click.option("--name", required=True, help="Prefect deployment name.")
@click.option(
    "--deployer-attribute-file", default=None, hidden=True,
    help="Write triggered-run info JSON here (used by Metaflow Deployer API).",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.pass_obj
def trigger(
    obj: object,
    name: str,
    deployer_attribute_file: str | None,
    run_params: tuple[str, ...],
) -> None:
    params: dict[str, str] = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        params[k.strip()] = v.strip()

    flow_name = obj.flow.name  # type: ignore[attr-defined]
    asyncio.run(
        _trigger_deployment(
            flow_name=flow_name,
            deployment_name=name,
            params=params,
            deployer_attribute_file=deployer_attribute_file,
            obj=obj,
        )
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compile_and_run_locally(
    obj: object,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    origin_run_id: str | None = None,
) -> None:
    """Compile the flow to a temp file, execute it in-process, then delete it."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w", dir=os.getcwd()) as tmp:
        tmp_path = tmp.name
    try:
        _make_flow_and_write(
            obj, tmp_path, tags, user_namespace, max_workers,
            with_decorators, workflow_timeout, origin_run_id=origin_run_id,
        )
        _exec_flow_file(tmp_path, obj.flow.name)  # type: ignore[attr-defined]
    finally:
        os.unlink(tmp_path)


def _exec_flow_file(path: str, flow_name: str) -> None:
    """Load a generated Prefect flow file and call its @flow entry point."""
    spec = importlib.util.spec_from_file_location("_mf_prefect_flow", path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    # Register in sys.modules so Prefect's deployment introspection can find it.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    getattr(mod, _python_name(flow_name))()


def _make_flow_and_write(
    obj: object,
    output_file: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    max_workers: int,
    with_decorators: tuple[str, ...] = (),
    workflow_timeout: int | None = None,
    origin_run_id: str | None = None,
) -> None:
    package = MetaflowPackage(
        obj.flow,                # type: ignore[attr-defined]
        obj.environment,         # type: ignore[attr-defined]
        obj.echo,                # type: ignore[attr-defined]
        flow_datastore=obj.flow_datastore,  # type: ignore[attr-defined]
    )
    code_package_url, code_package_sha = obj.flow_datastore.save_data(  # type: ignore[attr-defined]
        [package.blob], len_hint=1
    )[0]

    pf = PrefectFlow(
        graph=obj.graph,          # type: ignore[attr-defined]
        flow=obj.flow,            # type: ignore[attr-defined]
        code_package_metadata=package.package_metadata,
        code_package_sha=code_package_sha,
        code_package_url=code_package_url,
        metadata=obj.metadata,                # type: ignore[attr-defined]
        flow_datastore=obj.flow_datastore,    # type: ignore[attr-defined]
        environment=obj.environment,          # type: ignore[attr-defined]
        event_logger=obj.event_logger,        # type: ignore[attr-defined]
        monitor=obj.monitor,                  # type: ignore[attr-defined]
        tags=list(tags),
        namespace=user_namespace,
        username=get_username(),
        max_workers=max_workers,
        flow_file=os.path.abspath(sys.argv[0]),
        with_decorators=list(with_decorators),
        workflow_timeout=workflow_timeout,
        origin_run_id=origin_run_id,
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
    flow_spec: FlowSpec,
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
    if asyncio.iscoroutine(deployment):
        deployment = await deployment

    # Make the entrypoint absolute so the worker can find the flow file
    # regardless of its CWD (Prefect stores relative paths by default).
    if hasattr(deployment, "entrypoint") and deployment.entrypoint:
        parts = deployment.entrypoint.rsplit(":", 1)
        if len(parts) == 2:
            filepath, funcname = parts
            deployment.entrypoint = "%s:%s" % (os.path.abspath(filepath), funcname)

    apply_result = deployment.apply()
    deployment_id = await apply_result if asyncio.iscoroutine(apply_result) else apply_result

    # Always sync automations so stale ones are cleaned up even when all
    # @trigger/@trigger_on_finish decorators have been removed from the flow.
    async with get_client() as client:
        await _sync_automations(client, deployment_id, name, flow_spec, obj)

    obj.echo(  # type: ignore[attr-defined]
        "Deployment *{name}* registered with id *{id}*.".format(
            name=name, id=deployment_id
        ),
        bold=True,
    )


async def _sync_automations(
    client: object,
    deployment_id: object,
    deployment_name: str,
    spec: FlowSpec,
    obj: object,
) -> None:
    """Upsert Prefect automations for @trigger and @trigger_on_finish decorators.

    Each automation is named deterministically so that re-running ``prefect create``
    updates existing automations rather than creating duplicates.
    """
    from prefect.events.actions import RunDeployment
    from prefect.events.schemas.automations import AutomationCore, EventTrigger, Posture

    action = RunDeployment(source="selected", deployment_id=deployment_id)

    desired: list[tuple[str, AutomationCore]] = []

    for trigger in spec.triggers:
        auto_name = "metaflow/%s: on event '%s'" % (deployment_name, trigger.event_name)
        event_trigger = EventTrigger(
            expect={trigger.event_name},
            posture=Posture.Reactive,
            threshold=1,
            within=0,
        )
        desired.append((auto_name, AutomationCore(
            name=auto_name,
            trigger=event_trigger,
            actions=[action],
            enabled=True,
        )))

    for tof in spec.trigger_on_finishes:
        auto_name = "metaflow/%s: on finish of '%s'" % (deployment_name, tof.flow_name)
        event_trigger = EventTrigger(
            match_related={
                "prefect.resource.role": "flow",
                "prefect.resource.name": tof.flow_name,
            },
            expect={"prefect.flow-run.Completed"},
            posture=Posture.Reactive,
            threshold=1,
            within=0,
        )
        desired.append((auto_name, AutomationCore(
            name=auto_name,
            trigger=event_trigger,
            actions=[action],
            enabled=True,
        )))

    desired_names = {auto_name for auto_name, _ in desired}

    for auto_name, automation in desired:
        existing = await client.read_automations_by_name(auto_name)  # type: ignore[attr-defined]
        if existing:
            await client.update_automation(existing[0].id, automation)  # type: ignore[attr-defined]
            obj.echo(  # type: ignore[attr-defined]
                "Automation *{name}* updated.".format(name=auto_name), bold=False
            )
        else:
            await client.create_automation(automation)  # type: ignore[attr-defined]
            obj.echo(  # type: ignore[attr-defined]
                "Automation *{name}* created.".format(name=auto_name), bold=False
            )

    # Clean up stale automations from previous deploys of this deployment
    # (e.g. @trigger was removed from the flow).
    owned_prefix = "metaflow/%s:" % deployment_name
    try:
        all_automations = await client.read_automations()  # type: ignore[attr-defined]
        for auto in all_automations:
            if auto.name.startswith(owned_prefix) and auto.name not in desired_names:
                await client.delete_automation(auto.id)  # type: ignore[attr-defined]
                obj.echo(  # type: ignore[attr-defined]
                    "Automation *{name}* deleted (no longer referenced by flow).".format(
                        name=auto.name
                    ),
                    bold=False,
                )
    except Exception as exc:
        obj.echo(  # type: ignore[attr-defined]
            "Warning: could not check for stale automations: %s" % exc, bold=False
        )


async def _trigger_deployment(
    flow_name: str,
    deployment_name: str,
    params: dict[str, str],
    deployer_attribute_file: str | None,
    obj: object,
) -> None:
    """Trigger a Prefect deployment run and optionally write run info to a file."""
    try:
        from prefect.client.orchestration import get_client
        from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName, FlowFilter, FlowFilterName
    except ImportError:
        raise PrefectException(
            "prefect is required for triggering deployments. "
            "Install it with: pip install metaflow-prefect"
        ) from None

    async with get_client() as client:
        deployments = await client.read_deployments(
            flow_filter=FlowFilter(name=FlowFilterName(any_=[flow_name])),
            deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[deployment_name])),
        )
        if not deployments:
            raise PrefectException(
                "No deployment named %r found for flow %r." % (deployment_name, flow_name)
            )
        deployment = deployments[0]
        flow_run = await client.create_flow_run_from_deployment(
            deployment.id,
            parameters=params or None,
        )

    run_id = "prefect-%s" % flow_run.id
    pathspec = "%s/%s" % (flow_name, run_id)

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "name": deployment_name,
                    "metadata": "{}",
                },
                f,
            )

    obj.echo(  # type: ignore[attr-defined]
        "Triggered Prefect flow run *{run_id}* (pathspec: *{pathspec}*).".format(
            run_id=flow_run.id, pathspec=pathspec
        ),
        bold=True,
    )



