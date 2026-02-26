# metaflow-prefect

[![CI](https://github.com/npow/metaflow-prefect/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-prefect/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-prefect)](https://pypi.org/project/metaflow-prefect/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Deploy and run Metaflow flows as Prefect deployments.

`metaflow-prefect` generates a self-contained Prefect flow file from any Metaflow flow, letting
you schedule, deploy, and monitor your pipelines through Prefect while keeping all your existing
Metaflow code unchanged.

## Install

```bash
pip install metaflow-prefect
```

Or from source:

```bash
git clone https://github.com/npow/metaflow-prefect.git
cd metaflow-prefect
pip install -e ".[dev]"
```

## Quick start

```bash
python my_flow.py prefect create my_flow_prefect.py
python my_flow_prefect.py
```

## Usage

### Generate and run a Prefect flow

```bash
# Write the generated file and run it directly
python my_flow.py prefect create my_flow_prefect.py
python my_flow_prefect.py

# Or compile and run in one step
python my_flow.py prefect run

# Register a named deployment on a Prefect server
python my_flow.py prefect deploy --name prod --work-pool my-pool
```

### All graph shapes are supported

```python
# Linear
class SimpleFlow(FlowSpec):
    @step
    def start(self):
        self.value = 42
        self.next(self.end)
    @step
    def end(self): pass

# Split/join (branch)
class BranchFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.branch_a, self.branch_b)
    ...

# Foreach fan-out (body tasks run concurrently)
class ForeachFlow(FlowSpec):
    @step
    def start(self):
        self.items = [1, 2, 3]
        self.next(self.process, foreach="items")
    ...
```

### Parametrised flows

Parameters defined with `metaflow.Parameter` are forwarded automatically:

```bash
python param_flow.py prefect create param_flow_prefect.py
python param_flow_prefect.py --message "hello" --count 5
```

### Step decorator support

`@retry`, `@timeout`, and `@environment` decorators are read from your flow and applied
to the generated Prefect tasks automatically — no changes to your flow code required.

```python
class MyFlow(FlowSpec):
    @retry(times=3, minutes_between_retries=2)
    @timeout(seconds=600)
    @environment(vars={"API_KEY": "secret"})
    @step
    def train(self):
        ...
```

The generated Prefect task becomes:

```python
@task(name="train", retries=3, timeout_seconds=600, retry_delay_seconds=120)
def _step_train(run_id, prev_task_id):
    _extra_env.update({"API_KEY": "secret"})
    ...
```

## Configuration

### Metadata service and datastore

By default, `metaflow-prefect` uses whatever metadata and datastore backends are active in your
Metaflow environment. The generated Prefect file bakes in `METADATA_TYPE` and `DATASTORE_TYPE`
at creation time so every step subprocess uses the same backend.

To use a remote metadata service or object store, configure them before running `prefect create`:

```bash
# Remote metadata service + S3 datastore
python my_flow.py \
  --metadata=service \
  --datastore=s3 \
  prefect create my_flow_prefect.py
```

Or via environment variables (applied to all flows):

```bash
export METAFLOW_DEFAULT_METADATA=service
export METAFLOW_DEFAULT_DATASTORE=s3
python my_flow.py prefect create my_flow_prefect.py
```

### Flow-level timeout

```bash
python my_flow.py prefect create my_flow_prefect.py --workflow-timeout=3600
```

### Step decorators (`--with`)

Inject Metaflow step decorators at deploy time without modifying the flow source:

```bash
# Run each step inside a sandbox (e.g. metaflow-sandbox extension)
python my_flow.py prefect create my_flow_prefect.py --with=sandbox

# Multiple decorators are supported
python my_flow.py prefect deploy --name prod \
  --with=sandbox \
  --with="resources:cpu=4,memory=8000"
```

### `@project` support

Flows decorated with `@project` use a project-qualified name for the deployment:

```python
@project(name="my-team")
class MyFlow(FlowSpec):
    ...
```

```bash
# Deployment will be registered as "my-team.MyFlow"
python my_flow.py prefect deploy --name prod
```

## How it works

`metaflow-prefect` generates a self-contained Prefect flow file from your Metaflow flow's DAG.
Each Metaflow step becomes a `@task`. The generated file:

- runs each step as a subprocess via the standard `metaflow step` CLI
- passes `--input-paths` correctly for joins and foreach splits
- runs foreach body tasks concurrently via Prefect's task runner
- maps `@retry`, `@timeout`, and `@environment` decorators to Prefect task settings
- writes Metaflow artifacts to the Prefect UI as markdown artifacts with a ready-to-use retrieval snippet

### Prefect UI: flow run timeline

The generated flow preserves the Metaflow DAG structure — foreach fan-outs appear as parallel task
runs in the Prefect timeline:

![Flow run timeline showing foreach fan-out](docs/screenshots/flow-run.png)

### Prefect UI: artifact retrieval snippets

After each step completes, a Prefect artifact is posted showing the Metaflow `self.*` artifact
names and a one-liner to fetch each value:

![Artifact tab showing retrieval snippet](docs/screenshots/artifacts.png)

## Supported decorators

| Decorator | Behaviour |
|---|---|
| `@retry(times=N, minutes_between_retries=M)` | Maps to `@task(retries=N, retry_delay_seconds=M*60)` |
| `@timeout(seconds=N)` / `@timeout(minutes=N)` | Maps to `@task(timeout_seconds=N)` |
| `@environment(vars={...})` | Merges vars into the step subprocess environment |
| `@schedule(cron=...)` | Used as the deployment cron schedule |
| `@project(name=...)` | Prefixes the deployment name with the project name |

Unsupported decorators (`@batch`, `@slurm`, `@trigger`, `@trigger_on_finish`, `@exit_hook`,
`@parallel`) raise a clear error at compile time.

## Development

```bash
git clone https://github.com/npow/metaflow-prefect.git
cd metaflow-prefect
pip install -e ".[dev]"
pytest -v
```

## License

[Apache 2.0](LICENSE)
