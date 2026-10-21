<p align="center">
  <a href="https://www.paradime.io">
        <img alt="Paradime" src="https://app.paradime.io/logo192.png" width="60" />
    </a>
</p>

<h1 align="center">
  airflow-provider-paradime-dbt
</h1>


This is the provider for Paradime to run and manage dbt™ jobs in production. The provider enables interaction with Paradime’s Bolt scheduler and management APIs.

## Usage

### Create a connection

#### Account API key (recommended)

An [account API key](https://docs.paradime.io/developers/api-keys) (`prdm_cmp_...`) can access one or more workspaces. One Airflow connection is enough to trigger schedules in all of them.

1. In Paradime, create an account API key and select the workspaces it can access. Copy the key, the API endpoint, and the workspace token (UID) listed next to each workspace.
2. In Airflow, create a connection of type **Paradime** with these fields:

| Field | Value |
| --- | --- |
| API Endpoint | The API endpoint shown with the key |
| Account API Key | Your `prdm_cmp_...` key |
| Default Workspace Token | Optional. The workspace to use when a task does not set `workspace_token` |

Leave **API Key (legacy)** and **API Secret (legacy)** blank.

You can also create the connection from an environment variable:

```bash
export AIRFLOW_CONN_PARADIME_DEFAULT='{
    "conn_type": "paradime",
    "extra": {
        "api_endpoint": "https://api.paradime.io/api/v1/<company_token>/graphql",
        "api_token": "prdm_cmp_...",
        "workspace_token": "<default_workspace_token>"
    }
}'
```

Every request with an account API key needs a workspace token. Set it on the task with `workspace_token=`, or set a default on the connection. The task value wins. If neither is set, the task fails with an error that says so.

#### Workspace API key (legacy)

Workspace API keys still work with no changes. A workspace key and secret are bound to one workspace, so you need one connection per workspace.

1. Generate your API key, secret and endpoint from Paradime Workspace settings.
2. Create a connection in Airflow and fill in **API Endpoint**, **API Key (legacy)** and **API Secret (legacy)**. Leave **Account API Key** blank.

`workspace_token` has no effect on a connection that uses a workspace API key.

### Create a DAG

This DAG uses one account API key connection to trigger schedules in two workspaces, wait for them, and download their `manifest.json`:

```py
from airflow.decorators import dag

from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunArtifactOperator, ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor

PARADIME_CONN_ID = "your_paradime_conn_id"  # Update this to your connection id

# Map each Bolt schedule to the workspace that owns it
SCHEDULES = {
    "marketing": {"workspace_token": "your_marketing_workspace_token", "slug": "your-marketing-schedule-slug"},
    "finance": {"workspace_token": "your_finance_workspace_token", "slug": "your-finance-schedule-slug"},
}


@dag(
    default_args={"conn_id": PARADIME_CONN_ID},
)
def run_schedules_in_two_workspaces():
    for name, schedule in SCHEDULES.items():
        workspace_token = schedule["workspace_token"]

        # Run the schedule in its workspace and return the run id as the xcom return value
        task_run_schedule = ParadimeBoltDbtScheduleRunOperator(task_id=f"run_{name}", slug=schedule["slug"], workspace_token=workspace_token)

        # Get the run id from the xcom return value
        run_id = f"{{{{ task_instance.xcom_pull(task_ids='run_{name}') }}}}"

        # Wait for the run to complete. Use the same workspace token as the task that triggered it.
        task_wait_for_schedule = ParadimeBoltDbtScheduleRunSensor(task_id=f"wait_for_{name}", run_id=run_id, workspace_token=workspace_token)

        # Download the manifest.json file from the run and return the path as the xcom return value
        task_download_manifest = ParadimeBoltDbtScheduleRunArtifactOperator(
            task_id=f"download_{name}_manifest",
            run_id=run_id,
            artifact_path="target/manifest.json",
            workspace_token=workspace_token,
        )

        task_run_schedule >> task_wait_for_schedule >> task_download_manifest


run_schedules_in_two_workspaces()
```

`workspace_token` is a templated field on `ParadimeBoltDbtScheduleRunOperator`, `ParadimeBoltDbtScheduleRunSensor` and `ParadimeBoltDbtScheduleRunArtifactOperator`. `ParadimeHook` also takes a `workspace_token` argument. The sensor and the artifact operator must use the same workspace as the task that triggered the run.

With a workspace API key, or an account API key with a default workspace token on the connection, leave out `workspace_token`. See [`bolt_dbt_schedule_run.py`](https://github.com/paradime-io/airflow-provider-paradime-dbt/blob/main/paradime_dbt_provider/example_dags/bolt_dbt_schedule_run.py).

Refer to the [example DAGs](https://github.com/paradime-io/airflow-provider-paradime-dbt/tree/main/paradime_dbt_provider/example_dags) in this repository for more examples.

## Advanced Configuration

### HTTP / HTTPS Proxy

If you need to use a proxy to connect to Paradime's API, you can configure it in the connection on the Airflow UI. You can set the following parameters in the connection:

- **Proxy**: The URL for your proxy server (e.g., `http://proxy.example.com:8080`).

This setting is optional. If not provided, connections will be made directly without a proxy. 