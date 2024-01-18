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
1. Generate your API key, secret and endpoint from Paradime Workspace settings.
2. Create a connection in Airflow, as shown below.
![Create a connection](https://github.com/paradime-io/airflow-provider-paradime-dbt/assets/16359086/5f85f981-6555-4c4b-bec0-bf9b1ad79044)

### Create a DAG

Here is one example:
```py
from airflow.decorators import dag

from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunArtifactOperator, ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor

PARADIME_CONN_ID = "your_paradime_conn_id"  # Update this to your connection id
BOLT_SCHEDULE_NAME = "your_schedule_name"  # Update this to your schedule name


@dag(
    default_args={"conn_id": PARADIME_CONN_ID},
)
def run_schedule_and_download_manifest():
    # Run the schedule and return the run id as the xcom return value
    task_run_schedule = ParadimeBoltDbtScheduleRunOperator(task_id="run_schedule", schedule_name=BOLT_SCHEDULE_NAME)

    # Get the run id from the xcom return value
    run_id = "{{ task_instance.xcom_pull(task_ids='run_schedule') }}"

    # Wait for the schedule to complete before continuing
    task_wait_for_schedule = ParadimeBoltDbtScheduleRunSensor(task_id="wait_for_schedule", run_id=run_id)

    # Download the manifest.json file from the schedule run and return the path as the xcom return value
    task_download_manifest = ParadimeBoltDbtScheduleRunArtifactOperator(task_id="download_manifest", run_id=run_id, artifact_path="target/manifest.json")

    # Get the path to the manifest.json file from the xcom return value
    output_path = "{{ task_instance.xcom_pull(task_ids='download_manifest') }}"

    task_run_schedule >> task_wait_for_schedule >> task_download_manifest


run_schedule_and_download_manifest()

```

Refer to the [example DAGs](https://github.com/paradime-io/airflow-provider-paradime-dbt/tree/main/paradime_dbt_provider/example_dags) in this repository for more examples.
