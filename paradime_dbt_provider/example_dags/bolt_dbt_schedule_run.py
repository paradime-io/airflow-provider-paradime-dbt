# Third party modules
from datetime import datetime

from airflow.decorators import dag

from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunArtifactOperator, ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor


@dag(
    start_date=datetime(2024, 1, 1),
    default_args={"conn_id": "paradime"},  # Update this to your connection id
)
def run_schedule_and_download_manifest():
    # Run the schedule and return the run id as the xcom return value
    task_run_schedule = ParadimeBoltDbtScheduleRunOperator(task_id="run_schedule", schedule_name="your_schedule_name")  # Update this to your schedule name

    # Get the run id from the xcom return value
    run_id = "{{ task_instance.xcom_pull(task_ids='run_schedule') }}"

    # This will wait for the schedule to complete before continuing
    task_wait_for_schedule = ParadimeBoltDbtScheduleRunSensor(task_id="wait_for_schedule", run_id=run_id)

    # Download the manifest.json file from the schedule run and return the path as the xcom return value
    task_download_manifest = ParadimeBoltDbtScheduleRunArtifactOperator(task_id="download_manifest", run_id=run_id, artifact_path="target/manifest.json")

    # Get the path to the manifest.json file from the xcom return value
    output_path = "{{ task_instance.xcom_pull(task_ids='download') }}"

    task_run_schedule >> task_wait_for_schedule >> task_download_manifest


run_schedule_and_download_manifest()
