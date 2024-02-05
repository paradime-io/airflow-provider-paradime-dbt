from airflow.decorators import dag  # type: ignore[import]

from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunArtifactOperator, ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor

PARADIME_CONN_ID = "your_paradime_conn_id"  # Update this to your connection id
BOLT_SCHEDULE_NAME = "your_schedule_name"  # Update this to your schedule name


@dag(
    default_args={"conn_id": PARADIME_CONN_ID},
)
def run_schedule_with_custom_commands():
    # Define the custom commands to run
    custom_commands = ["dbt run", "dbt test"]

    # Run the schedule with custom commands and return the run id as the xcom return value
    task_run_schedule = ParadimeBoltDbtScheduleRunOperator(task_id="run_schedule", schedule_name=BOLT_SCHEDULE_NAME, commands=custom_commands)

    # Get the run id from the xcom return value
    run_id = "{{ task_instance.xcom_pull(task_ids='run_schedule') }}"

    # Wait for the schedule to complete before continuing
    task_wait_for_schedule = ParadimeBoltDbtScheduleRunSensor(task_id="wait_for_schedule", run_id=run_id)

    # Download the manifest.json file from the schedule run and return the path as the xcom return value
    task_download_manifest = ParadimeBoltDbtScheduleRunArtifactOperator(task_id="download_manifest", run_id=run_id, artifact_path="target/manifest.json")

    # Get the path to the manifest.json file from the xcom return value
    output_path = "{{ task_instance.xcom_pull(task_ids='download_manifest') }}"

    task_run_schedule >> task_wait_for_schedule >> task_download_manifest


run_schedule_with_custom_commands()
