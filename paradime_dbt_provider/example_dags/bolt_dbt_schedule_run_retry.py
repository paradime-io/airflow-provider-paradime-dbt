from airflow.decorators import dag  # type: ignore[import]

from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor

PARADIME_CONN_ID = "your_paradime_conn_id"  # Update this to your connection id
BOLT_SCHEDULE_SLUG = "your-schedule-slug"  # Update this to the slug returned by createBoltSchedule (shown in the Bolt UI)


@dag(
    default_args={"conn_id": PARADIME_CONN_ID},
)
def run_schedule_with_retry_from_failure():
    # Run the schedule and return the run id as the xcom return value.
    # With retry_from_failure=True, if the schedule's most recent run failed,
    # this resumes it from the point of failure (re-running only the failed
    # commands) instead of triggering a fresh run.
    task_run_schedule = ParadimeBoltDbtScheduleRunOperator(
        task_id="run_schedule",
        slug=BOLT_SCHEDULE_SLUG,
        retry_from_failure=True,
    )

    # Get the run id from the xcom return value
    run_id = "{{ task_instance.xcom_pull(task_ids='run_schedule') }}"

    # Wait for the run to complete before continuing
    task_wait_for_schedule = ParadimeBoltDbtScheduleRunSensor(task_id="wait_for_schedule", run_id=run_id)

    task_run_schedule >> task_wait_for_schedule


run_schedule_with_retry_from_failure()
