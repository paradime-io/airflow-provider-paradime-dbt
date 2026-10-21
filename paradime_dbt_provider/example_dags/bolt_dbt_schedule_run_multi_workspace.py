from airflow.decorators import dag  # type: ignore[import]

from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunArtifactOperator, ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor

# One connection with an account API key (prdm_cmp_...) can trigger schedules in every
# workspace the key can access. Each task picks its workspace with `workspace_token`.
PARADIME_CONN_ID = "your_paradime_conn_id"  # Update this to your connection id

# Map each Bolt schedule to the workspace that owns it.
# The workspace token (UID) is listed next to each workspace when you create the account API key.
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
