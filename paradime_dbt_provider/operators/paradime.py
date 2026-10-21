from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from airflow.models import BaseOperator  # type: ignore[import]

from paradime_dbt_provider.hooks.paradime import BoltRunState, ParadimeException, ParadimeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context  # type: ignore


class ParadimeBoltDbtScheduleRunOperator(BaseOperator):
    """
    Triggers a Paradime Bolt dbt schedule run.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    :param slug: The slug of the bolt schedule to run (preferred). Returned by
        ``createBoltSchedule`` and shown in the Bolt UI.
    :param schedule_name: Deprecated alias for ``slug`` — carries a schedule
        slug. Kept for backwards compatibility with existing DAGs. Exactly one
        of ``slug`` or ``schedule_name`` must be provided.
    :param commands: Optional. A list of dbt commands to run. This will override the commands defined in the schedule.
    :param branch: Optional. A branch or commit hash to run the schedule on. This will override the branch defined in the schedule.
    :param retry_from_failure: Optional. When True, if the schedule's most
        recent run failed, resume it from the point of failure (re-running only
        the failed commands) instead of triggering a fresh run. If the latest
        run did not fail (or there is no previous run), a new run is triggered
        as usual. Note that ``commands`` and ``branch`` overrides do not apply
        to a resumed run. Defaults to False.
    :param workspace_token: Optional. The workspace to call when the connection uses an
        account API key. Overrides the default workspace token set on the connection, so
        one connection can reach every workspace the key can access. Templated.
    """

    template_fields = ["slug", "schedule_name", "commands", "workspace_token"]

    def __init__(
        self,
        *,
        conn_id: str,
        slug: str | None = None,
        schedule_name: str | None = None,
        commands: list[str] | None = None,
        branch: str | None = None,
        retry_from_failure: bool = False,
        workspace_token: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.slug = slug
        self.schedule_name = schedule_name
        self.workspace_token = workspace_token
        self.hook = ParadimeHook(conn_id=conn_id, workspace_token=workspace_token)
        self.commands = commands
        self.branch = branch
        self.retry_from_failure = retry_from_failure

    def execute(self, context: Context) -> int:
        self.hook.workspace_token = self.workspace_token  # pick up the rendered template value
        # XOR validation is deferred to the hook so DAG parsing doesn't raise
        # on import — only the scheduler-heartbeat-driven execute() does.
        if self.retry_from_failure and self._latest_run_failed():
            self.log.info(f"Latest run of schedule {self.slug or self.schedule_name!r} failed; resuming from the point of failure")
            return self.hook.retry_bolt_schedule_from_failure(
                slug=self.slug,
                schedule_name=self.schedule_name,
            )

        run_id = self.hook.trigger_bolt_run(
            slug=self.slug,
            schedule_name=self.schedule_name,
            commands=self.commands,
            branch=self.branch,
        )
        return run_id

    def _latest_run_failed(self) -> bool:
        """Return True if the schedule's most recent run ended in a failure state."""
        schedule = self.hook.get_bolt_schedule(slug=self.slug, schedule_name=self.schedule_name)
        if schedule.latest_run_id is None:
            return False

        state = self.hook.get_bolt_run_status(schedule.latest_run_id)
        return state in (BoltRunState.FAILED.value, BoltRunState.ERROR.value)


class ParadimeBoltDbtScheduleRunArtifactOperator(BaseOperator):
    """
    Downloads the artifact from a Paradime Bolt dbt schedule run.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    :param run_id: The schedule run id to download the artifact from.
    :param artifact_path: The path to download the artifact to. Example: target/manifest.json
    :param command_index: Optional. The index of the command to download the artifact from. Defaults to searching from the last command up to the first, and returning the first artifact found. Index starts at 0.
    :param output_file_name: Optional. The name of the file to download the artifact to. Defaults to the <run_id>_<artifact_file_name>. Example: 42_manifest.json
    :param workspace_token: Optional. The workspace to call when the connection uses an
        account API key. Overrides the default workspace token set on the connection, so
        one connection can reach every workspace the key can access. Templated.
    """

    template_fields = ["run_id", "artifact_path", "command_index", "output_file_name", "workspace_token"]

    def __init__(
        self,
        *,
        conn_id: str,
        run_id: int,
        artifact_path: str,
        command_index: int | None = None,
        output_file_name: str | None = None,
        workspace_token: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.workspace_token = workspace_token
        self.hook = ParadimeHook(conn_id=conn_id, workspace_token=workspace_token)
        self.run_id = run_id
        self.artifact_path = artifact_path
        self.command_index = command_index
        self.output_file_name = output_file_name

    def execute(self, context: Context) -> str:
        self.hook.workspace_token = self.workspace_token  # pick up the rendered template value
        run_commands = self.hook.get_bolt_run_commands(run_id=self.run_id)

        commands_to_search = []
        if self.command_index is None:
            commands_to_search = run_commands[::-1]
        else:
            if len(run_commands) <= self.command_index:
                raise ParadimeException(f"command_index {self.command_index!r} is out of range for run_id {self.run_id}. There are only {len(run_commands)} commands.")

            commands_to_search = [run_commands[self.command_index]]

        artifact = None
        for command in commands_to_search:
            self.log.info(f"Searching for artifact {self.artifact_path!r} in command ({command.id}) {command.command!r}")
            artifact = self.hook.get_artifact_from_command_by_path(command_id=command.id, artifact_path=self.artifact_path)
            if artifact is not None:
                self.log.info(f"Found artifact {self.artifact_path!r} in command ({command.id}) {command.command!r}")
                break
            else:
                self.log.info(f"Artifact {self.artifact_path!r} not found in command ({command.id}) {command.command!r}")

        if artifact is None:
            raise ParadimeException(f"Artifact {self.artifact_path!r} not found in run {self.run_id!r}")

        if self.output_file_name is None:
            self.output_file_name = f"{self.run_id}_{Path(self.artifact_path).name}"

        output_file_path = self.hook.download_artifact(artifact_id=artifact.id, output_file_name=self.output_file_name)

        self.log.info(f"Downloaded artifact {self.artifact_path!r} from run {self.run_id!r} to {output_file_path!r}")

        return output_file_path
