from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from airflow.models import BaseOperator  # type: ignore[import]

from paradime_dbt_provider.hooks.paradime import ParadimeException, ParadimeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context  # type: ignore


class ParadimeBoltDbtScheduleRunOperator(BaseOperator):
    """
    Triggers a Paradime Bolt dbt schedule run.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    :param schedule_name: The name of the bolt schedule to run.
    :param commands: Optional. A list of dbt commands to run. This will override the commands defined in the schedule.
    """

    template_fields = ["schedule_name", "commands"]

    def __init__(
        self,
        *,
        conn_id: str,
        schedule_name: str,
        commands: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.schedule_name = schedule_name
        self.hook = ParadimeHook(conn_id=conn_id)
        self.commands = commands

    def execute(self, context: Context) -> int:
        run_id = self.hook.trigger_bolt_run(schedule_name=self.schedule_name, commands=self.commands)
        return run_id


class ParadimeBoltDbtScheduleRunArtifactOperator(BaseOperator):
    """
    Downloads the artifact from a Paradime Bolt dbt schedule run.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    :param run_id: The schedule run id to download the artifact from.
    :param artifact_path: The path to download the artifact to. Example: target/manifest.json
    :param command_index: Optional. The index of the command to download the artifact from. Defaults to searching from the last command up to the first, and returning the first artifact found. Index starts at 0.
    :param output_file_name: Optional. The name of the file to download the artifact to. Defaults to the <run_id>_<artifact_file_name>. Example: 42_manifest.json
    """

    template_fields = ["run_id", "artifact_path", "command_index", "output_file_name"]

    def __init__(
        self,
        *,
        conn_id: str,
        run_id: int,
        artifact_path: str,
        command_index: int | None = None,
        output_file_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hook = ParadimeHook(conn_id=conn_id)
        self.run_id = run_id
        self.artifact_path = artifact_path
        self.command_index = command_index
        self.output_file_name = output_file_name

    def execute(self, context: Context) -> str:
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
