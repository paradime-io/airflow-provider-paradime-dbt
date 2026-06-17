import unittest
from unittest.mock import MagicMock, patch

from paradime_dbt_provider.hooks.paradime import BoltCommand, ParadimeHook
from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunArtifactOperator, ParadimeBoltDbtScheduleRunOperator


class TestParadimeBoltDbtScheduleRunOperator(unittest.TestCase):
    @patch("paradime_dbt_provider.operators.paradime.ParadimeHook")
    def setUp(self, mock_paradime_hook):
        self.conn_id = "paradime_conn_default"
        self.slug = "test-schedule-a1b2c3"
        self.operator = ParadimeBoltDbtScheduleRunOperator(conn_id=self.conn_id, slug=self.slug, task_id="test_task")
        self.mock_hook_instance = mock_paradime_hook.return_value

    def test_init_with_slug(self):
        self.assertEqual(self.operator.hook, self.mock_hook_instance)
        self.assertEqual(self.operator.slug, self.slug)
        self.assertIsNone(self.operator.schedule_name)

    @patch("paradime_dbt_provider.operators.paradime.ParadimeHook")
    def test_init_with_deprecated_schedule_name(self, mock_paradime_hook):
        """Existing DAGs using ``schedule_name=`` still construct the operator successfully.

        XOR validation is deferred to execute() so DAG parsing never raises on
        import, no matter what kwargs the caller used.
        """
        operator = ParadimeBoltDbtScheduleRunOperator(
            conn_id=self.conn_id, schedule_name="legacy_name", task_id="legacy_task"
        )
        self.assertIsNone(operator.slug)
        self.assertEqual(operator.schedule_name, "legacy_name")

    @patch.object(ParadimeHook, "trigger_bolt_run")
    def test_execute(self, mock_trigger_bolt_run):
        # Mock
        run_id = 123
        mock_trigger_bolt_run.return_value = run_id
        self.mock_hook_instance.trigger_bolt_run = mock_trigger_bolt_run
        context = MagicMock()

        # Call
        result = self.operator.execute(context)

        # Assert
        self.assertEqual(result, run_id)
        self.mock_hook_instance.trigger_bolt_run.assert_called_once_with(
            slug=self.slug, schedule_name=None, commands=None, branch=None
        )

    @patch.object(ParadimeHook, "trigger_bolt_run")
    def test_execute_with_commands(self, mock_trigger_bolt_run):
        # Mock
        run_id = 42
        commands = ["dbt run", "dbt test"]
        self.operator.commands = commands
        mock_trigger_bolt_run.return_value = run_id
        self.mock_hook_instance.trigger_bolt_run = mock_trigger_bolt_run
        context = MagicMock()

        # Call
        result = self.operator.execute(context)

        # Assert
        self.assertEqual(result, run_id)
        self.mock_hook_instance.trigger_bolt_run.assert_called_once_with(
            slug=self.slug, schedule_name=None, commands=commands, branch=None
        )

    @patch.object(ParadimeHook, "trigger_bolt_run")
    def test_execute_with_branch(self, mock_trigger_bolt_run):
        # Mock
        run_id = 42
        branch = "feature/branch"
        self.operator.branch = branch
        mock_trigger_bolt_run.return_value = run_id
        self.mock_hook_instance.trigger_bolt_run = mock_trigger_bolt_run
        context = MagicMock()

        # Call
        result = self.operator.execute(context)

        # Assert
        self.assertEqual(result, run_id)
        self.mock_hook_instance.trigger_bolt_run.assert_called_once_with(
            slug=self.slug, schedule_name=None, commands=None, branch=branch
        )

    @patch("paradime_dbt_provider.operators.paradime.ParadimeHook")
    def test_execute_forwards_deprecated_schedule_name(self, mock_paradime_hook):
        """Legacy DAGs using ``schedule_name=`` forward the value to the hook unchanged.

        The hook's XOR helper handles the resolution + DeprecationWarning at
        runtime — the operator itself just passes both kwargs through.
        """
        operator = ParadimeBoltDbtScheduleRunOperator(
            conn_id=self.conn_id, schedule_name="legacy_name", task_id="legacy_task"
        )
        mock_hook_instance = mock_paradime_hook.return_value
        mock_hook_instance.trigger_bolt_run.return_value = 99

        result = operator.execute(MagicMock())

        self.assertEqual(result, 99)
        mock_hook_instance.trigger_bolt_run.assert_called_once_with(
            slug=None, schedule_name="legacy_name", commands=None, branch=None
        )


class TestParadimeBoltDbtScheduleRunArtifactOperator(unittest.TestCase):
    @patch("paradime_dbt_provider.operators.paradime.ParadimeHook")
    def test_init(self, mock_paradime_hook):
        mock_conn_id = "paradime_conn_default"
        mock_run_id = 123
        mock_artifact_path = "target/manifest.json"
        mock_operator = ParadimeBoltDbtScheduleRunArtifactOperator(conn_id=mock_conn_id, run_id=mock_run_id, artifact_path=mock_artifact_path, task_id="test_task")
        mock_hook_instance = mock_paradime_hook.return_value

        # Assert
        self.assertEqual(mock_operator.hook, mock_hook_instance)
        self.assertEqual(mock_operator.run_id, mock_run_id)
        self.assertEqual(mock_operator.artifact_path, mock_artifact_path)
        self.assertIsNone(mock_operator.command_index)
        self.assertIsNone(mock_operator.output_file_name)

    @patch("paradime_dbt_provider.operators.paradime.ParadimeHook")
    def test_execute_success(self, mock_paradime_hook):
        mock_conn_id = "paradime_conn_default"
        mock_run_id = 123
        mock_artifact_path = "target/manifest.json"
        mock_operator = ParadimeBoltDbtScheduleRunArtifactOperator(conn_id=mock_conn_id, run_id=mock_run_id, artifact_path=mock_artifact_path, task_id="test_task")
        mock_hook_instance = mock_paradime_hook.return_value

        run_commands = [
            BoltCommand(id=1, command="dbt run", start_dttm="2021-01-01T00:00:00.000Z", end_dttm="2021-01-01T00:00:00.000Z", stderr="", stdout="", return_code=0),
            BoltCommand(id=2, command="dbt test", start_dttm="2021-01-01T00:00:00.000Z", end_dttm="2021-01-01T00:00:00.000Z", stderr="", stdout="", return_code=0),
        ]
        mock_hook_instance.get_bolt_run_commands.return_value = run_commands
        artifact_id = 456
        mock_hook_instance.get_artifact_from_command_by_path.return_value = MagicMock(id=artifact_id)
        context = MagicMock()

        # Call
        result = mock_operator.execute(context)

        # Assert
        self.assertEqual(result, mock_hook_instance.download_artifact.return_value)
        mock_hook_instance.get_bolt_run_commands.assert_called_once_with(run_id=mock_run_id)
        mock_hook_instance.get_artifact_from_command_by_path.assert_called_once_with(command_id=run_commands[1].id, artifact_path=mock_artifact_path)
        mock_hook_instance.download_artifact.assert_called_once_with(artifact_id=artifact_id, output_file_name="123_manifest.json")

    @patch("paradime_dbt_provider.operators.paradime.ParadimeHook")
    def test_execute_success_with_custom_file_name(self, mock_paradime_hook):
        mock_conn_id = "paradime_conn_default"
        mock_run_id = 123
        mock_artifact_path = "target/manifest.json"
        mock_operator = ParadimeBoltDbtScheduleRunArtifactOperator(conn_id=mock_conn_id, run_id=mock_run_id, artifact_path=mock_artifact_path, output_file_name="custom_name.json", task_id="test_task")
        mock_hook_instance = mock_paradime_hook.return_value

        run_commands = [
            BoltCommand(id=1, command="dbt run", start_dttm="2021-01-01T00:00:00.000Z", end_dttm="2021-01-01T00:00:00.000Z", stderr="", stdout="", return_code=0),
            BoltCommand(id=2, command="dbt test", start_dttm="2021-01-01T00:00:00.000Z", end_dttm="2021-01-01T00:00:00.000Z", stderr="", stdout="", return_code=0),
        ]
        mock_hook_instance.get_bolt_run_commands.return_value = run_commands
        artifact_id = 456
        mock_hook_instance.get_artifact_from_command_by_path.return_value = MagicMock(id=artifact_id)
        context = MagicMock()

        # Call
        result = mock_operator.execute(context)

        # Assert
        self.assertEqual(result, mock_hook_instance.download_artifact.return_value)
        mock_hook_instance.get_bolt_run_commands.assert_called_once_with(run_id=mock_run_id)
        mock_hook_instance.get_artifact_from_command_by_path.assert_called_once_with(command_id=run_commands[1].id, artifact_path=mock_artifact_path)
        mock_hook_instance.download_artifact.assert_called_once_with(artifact_id=artifact_id, output_file_name="custom_name.json")

    @patch("paradime_dbt_provider.operators.paradime.ParadimeHook")
    def test_execute_success_with_command_idx(self, mock_paradime_hook):
        mock_conn_id = "paradime_conn_default"
        mock_run_id = 123
        mock_artifact_path = "target/manifest.json"
        mock_operator = ParadimeBoltDbtScheduleRunArtifactOperator(conn_id=mock_conn_id, run_id=mock_run_id, artifact_path=mock_artifact_path, command_index=1, task_id="test_task")
        mock_hook_instance = mock_paradime_hook.return_value

        run_commands = [
            BoltCommand(id=1, command="dbt run", start_dttm="2021-01-01T00:00:00.000Z", end_dttm="2021-01-01T00:00:00.000Z", stderr="", stdout="", return_code=0),
            BoltCommand(id=2, command="dbt test", start_dttm="2021-01-01T00:00:00.000Z", end_dttm="2021-01-01T00:00:00.000Z", stderr="", stdout="", return_code=0),
            BoltCommand(id=3, command="dbt test", start_dttm="2021-01-01T00:00:00.000Z", end_dttm="2021-01-01T00:00:00.000Z", stderr="", stdout="", return_code=0),
        ]
        mock_hook_instance.get_bolt_run_commands.return_value = run_commands
        artifact_id = 456
        mock_hook_instance.get_artifact_from_command_by_path.return_value = MagicMock(id=artifact_id)
        context = MagicMock()

        # Call
        result = mock_operator.execute(context)

        # Assert
        self.assertEqual(result, mock_hook_instance.download_artifact.return_value)
        mock_hook_instance.get_bolt_run_commands.assert_called_once_with(run_id=mock_run_id)
        mock_hook_instance.get_artifact_from_command_by_path.assert_called_once_with(command_id=run_commands[1].id, artifact_path=mock_artifact_path)
        mock_hook_instance.download_artifact.assert_called_once_with(artifact_id=artifact_id, output_file_name="123_manifest.json")


if __name__ == "__main__":
    unittest.main()
