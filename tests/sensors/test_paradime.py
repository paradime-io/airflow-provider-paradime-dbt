import unittest
from unittest.mock import MagicMock, patch

from airflow.utils.context import Context  # type: ignore[import]

from paradime_dbt_provider.hooks.paradime import ParadimeException
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor


class TestParadimeBoltDbtScheduleRunSensor(unittest.TestCase):
    @patch("paradime_dbt_provider.sensors.paradime.ParadimeHook")
    def setUp(self, mock_paradime_hook):
        self.conn_id = "paradime_conn_default"
        self.run_id = 123
        self.task_id = "test_task"
        self.sensor = ParadimeBoltDbtScheduleRunSensor(task_id=self.task_id, conn_id=self.conn_id, run_id=self.run_id)
        self.mock_hook_instance = mock_paradime_hook.return_value

    def test_init(self):
        # Assert
        self.assertEqual(self.sensor.task_id, self.task_id)
        self.assertEqual(self.sensor.hook, self.mock_hook_instance)
        self.assertEqual(self.sensor.run_id, self.run_id)

    def test_poke_success(self):
        # Mock
        self.mock_hook_instance.get_bolt_run_status.return_value = "SUCCESS"
        context = MagicMock(spec=Context)

        # Call
        result = self.sensor.poke(context)

        # Assert
        self.assertTrue(result)
        self.mock_hook_instance.get_bolt_run_status.assert_called_once_with(self.run_id)

    def test_poke_failure(self):
        # Mock
        self.mock_hook_instance.get_bolt_run_status.return_value = "FAILED"
        context = MagicMock(spec=Context)

        # Call/Assert
        with self.assertRaises(ParadimeException):
            self.sensor.poke(context)

        self.mock_hook_instance.get_bolt_run_status.assert_called_once_with(self.run_id)

    def test_poke_error(self):
        # Mock
        self.mock_hook_instance.get_bolt_run_status.return_value = "ERROR"
        context = MagicMock(spec=Context)

        # Call/Assert
        with self.assertRaises(ParadimeException):
            self.sensor.poke(context)

        self.mock_hook_instance.get_bolt_run_status.assert_called_once_with(self.run_id)

    def test_poke_canceled(self):
        # Mock
        self.mock_hook_instance.get_bolt_run_status.return_value = "CANCELED"
        context = MagicMock(spec=Context)

        # Call/Assert
        with self.assertRaises(ParadimeException):
            self.sensor.poke(context)

        self.mock_hook_instance.get_bolt_run_status.assert_called_once_with(self.run_id)

    def test_poke_running(self):
        # Mock
        self.mock_hook_instance.get_bolt_run_status.return_value = "RUNNING"
        context = MagicMock(spec=Context)

        # Call
        result = self.sensor.poke(context)

        # Assert
        self.assertFalse(result)
        self.mock_hook_instance.get_bolt_run_status.assert_called_once_with(self.run_id)


if __name__ == "__main__":
    unittest.main()
