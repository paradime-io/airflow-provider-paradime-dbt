"""End-to-end check: one account API key connection, schedules in two workspaces.

Runs the real hook against a mocked HTTP layer and asserts that each task sends
its own ``X-Paradime-Workspace`` header through the same connection.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from paradime_dbt_provider.hooks.paradime import ParadimeHook
from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunArtifactOperator, ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor

CONN_EXTRA = {"api_endpoint": "https://api.paradime.io/api/v1/company/graphql", "api_token": "prdm_cmp_abc"}

# Run ids returned per workspace, so the mocked API can check that each call
# reaches the workspace that owns the run.
RUN_IDS = {"ws_marketing": 101, "ws_finance": 202}


def _fake_post(*, url, json, headers, proxies, timeout):  # noqa: A002 - mirrors requests.post kwargs
    workspace = headers["X-Paradime-Workspace"]
    assert headers["Authorization"] == "Bearer prdm_cmp_abc"
    query = json["query"]
    variables = json["variables"]

    if "triggerBoltRun" in query:
        data = {"triggerBoltRun": {"runId": RUN_IDS[workspace]}}
    elif "boltRunStatus" in query and "commands" in query:
        assert variables["runId"] == RUN_IDS[workspace]
        data = {"boltRunStatus": {"commands": [{"id": RUN_IDS[workspace], "command": "dbt run", "startDttm": "", "endDttm": "", "stdout": "", "stderr": "", "returnCode": 0}]}}
    elif "boltRunStatus" in query:
        assert variables["runId"] == RUN_IDS[workspace]
        data = {"boltRunStatus": {"state": "SUCCESS"}}
    elif "boltCommand" in query:
        data = {"boltCommand": {"resources": [{"id": RUN_IDS[workspace], "path": "target/manifest.json"}]}}
    elif "boltResourceUrl" in query:
        data = {"boltResourceUrl": {"url": f"https://s3.example.com/{workspace}/manifest.json"}}
    else:
        raise AssertionError(f"Unexpected query: {query}")

    response = MagicMock()
    response.json.return_value = {"data": data}
    return response


def _fake_get(*, url, proxies, timeout):
    response = MagicMock()
    response.text = url
    return response


class TestWorkspacePerTask(unittest.TestCase):
    def setUp(self):
        conn = Mock()
        conn.extra_dejson = CONN_EXTRA
        patcher = patch.object(ParadimeHook, "get_connection", return_value=conn)
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch("paradime_dbt_provider.hooks.paradime.requests.get", side_effect=_fake_get)
    @patch("paradime_dbt_provider.hooks.paradime.requests.post", side_effect=_fake_post)
    def test_one_connection_two_workspaces(self, mock_post, mock_get):
        with tempfile.TemporaryDirectory() as tmp:
            for workspace, slug in (("ws_marketing", "marketing-daily"), ("ws_finance", "finance-daily")):
                with self.subTest(workspace=workspace):
                    run_id = ParadimeBoltDbtScheduleRunOperator(
                        task_id=f"run_{workspace}", conn_id="paradime", slug=slug, workspace_uid=workspace
                    ).execute(context=MagicMock())
                    self.assertEqual(run_id, RUN_IDS[workspace])

                    sensor = ParadimeBoltDbtScheduleRunSensor(task_id=f"wait_{workspace}", conn_id="paradime", run_id=run_id, workspace_uid=workspace)
                    self.assertTrue(sensor.poke(context=MagicMock()))

                    output_file = str(Path(tmp) / f"{workspace}_manifest.json")
                    path = ParadimeBoltDbtScheduleRunArtifactOperator(
                        task_id=f"download_{workspace}",
                        conn_id="paradime",
                        run_id=run_id,
                        artifact_path="target/manifest.json",
                        output_file_name=output_file,
                        workspace_uid=workspace,
                    ).execute(context=MagicMock())
                    self.assertEqual(Path(path).read_text(), f"https://s3.example.com/{workspace}/manifest.json")

        sent_workspaces = {call.kwargs["headers"]["X-Paradime-Workspace"] for call in mock_post.call_args_list}
        self.assertEqual(sent_workspaces, set(RUN_IDS))

    def test_rendered_template_value_is_used_at_execute(self):
        """``workspace_uid`` is a template field: the value rendered before execute() wins."""
        operator = ParadimeBoltDbtScheduleRunOperator(task_id="run", conn_id="paradime", slug="s", workspace_uid="{{ params.ws }}")
        self.assertIn("workspace_uid", operator.template_fields)
        operator.workspace_uid = "ws_finance"  # what Airflow does after rendering

        with patch("paradime_dbt_provider.hooks.paradime.requests.post", side_effect=_fake_post):
            self.assertEqual(operator.execute(context=MagicMock()), RUN_IDS["ws_finance"])


if __name__ == "__main__":
    unittest.main()
