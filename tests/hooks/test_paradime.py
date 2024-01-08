# Standard library modules
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import requests

# First party modules
from paradime_dbt_provider.hooks.paradime import BoltResource, ParadimeException, ParadimeHook


class TestParadimeHook(unittest.TestCase):
    def setUp(self):
        self.hook = ParadimeHook(conn_id="test_conn_id")

    def test_get_auth_config(self):
        # Mock
        extra = {
            "api_endpoint": "https://example.com",
            "api_key": "key",
            "api_secret": "secret",
        }
        conn = Mock()
        conn.extra_dejson = extra
        self.hook.get_connection = Mock(return_value=conn)

        # Call
        result = self.hook._get_auth_config()

        # Assert
        self.assertEqual(
            result,
            ParadimeHook.AuthConfig(
                api_endpoint="https://example.com",
                api_key="key",
                api_secret="secret",
            ),
        )

    def test_get_api_endpoint(self):
        # Mock
        self.hook._get_auth_config = Mock(
            return_value=ParadimeHook.AuthConfig(
                api_endpoint="https://example.com",
                api_key="key",
                api_secret="secret",
            )
        )

        # Call
        result = self.hook._get_api_endpoint()

        # Assert
        self.assertEqual(result, "https://example.com")

    def test_get_request_headers(self):
        # Mock
        self.hook._get_auth_config = Mock(
            return_value=ParadimeHook.AuthConfig(
                api_endpoint="https://example.com",
                api_key="key",
                api_secret="secret",
            )
        )

        # Call
        result = self.hook._get_request_headers()

        # Assert
        expected_headers = {
            "Content-Type": "application/json",
            "X-API-KEY": "key",
            "X-API-SECRET": "secret",
        }
        self.assertEqual(result, expected_headers)

    def test_raise_for_gql_errors_with_errors(self):
        # Mock
        mock_response = MagicMock()
        mock_response.json.return_value = {"errors": ["Error 1", "Error 2"]}

        # Call & Assert
        with self.assertRaises(ParadimeException) as context:
            self.hook._raise_for_gql_errors(mock_response)

        self.assertEqual(str(context.exception), "['Error 1', 'Error 2']")
        mock_response.json.assert_called_once()

    def test_raise_for_gql_errors_without_errors(self):
        # Mock
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"result": "success"}}

        # Call
        self.hook._raise_for_gql_errors(mock_response)

        # Assert
        mock_response.json.assert_called_once()

    def test_raise_for_errors_with_status_error(self):
        # Mock
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        # Call & Assert
        with self.assertRaises(ParadimeException) as context:
            self.hook._raise_for_errors(mock_response)

        expected_error_message = "Error: 500 - Internal Server Error"
        self.assertEqual(str(context.exception), expected_error_message)

    @patch.object(ParadimeHook, "_raise_for_gql_errors")
    def test_raise_for_errors_without_status_error(self, mock_raise_for_gql_errors):
        # Mock
        response = Mock()

        # Call
        self.hook._raise_for_errors(response)

        # Assert
        response.raise_for_status.assert_called_once()
        mock_raise_for_gql_errors.assert_called_once_with(response)

    @patch("requests.post")
    @patch.object(ParadimeHook, "_get_api_endpoint")
    @patch.object(ParadimeHook, "_get_request_headers")
    def test_call_gql(self, mock_get_headers, mock_get_api_endpoint, mock_post):
        # Mock
        mock_response = MagicMock(spec=requests.Response)
        mock_post.return_value = mock_response
        mock_response.json.return_value = {"data": {"result_key": "result_value"}}
        mock_response.raise_for_status.return_value = None

        mock_get_api_endpoint.return_value = "http://test-api-endpoint"
        mock_get_headers.return_value = {"Content-Type": "application/json"}

        # Call
        result = self.hook._call_gql(query="test_query", variables={"var_key": "var_value"})

        # Assert
        mock_get_api_endpoint.assert_called_once()
        mock_get_headers.assert_called_once()

        mock_post.assert_called_once_with(
            url="http://test-api-endpoint",
            json={"query": "test_query", "variables": {"var_key": "var_value"}},
            headers={"Content-Type": "application/json"},
            timeout=60,
        )

        self.assertEqual(result, {"result_key": "result_value"})

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_bolt_schedule(self, mock_call_gql):
        # Mock
        schedule_name = "test_schedule"
        expected_response = {
            "boltScheduleName": {
                "ok": True,
                "latestRunId": 1,
                "commands": ["command1", "command2"],
                "owner": "owner",
                "schedule": "*/5 * * * *",
                "uuid": "uuid",
                "source": "source",
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_bolt_schedule(schedule_name=schedule_name)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"scheduleName": schedule_name})
        self.assertEqual(result.name, schedule_name)
        self.assertEqual(result.commands, expected_response["boltScheduleName"]["commands"])
        self.assertEqual(result.owner, expected_response["boltScheduleName"]["owner"])
        self.assertEqual(result.schedule, expected_response["boltScheduleName"]["schedule"])
        self.assertEqual(result.uuid, expected_response["boltScheduleName"]["uuid"])
        self.assertEqual(result.source, expected_response["boltScheduleName"]["source"])
        self.assertEqual(result.latest_run_id, expected_response["boltScheduleName"]["latestRunId"])

    @patch.object(ParadimeHook, "_call_gql")
    def test_trigger_bolt_run(self, mock_call_gql):
        # Mock
        schedule_name = "test_schedule"
        commands = ["cmd1", "cmd2"]
        expected_response = {"triggerBoltRun": {"ok": True, "runId": 123}}
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.trigger_bolt_run(schedule_name=schedule_name, commands=commands)

        # Assert
        mock_call_gql.assert_called_once_with(
            query=unittest.mock.ANY,
            variables={"scheduleName": schedule_name, "commands": commands},
        )
        self.assertEqual(result, expected_response["triggerBoltRun"]["runId"])

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_bolt_run_status(self, mock_call_gql):
        # Mock
        run_id = 123
        expected_response = {"boltRunStatus": {"state": "SUCCESS"}}
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_bolt_run_status(run_id=run_id)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"runId": run_id})
        self.assertEqual(result, expected_response["boltRunStatus"]["state"])

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_bolt_run_commands(self, mock_call_gql):
        # Mock
        run_id = 123
        expected_response = {
            "boltRunStatus": {
                "commands": [
                    {
                        "id": 1,
                        "command": "cmd1",
                        "startDttm": "2024-01-01 00:00:00",
                        "endDttm": "2024-01-02 00:00:00",
                        "stdout": "output1",
                        "stderr": "error1",
                        "returnCode": 0,
                    },
                    {
                        "id": 2,
                        "command": "cmd2",
                        "startDttm": "2024-01-03 00:00:00",
                        "endDttm": "2024-01-04 00:00:00",
                        "stdout": "output2",
                        "stderr": "error2",
                        "returnCode": 1,
                    },
                ]
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_bolt_run_commands(run_id=run_id)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"runId": run_id})
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].id, 1)
        self.assertEqual(result[0].command, "cmd1")
        self.assertEqual(result[1].id, 2)
        self.assertEqual(result[1].command, "cmd2")

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_artifacts_from_command(self, mock_call_gql):
        # Mock
        command_id = 1
        expected_response = {
            "boltCommand": {
                "resources": [
                    {"id": 101, "path": "/path/to/resource1"},
                    {"id": 102, "path": "/path/to/resource2"},
                ]
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_artifacts_from_command(command_id=command_id)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"commandId": command_id})
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].id, 101)
        self.assertEqual(result[0].path, "/path/to/resource1")
        self.assertEqual(result[1].id, 102)
        self.assertEqual(result[1].path, "/path/to/resource2")

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_artifact_from_command_by_path(self, mock_call_gql):
        # Mock
        command_id = 1
        artifact_path = "/path/to/resource1"
        expected_response = {
            "boltCommand": {
                "resources": [
                    {"id": 101, "path": "/path/to/resource1"},
                    {"id": 102, "path": "/path/to/resource2"},
                ]
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_artifact_from_command_by_path(command_id=command_id, artifact_path=artifact_path)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"commandId": command_id})
        self.assertEqual(result, BoltResource(id=101, path="/path/to/resource1"))

    def test_get_artifact_from_command_by_path_not_found(self):
        # Mock
        command_id = 1
        artifact_path = "/path/to/resource1"
        expected_response = {
            "boltCommand": {
                "resources": [
                    {"id": 102, "path": "/path/to/resource2"},
                ]
            }
        }
        self.hook._call_gql = Mock(return_value=expected_response)

        # Call
        result = self.hook.get_artifact_from_command_by_path(command_id=command_id, artifact_path=artifact_path)

        # Assert
        self.assertIsNone(result)

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_artifact_download_url(self, mock_call_gql):
        # Mock
        artifact_id = 101
        expected_response = {
            "boltResourceUrl": {
                "ok": True,
                "url": "https://example.com/download/resource1",
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_artifact_download_url(artifact_id=artifact_id)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"resourceId": artifact_id})
        self.assertEqual(result, expected_response["boltResourceUrl"]["url"])

    @patch.object(ParadimeHook, "_call_gql")
    def test_cancel_bolt_run(self, mock_call_gql):
        # Mock
        run_id = 123
        expected_response = {"cancelBoltRun": {"ok": True, "errorLog": ""}}
        mock_call_gql.return_value = expected_response

        # Call
        self.hook.cancel_bolt_run(run_id=run_id)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"runId": run_id})

    @patch.object(ParadimeHook, "_call_gql")
    def test_download_artifact(self, mock_call_gql):
        # Mock
        artifact_id = 101
        output_file_name = "output.txt"
        expected_response = {
            "boltResourceUrl": {
                "ok": True,
                "url": "https://example.com/download/resource1",
            }
        }
        mock_call_gql.return_value = expected_response

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.text = "Artifact Content"
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            # Call
            result = self.hook.download_artifact(artifact_id=artifact_id, output_file_name=output_file_name)

            # Assert
            mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"resourceId": artifact_id})
            mock_get.assert_called_once_with(url=expected_response["boltResourceUrl"]["url"], timeout=300)
            mock_response.raise_for_status.assert_called_once()
            self.assertEqual(result, Path(output_file_name).absolute().as_posix())
            Path(output_file_name).unlink()

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_workspaces(self, mock_call_gql):
        # Mock
        expected_response = {
            "listWorkspaces": {
                "workspaces": [
                    {"name": "Workspace1", "uid": "uid1"},
                    {"name": "Workspace2", "uid": "uid2"},
                ]
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_workspaces()

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={})
        self.assertEqual(result, expected_response["listWorkspaces"]["workspaces"])

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_active_users(self, mock_call_gql):
        # Mock
        expected_response = {
            "listUsers": {
                "activeUsers": [
                    {
                        "uid": "uid1",
                        "email": "user1@example.com",
                        "name": "User1",
                        "accountType": "ADMIN",
                    },
                    {
                        "uid": "uid2",
                        "email": "user2@example.com",
                        "name": "User2",
                        "accountType": "DEVELOPER",
                    },
                ]
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_active_users()

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={})
        self.assertEqual(result, expected_response["listUsers"]["activeUsers"])

    @patch.object(ParadimeHook, "_call_gql")
    def test_get_invited_users(self, mock_call_gql):
        # Mock
        expected_response = {
            "listUsers": {
                "invitedUsers": [
                    {
                        "email": "invitee1@example.com",
                        "accountType": "DEVELOPER",
                        "inviteStatus": "PENDING",
                    },
                    {
                        "email": "invitee2@example.com",
                        "accountType": "BUSINESS",
                        "inviteStatus": "ACCEPTED",
                    },
                ]
            }
        }
        mock_call_gql.return_value = expected_response

        # Call
        result = self.hook.get_invited_users()

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={})
        self.assertEqual(result, expected_response["listUsers"]["invitedUsers"])

    @patch.object(ParadimeHook, "_call_gql")
    def test_invite_user(self, mock_call_gql):
        # Mock
        email = "invitee@example.com"
        account_type = ParadimeHook.UserAccountType.DEVELOPER
        expected_response = {"inviteUser": {"ok": True}}
        mock_call_gql.return_value = expected_response

        # Call
        self.hook.invite_user(email=email, account_type=account_type)

        # Assert
        mock_call_gql.assert_called_once_with(
            query=unittest.mock.ANY,
            variables={"email": email, "accountType": account_type.value},
        )

    @patch.object(ParadimeHook, "_call_gql")
    def test_update_user_account_type(self, mock_call_gql):
        # Mock
        uid = "user123"
        account_type = ParadimeHook.UserAccountType.BUSINESS
        expected_response = {"updateUserAccountType": {"ok": True}}
        mock_call_gql.return_value = expected_response

        # Call
        self.hook.update_user_account_type(uid=uid, account_type=account_type)

        # Assert
        mock_call_gql.assert_called_once_with(
            query=unittest.mock.ANY,
            variables={"uid": uid, "accountType": account_type.value},
        )

    @patch.object(ParadimeHook, "_call_gql")
    def test_disable_user(self, mock_call_gql):
        # Mock
        uid = "user123"
        expected_response = {"disableUser": {"ok": True}}
        mock_call_gql.return_value = expected_response

        # Call
        self.hook.disable_user(uid=uid)

        # Assert
        mock_call_gql.assert_called_once_with(query=unittest.mock.ANY, variables={"uid": uid})


if __name__ == "__main__":
    unittest.main()
