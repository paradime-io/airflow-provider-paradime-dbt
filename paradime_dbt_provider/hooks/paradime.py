from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import requests
from airflow.hooks.base import BaseHook  # type: ignore[import]


@dataclass
class BoltSchedule:
    name: str
    commands: list[str]
    schedule: str
    uuid: str
    source: str
    owner: str
    latest_run_id: int | None


@dataclass
class BoltCommand:
    id: int
    command: str
    start_dttm: str
    end_dttm: str
    stdout: str
    stderr: str
    return_code: int | None


@dataclass
class BoltResource:
    id: int
    path: str


class ParadimeHook(BaseHook):
    conn_name_attr = "conn_id"
    default_conn_name = "paradime_conn_default"
    conn_type = "paradime"
    hook_name = "Paradime"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        # Third party modules
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget  # type: ignore[import]
        from flask_babel import lazy_gettext  # type: ignore[import]
        from wtforms import PasswordField, StringField  # type: ignore[import]

        return {
            "api_endpoint": StringField(lazy_gettext("API Endpoint"), widget=BS3TextFieldWidget()),
            "api_key": StringField(lazy_gettext("API Key"), widget=BS3TextFieldWidget()),
            "api_secret": PasswordField(lazy_gettext("API Secret"), widget=BS3PasswordFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["port", "password", "login", "schema", "extra", "host"],
            "relabeling": {},
            "placeholders": {
                "api_endpoint": "Generate API endpoint from Paradime Workspace settings.",
                "api_key": "Generate API key from Paradime Workspace settings.",
                "api_secret": "Generate API secret from Paradime Workspace settings.",
            },
        }

    def __init__(
        self,
        conn_id: str,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id

    @dataclass
    class AuthConfig:
        api_endpoint: str
        api_key: str
        api_secret: str

    def _get_auth_config(self) -> AuthConfig:
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson
        return self.AuthConfig(
            api_endpoint=extra["api_endpoint"],
            api_key=extra["api_key"],
            api_secret=extra["api_secret"],
        )

    def _get_api_endpoint(self) -> str:
        return self._get_auth_config().api_endpoint

    def _get_request_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-KEY": self._get_auth_config().api_key,
            "X-API-SECRET": self._get_auth_config().api_secret,
        }

    def _raise_for_gql_errors(self, response: requests.Response) -> None:
        response_json = response.json()
        if "errors" in response_json:
            raise Exception(f"{response_json['errors']}")

    def _raise_for_errors(self, response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except Exception as e:
            self.log.error(f"Error: {response.status_code} - {response.text}")
            raise Exception(f"Error: {response.status_code} - {response.text}") from e

        self._raise_for_gql_errors(response)

    def _call_gql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        response = requests.post(
            url=self._get_api_endpoint(),
            json={"query": query, "variables": variables},
            headers=self._get_request_headers(),
            timeout=60,
        )
        self._raise_for_errors(response)

        return response.json()["data"]

    def get_bolt_schedule(self, schedule_name: str) -> BoltSchedule:
        query = """
            query boltScheduleName($scheduleName: String!) {
                boltScheduleName(scheduleName: $scheduleName) {
                    ok
                    latestRunId
                    commands
                    owner
                    schedule
                    uuid
                    source
                }
            }
        """

        response_json = self._call_gql(query=query, variables={"scheduleName": schedule_name})["boltScheduleName"]

        return BoltSchedule(
            name=schedule_name,
            commands=response_json["commands"],
            schedule=response_json["schedule"],
            uuid=response_json["uuid"],
            source=response_json["source"],
            owner=response_json["owner"],
            latest_run_id=response_json["latestRunId"],
        )

    def trigger_bolt_run(self, schedule_name: str, commands: list[str] | None = None) -> int:
        query = """
            mutation triggerBoltRun($scheduleName: String!, $commands: [String!]) {
                triggerBoltRun(scheduleName: $scheduleName, commands: $commands){
                    ok
                    runId
                }
            }
        """
        response_json = self._call_gql(query=query, variables={"scheduleName": schedule_name, "commands": commands})["triggerBoltRun"]

        return response_json["runId"]

    def get_bolt_run_status(self, run_id: int) -> str:
        query = """
            query boltRunStatus($runId: Int!) {
                boltRunStatus(runId: $runId) {
                    state
                }
            }
        """

        response_json = self._call_gql(query=query, variables={"runId": int(run_id)})["boltRunStatus"]

        return response_json["state"]

    def get_bolt_run_commands(self, run_id: int) -> list[BoltCommand]:
        query = """
            query boltRunStatus($runId: Int!) {
                boltRunStatus(runId: $runId) {
                    commands {
                        id
                        command
                        startDttm
                        endDttm
                        stdout
                        stderr
                        returnCode
                    }
                }
            }
        """

        response_json = self._call_gql(query=query, variables={"runId": int(run_id)})["boltRunStatus"]

        commands: list[BoltCommand] = []
        for command_json in response_json["commands"]:
            commands.append(
                BoltCommand(
                    id=command_json["id"],
                    command=command_json["command"],
                    start_dttm=command_json["startDttm"],
                    end_dttm=command_json["endDttm"],
                    stdout=command_json["stdout"],
                    stderr=command_json["stderr"],
                    return_code=command_json["returnCode"],
                )
            )

        return sorted(commands, key=lambda command: command.id)

    def get_artifacts_from_command(self, command_id: int) -> list[BoltResource]:
        query = """
            query boltCommand($commandId: Int!) {
                boltCommand(commandId: $commandId) {
                    resources {
                        id
                        path
                    }
                }
            }
        """

        response_json = self._call_gql(query=query, variables={"commandId": int(command_id)})["boltCommand"]

        artifacts: list[BoltResource] = []
        for artifact_json in response_json["resources"]:
            artifacts.append(
                BoltResource(
                    id=artifact_json["id"],
                    path=artifact_json["path"],
                )
            )

        return artifacts

    def get_artifact_from_command_by_path(self, command_id: int, artifact_path: str) -> BoltResource | None:
        artifacts = self.get_artifacts_from_command(command_id=command_id)
        for artifact in artifacts:
            if artifact.path == artifact_path:
                return artifact

        return None

    def get_artifact_download_url(self, artifact_id: int) -> str:
        query = """
            query boltResourceUrl($resourceId: Int!) {
                boltResourceUrl(resourceId: $resourceId) {
                    ok
                    url
                }
            }
        """

        response_json = self._call_gql(query=query, variables={"resourceId": int(artifact_id)})["boltResourceUrl"]

        return response_json["url"]

    def cancel_bolt_run(self, run_id) -> None:
        query = """
            mutation CancelBoltRun($runId: Int!) {
                cancelBoltRun(runId: $runId) {
                    ok
                    errorLog
                }
            }
        """

        self._call_gql(query=query, variables={"runId": int(run_id)})

    def download_artifact(self, artifact_id: int, output_file_name: str) -> str:
        artifact_url = self.get_artifact_download_url(artifact_id=artifact_id)
        response = requests.get(url=artifact_url, timeout=300)
        response.raise_for_status()

        output_file_path = Path(output_file_name).absolute()

        Path(output_file_path).write_text(response.text)

        return output_file_path.as_posix()

    def get_workspaces(self) -> Any:
        query = """
            query listWorkspaces{
                listWorkspaces{
                    workspaces{
                        name
                        uid
                    }
                }
            }
        """
        response_json = self._call_gql(query=query, variables={})["listWorkspaces"]
        return response_json["workspaces"]

    def get_active_users(self) -> Any:
        query = """
            query listActiveUsers {
                listUsers{
                    activeUsers{
                        uid
                        email
                        name
                        accountType
                    }
                }
            }
        """

        response_json = self._call_gql(query=query, variables={})["listUsers"]

        return response_json["activeUsers"]

    def get_invited_users(self) -> Any:
        query = """
            query listInvitedUsers {
                listUsers{
                    invitedUsers{
                        email
                        accountType
                        inviteStatus
                    }
                }
            }
        """

        response_json = self._call_gql(query=query, variables={})["listUsers"]

        return response_json["invitedUsers"]

    class UserAccountType(Enum):
        ADMIN = "ADMIN"
        DEVELOPER = "DEVELOPER"
        BUSINESS = "BUSINESS"

    def invite_user(self, email: str, account_type: UserAccountType) -> None:
        query = """
            mutation inviteUser($email: String!, $accountType: UserAccountType!) {
                inviteUser(email: $email, accountType: $accountType){
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"email": email, "accountType": account_type.value})

    def update_user_account_type(self, uid: str, account_type: UserAccountType) -> None:
        query = """
            mutation updateUserAccountType($uid: String!, $accountType: UserAccountType!) {
                updateUserAccountType(uid: $uid, accountType: $accountType){
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"uid": uid, "accountType": account_type.value})

    def disable_user(self, uid: str) -> None:
        query = """
            mutation disableUser($uid: String!) {
                disableUser(uid: $uid){
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"uid": uid})
