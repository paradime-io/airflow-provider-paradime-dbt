from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import requests
from airflow.hooks.base import BaseHook  # type: ignore[import]


class ParadimeException(Exception):
    pass


class BoltRunState(Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    SKIPPED = "SKIPPED"

    @classmethod
    def from_str(cls, value: str) -> BoltRunState | None:
        try:
            return BoltRunState(value)
        except ValueError:
            return None


@dataclass
class BoltDeferredSchedule:
    enabled: bool
    deferred_schedule_name: str | None
    successful_run_only: bool


@dataclass
class BoltSchedule:
    name: str
    schedule: str
    owner: str
    last_run_at: str | None
    last_run_state: BoltRunState | None
    next_run_at: str | None
    id: int
    uuid: str
    source: str
    deferred_schedule: BoltDeferredSchedule | None
    turbo_ci: BoltDeferredSchedule | None
    commands: list[str]
    git_branch: str | None
    slack_on: list[str]
    slack_notify: list[str]
    email_on: list[str]
    email_notify: list[str]


@dataclass
class BoltSchedules:
    schedules: list[BoltSchedule]
    total_count: int


@dataclass
class BoltScheduleInfo:
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


class UserAccountType(Enum):
    ADMIN = "ADMIN"
    DEVELOPER = "DEVELOPER"
    BUSINESS = "BUSINESS"


@dataclass
class ActiveUser:
    uid: str
    email: str
    name: str
    account_type: str


@dataclass
class InvitedUser:
    email: str
    account_type: str
    invite_status: str


@dataclass
class Workspace:
    name: str
    uid: str


class ParadimeHook(BaseHook):
    """
    Interact with Paradime API.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "paradime_conn_default"
    conn_type = "paradime"
    hook_name = "Paradime"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """
        Defines connection widgets to add to connection form.
        """

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
        """
        Defines custom field behaviour for the connection form.
        """

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
        """
        Configuration for Paradime authentication.
        """

        api_endpoint: str
        api_key: str
        api_secret: str

    def _get_auth_config(self) -> AuthConfig:
        """
        Get Paradime authentication configuration from Airflow connection.
        """

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
            raise ParadimeException(f"{response_json['errors']}")

    def _raise_for_errors(self, response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except Exception as e:
            self.log.error(f"Error: {response.status_code} - {response.text}")
            raise ParadimeException(f"Error: {response.status_code} - {response.text}") from e

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

    def list_bolt_schedules(self, offset: int, limit: int, show_inactive: bool = False) -> BoltSchedules:
        """
        Get a list of Bolt schedules. The list is paginated. The total count of schedules is also returned.
        Use the offset and limit parameters to page through the list. Set show_inactive to True to get inactive schedules.
        """

        query = """
            query listBoltSchedules($offset: Int!, $limit: Int!, $showInactive: Boolean!) {
                listBoltSchedules(offset: $offset, limit: $limit, showInactive: $showInactive) {
                    schedules {
                        name
                        schedule
                        owner
                        lastRunAt
                        lastRunState
                        nextRunAt
                        id
                        uuid
                        source
                        turboCi {
                            enabled
                            deferredScheduleName
                            successfulRunOnly
                        }
                        deferredSchedule {
                            enabled
                            deferredScheduleName
                            successfulRunOnly
                        }
                        commands
                        gitBranch
                        slackOn
                        slackNotify
                        emailOn
                        emailNotify
                    }
                    totalCount
                }
            }
        """

        response_json = self._call_gql(
            query=query,
            variables={"offset": offset, "limit": limit, "showInactive": show_inactive},
        )["listBoltSchedules"]

        schedules: list[BoltSchedule] = []
        for schedule_json in response_json["schedules"]:
            schedules.append(
                BoltSchedule(
                    name=schedule_json["name"],
                    schedule=schedule_json["schedule"],
                    owner=schedule_json["owner"],
                    last_run_at=schedule_json["lastRunAt"],
                    last_run_state=BoltRunState.from_str(schedule_json["lastRunState"]),
                    next_run_at=schedule_json["nextRunAt"],
                    id=schedule_json["id"],
                    uuid=schedule_json["uuid"],
                    source=schedule_json["source"],
                    deferred_schedule=BoltDeferredSchedule(
                        enabled=schedule_json["deferredSchedule"]["enabled"],
                        deferred_schedule_name=schedule_json["deferredSchedule"]["deferredScheduleName"],
                        successful_run_only=schedule_json["deferredSchedule"]["successfulRunOnly"],
                    )
                    if schedule_json["deferredSchedule"]
                    else None,
                    turbo_ci=BoltDeferredSchedule(
                        enabled=schedule_json["turboCi"]["enabled"],
                        deferred_schedule_name=schedule_json["turboCi"]["deferredScheduleName"],
                        successful_run_only=schedule_json["turboCi"]["successfulRunOnly"],
                    )
                    if schedule_json["turboCi"]
                    else None,
                    commands=schedule_json["commands"],
                    git_branch=schedule_json["gitBranch"],
                    slack_on=schedule_json["slackOn"],
                    slack_notify=schedule_json["slackNotify"],
                    email_on=schedule_json["emailOn"],
                    email_notify=schedule_json["emailNotify"],
                )
            )

        return BoltSchedules(
            schedules=schedules,
            total_count=response_json["totalCount"],
        )

    def get_bolt_schedule(self, schedule_name: str) -> BoltScheduleInfo:
        """
        Get details of a Bolt schedule.
        """

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

        return BoltScheduleInfo(
            name=schedule_name,
            commands=response_json["commands"],
            schedule=response_json["schedule"],
            uuid=response_json["uuid"],
            source=response_json["source"],
            owner=response_json["owner"],
            latest_run_id=response_json["latestRunId"],
        )

    def trigger_bolt_run(self, schedule_name: str, commands: list[str] | None = None) -> int:
        """
        Trigger a Bolt run. Returns the run ID.
        """

        query = """
            mutation triggerBoltRun($scheduleName: String!, $commands: [String!]) {
                triggerBoltRun(scheduleName: $scheduleName, commands: $commands){
                    runId
                }
            }
        """
        response_json = self._call_gql(query=query, variables={"scheduleName": schedule_name, "commands": commands})["triggerBoltRun"]

        return response_json["runId"]

    def get_bolt_run_status(self, run_id: int) -> str:
        """
        Get the status of a Bolt run.
        """
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
        """
        Get the command details of a Bolt run.
        """

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
        """
        Get the artifacts produced by a Bolt command.
        """

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
        """
        Get a specific artifact produced by a Bolt command.
        """

        artifacts = self.get_artifacts_from_command(command_id=command_id)
        for artifact in artifacts:
            if artifact.path == artifact_path:
                return artifact

        return None

    def get_artifact_download_url(self, artifact_id: int) -> str:
        """
        Get the pre-signed s3 download URL for an artifact.
        """

        query = """
            query boltResourceUrl($resourceId: Int!) {
                boltResourceUrl(resourceId: $resourceId) {
                    url
                }
            }
        """

        response_json = self._call_gql(query=query, variables={"resourceId": int(artifact_id)})["boltResourceUrl"]

        return response_json["url"]

    def cancel_bolt_run(self, run_id: int) -> None:
        """
        Cancel a Bolt run.
        """

        query = """
            mutation CancelBoltRun($runId: Int!) {
                cancelBoltRun(runId: $runId) {
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"runId": int(run_id)})

    def download_artifact(self, artifact_id: int, output_file_name: str) -> str:
        """
        Download an artifact to a file. Returns the absolute file path.
        """

        artifact_url = self.get_artifact_download_url(artifact_id=artifact_id)
        response = requests.get(url=artifact_url, timeout=300)
        response.raise_for_status()

        output_file_path = Path(output_file_name).absolute()

        Path(output_file_path).write_text(response.text)

        return output_file_path.as_posix()

    def get_workspaces(self) -> Any:
        """
        Get a list of active workspaces.
        """

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

        workspaces: list[Workspace] = []
        for workspace_json in response_json["workspaces"]:
            workspaces.append(
                Workspace(
                    name=workspace_json["name"],
                    uid=workspace_json["uid"],
                )
            )

        return workspaces

    def get_active_users(self) -> list[ActiveUser]:
        """
        Get a list of active users.
        """

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

        active_users: list[ActiveUser] = []
        for active_user_json in response_json["activeUsers"]:
            active_users.append(
                ActiveUser(
                    uid=active_user_json["uid"],
                    email=active_user_json["email"],
                    name=active_user_json["name"],
                    account_type=active_user_json["accountType"],
                )
            )

        return active_users

    def get_invited_users(self) -> list[InvitedUser]:
        """
        Get a list of invited users and their invite status.
        """

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

        invited_users: list[InvitedUser] = []
        for invited_user_json in response_json["invitedUsers"]:
            invited_users.append(
                InvitedUser(
                    email=invited_user_json["email"],
                    account_type=invited_user_json["accountType"],
                    invite_status=invited_user_json["inviteStatus"],
                )
            )

        return invited_users

    def invite_user(self, email: str, account_type: UserAccountType) -> None:
        """
        Invite a user to the workspace.
        """

        query = """
            mutation inviteUser($email: String!, $accountType: UserAccountType!) {
                inviteUser(email: $email, accountType: $accountType){
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"email": email, "accountType": account_type.value})

    def update_user_account_type(self, uid: str, account_type: UserAccountType) -> None:
        """
        Update a user's account type.
        """

        query = """
            mutation updateUserAccountType($uid: String!, $accountType: UserAccountType!) {
                updateUserAccountType(uid: $uid, accountType: $accountType){
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"uid": uid, "accountType": account_type.value})

    def disable_user(self, uid: str) -> None:
        """
        Disable a user.
        """

        query = """
            mutation disableUser($uid: String!) {
                disableUser(uid: $uid){
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"uid": uid})
