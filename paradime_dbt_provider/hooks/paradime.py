from __future__ import annotations

import warnings
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import requests
from airflow.hooks.base import BaseHook  # type: ignore[import]


class ParadimeException(Exception):
    pass


# Prefixes of bearer-token style API keys. An account API key (``prdm_cmp_``) can
# access several workspaces, so every request must pick its target workspace with
# the ``X-Paradime-Workspace`` header. A workspace token (``prdm_wsp_``) is already
# scoped to one workspace, same as a legacy key/secret pair. Mirrors the constants
# in paradime-python-sdk's API client.
ACCOUNT_API_KEY_PREFIX = "prdm_cmp_"
WORKSPACE_API_TOKEN_PREFIX = "prdm_wsp_"

WORKSPACE_SELECTION_HEADER = "X-Paradime-Workspace"


def _is_bearer_token(value: str | None) -> bool:
    """Return True if ``value`` is a bearer token rather than a legacy API key or secret."""
    return bool(value) and value.startswith((ACCOUNT_API_KEY_PREFIX, WORKSPACE_API_TOKEN_PREFIX))  # type: ignore[union-attr]


def _resolve_slug_or_schedule_name(
    *,
    slug: str | None,
    schedule_name: str | None,
    method: str,
) -> str:
    """Resolve the schedule slug from the dual ``slug`` / ``schedule_name`` input.

    Both arguments accept a slug — ``schedule_name`` is the deprecated alias
    kept for backwards compatibility with existing DAGs. Exactly one of the
    two must be provided; ``ValueError`` is raised otherwise. When the caller
    uses the deprecated ``schedule_name`` kwarg, a ``DeprecationWarning`` is
    emitted pointing at their call site.

    Mirrors the same helper in paradime-python-sdk's bolt client. The resolved
    value is sent on the wire as the new ``slug`` field, which both new and
    old backends accept.
    """
    if bool(slug) == bool(schedule_name):
        raise ValueError(f"`{method}` requires exactly one of `slug` or `schedule_name` (deprecated). " "Both fields accept a schedule slug.")
    if schedule_name is not None:
        warnings.warn(
            f"Passing `schedule_name=` to `{method}` is deprecated; use `slug=` instead. " "Both kwargs accept a schedule slug.",
            DeprecationWarning,
            stacklevel=3,
        )
    return slug or schedule_name  # type: ignore[return-value]


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
    slack_on: list[str] | None
    slack_notify: list[str] | None
    email_on: list[str] | None
    email_notify: list[str] | None


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

    The connection authenticates with either an account API key (``prdm_cmp_...``,
    recommended) or a legacy workspace API key + secret pair. An account API key
    can access several workspaces, so each request also needs a workspace token:
    set a default on the connection, or pass ``workspace_token`` here to override it.

    :param conn_id: The Airflow connection id to use when connecting to Paradime.
    :param workspace_token: Optional. The workspace to send requests to when the
        connection uses an account API key. Overrides the ``workspace_token`` set on
        the connection. Not used with workspace API keys, which are bound to one workspace.
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
            "api_token": PasswordField(lazy_gettext("Account API Key"), widget=BS3PasswordFieldWidget()),
            "workspace_token": StringField(lazy_gettext("Default Workspace Token"), widget=BS3TextFieldWidget()),
            "api_key": StringField(lazy_gettext("API Key (legacy)"), widget=BS3TextFieldWidget()),
            "api_secret": PasswordField(lazy_gettext("API Secret (legacy)"), widget=BS3PasswordFieldWidget()),
            "proxy": StringField(lazy_gettext("Proxy"), widget=BS3TextFieldWidget()),
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
                "api_endpoint": "API endpoint shown when you create the API key in Paradime.",
                "api_token": "Account API key (prdm_cmp_...) from Paradime Account settings. Recommended.",
                "workspace_token": "Workspace token (UID) to use when a task does not set workspace_token.",
                "api_key": "Legacy workspace API key. Leave blank when you use an account API key.",
                "api_secret": "Legacy workspace API secret. Leave blank when you use an account API key.",
                "proxy": "Proxy URL (e.g., http://proxy.example.com:8080). Leave blank or set to 'none' to disable.",
            },
        }

    def __init__(
        self,
        conn_id: str,
        workspace_token: str | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.workspace_token = workspace_token

    @dataclass
    class AuthConfig:
        """
        Configuration for Paradime authentication.
        """

        api_endpoint: str
        api_key: str | None
        api_secret: str | None
        proxy: str | None = None
        api_token: str | None = None
        workspace_token: str | None = None

        def get_bearer_token(self) -> str | None:
            """
            Return the bearer token to authenticate with, or None for legacy key/secret auth.

            The token is read from ``api_token``. A token pasted into the legacy
            ``api_secret`` or ``api_key`` field is detected by its prefix, same as
            in paradime-python-sdk.
            """

            if self.api_token:
                return self.api_token
            for value in (self.api_secret, self.api_key):
                if _is_bearer_token(value):
                    return value
            return None

    def _get_auth_config(self) -> AuthConfig:
        """
        Get Paradime authentication configuration from Airflow connection.
        """

        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson

        proxy = extra.get("proxy")

        # Convert proxy to None if it is set to "none" (case insensitive)
        # This is useful to unset the proxy, as Airflow UI does not unset the value by simply leaving it blank.
        if proxy and proxy.lower() == "none":
            proxy = None

        return self.AuthConfig(
            api_endpoint=extra["api_endpoint"],
            api_key=extra.get("api_key") or None,
            api_secret=extra.get("api_secret") or None,
            proxy=proxy,
            api_token=extra.get("api_token") or None,
            workspace_token=extra.get("workspace_token") or None,
        )

    def _get_api_endpoint(self) -> str:
        return self._get_auth_config().api_endpoint

    def _get_request_headers(self) -> dict[str, str]:
        auth_config = self._get_auth_config()
        headers = {"Content-Type": "application/json"}

        bearer_token = auth_config.get_bearer_token()
        if bearer_token is None:
            # Legacy workspace API key + secret. The key is bound to one workspace,
            # so no workspace header is sent.
            if not auth_config.api_key or not auth_config.api_secret:
                raise ParadimeException(
                    f"Paradime connection {self.conn_id!r} has no credentials. "
                    f"Set an account API key ({ACCOUNT_API_KEY_PREFIX}...) in the 'Account API Key' field, "
                    "or a legacy workspace API key and secret."
                )
            headers["X-API-KEY"] = auth_config.api_key
            headers["X-API-SECRET"] = auth_config.api_secret
            return headers

        headers["Authorization"] = f"Bearer {bearer_token}"

        # The task-level workspace token overrides the connection default.
        workspace_token = self.workspace_token or auth_config.workspace_token
        if workspace_token:
            headers[WORKSPACE_SELECTION_HEADER] = workspace_token
        elif bearer_token.startswith(ACCOUNT_API_KEY_PREFIX):
            raise ParadimeException(
                f"Paradime connection {self.conn_id!r} uses an account API key, which needs a workspace token "
                "to know which workspace to call. Pass `workspace_token=` to the task (or ParadimeHook), "
                "or set a 'Default Workspace Token' on the connection."
            )

        return headers

    def _get_proxies(self) -> dict[str, str]:
        """
        Get proxy configuration from auth config.
        """
        auth_config = self._get_auth_config()
        proxies = {}
        if auth_config.proxy:
            proxies["http"] = auth_config.proxy
            proxies["https"] = auth_config.proxy
        return proxies

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
            proxies=self._get_proxies(),
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
                    deferred_schedule=(
                        BoltDeferredSchedule(
                            enabled=schedule_json["deferredSchedule"]["enabled"],
                            deferred_schedule_name=schedule_json["deferredSchedule"]["deferredScheduleName"],
                            successful_run_only=schedule_json["deferredSchedule"]["successfulRunOnly"],
                        )
                        if schedule_json["deferredSchedule"]
                        else None
                    ),
                    turbo_ci=(
                        BoltDeferredSchedule(
                            enabled=schedule_json["turboCi"]["enabled"],
                            deferred_schedule_name=schedule_json["turboCi"]["deferredScheduleName"],
                            successful_run_only=schedule_json["turboCi"]["successfulRunOnly"],
                        )
                        if schedule_json["turboCi"]
                        else None
                    ),
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

    def get_bolt_schedule(
        self,
        schedule_name: str | None = None,
        *,
        slug: str | None = None,
    ) -> BoltScheduleInfo:
        """
        Get details of a Bolt schedule.

        Identify the schedule with ``slug`` (preferred) or ``schedule_name``
        (deprecated alias kept for backwards compatibility — both accept a
        schedule slug). Exactly one of the two must be provided.
        """

        resolved_slug = _resolve_slug_or_schedule_name(slug=slug, schedule_name=schedule_name, method="get_bolt_schedule")

        query = """
            query boltScheduleName($slug: String) {
                boltScheduleName(slug: $slug) {
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

        response_json = self._call_gql(query=query, variables={"slug": resolved_slug})["boltScheduleName"]

        return BoltScheduleInfo(
            name=resolved_slug,
            commands=response_json["commands"],
            schedule=response_json["schedule"],
            uuid=response_json["uuid"],
            source=response_json["source"],
            owner=response_json["owner"],
            latest_run_id=response_json["latestRunId"],
        )

    def trigger_bolt_run(
        self,
        schedule_name: str | None = None,
        commands: list[str] | None = None,
        branch: str | None = None,
        *,
        slug: str | None = None,
    ) -> int:
        """
        Trigger a Bolt run. Returns the run ID.

        Identify the schedule with ``slug`` (preferred) or ``schedule_name``
        (deprecated alias kept for backwards compatibility — both accept a
        schedule slug). Exactly one of the two must be provided.
        """

        resolved_slug = _resolve_slug_or_schedule_name(slug=slug, schedule_name=schedule_name, method="trigger_bolt_run")

        query = """
            mutation triggerBoltRun($slug: String, $commands: [String!], $branch: String) {
                triggerBoltRun(slug: $slug, commands: $commands, branch: $branch) {
                    runId
                }
            }
        """
        response_json = self._call_gql(
            query=query,
            variables={
                "slug": resolved_slug,
                "commands": commands,
                "branch": branch,
            },
        )["triggerBoltRun"]

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
                cancelBoltRun(scheduleRunId: $runId) {
                    ok
                }
            }
        """

        self._call_gql(query=query, variables={"runId": int(run_id)})

    def retry_bolt_schedule_from_failure(
        self,
        schedule_name: str | None = None,
        *,
        slug: str | None = None,
    ) -> int:
        """
        Retry the latest failed run of a Bolt schedule, resuming from its failed command.

        Looks up the most recent run of the schedule and resumes it from the
        point of failure, re-running only the failed commands. Returns the new
        run ID.

        Identify the schedule with ``slug`` (preferred) or ``schedule_name``
        (deprecated alias kept for backwards compatibility — both accept a
        schedule slug). Exactly one of the two must be provided.

        Mirrors ``retry_schedule_from_failure`` in the paradime-python-sdk Bolt client.
        """

        resolved_slug = _resolve_slug_or_schedule_name(slug=slug, schedule_name=schedule_name, method="retry_bolt_schedule_from_failure")

        query = """
            mutation RetryBoltRunFromFailure($slug: String) {
                retryBoltRunFromFailure(slug: $slug) {
                    runId
                }
            }
        """

        response_json = self._call_gql(query=query, variables={"slug": resolved_slug})["retryBoltRunFromFailure"]

        return response_json["runId"]

    def download_artifact(self, artifact_id: int, output_file_name: str) -> str:
        """
        Download an artifact to a file. Returns the absolute file path.
        """

        artifact_url = self.get_artifact_download_url(artifact_id=artifact_id)
        response = requests.get(url=artifact_url, proxies=self._get_proxies(), timeout=300)
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
