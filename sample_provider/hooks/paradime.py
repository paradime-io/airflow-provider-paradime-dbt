from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

from typing import Any

from airflow.hooks.base import BaseHook
import requests


class ParadimeHook(BaseHook):
    conn_name_attr = "conn_id"
    default_conn_name = "paradime_conn_default" 
    conn_type = "paradime"
    hook_name = "Paradime"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

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
                "api_endpoint": "https://api.paradime.io/api/v1/....",
                "api_key": "You can generate the API key from the Paradime workspace settings...",
                "api_secret": "You can generate the API secret from the Paradime workspace settings...",
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

    def get_auth_config(self) -> AuthConfig:
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson
        return self.AuthConfig(
            api_endpoint=extra["api_endpoint"],
            api_key=extra["api_key"],
            api_secret=extra["api_secret"],
        )
    
    def get_api_endpoint(self) -> str:
        return self.get_auth_config().api_endpoint
    
    def get_request_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-KEY": self.get_auth_config().api_key,
            "X-API-SECRET": self.get_auth_config().api_secret,
        }

    def _raise_for_gql_errors(self, request: requests.Response) -> None:
        response_json = request.json()
        if "errors" in response_json:
            raise Exception(f"{response_json['errors']}")

    def _raise_for_errors(self, response: requests.Response) -> None:
        response.raise_for_status()
        self._raise_for_gql_errors(response)

    def trigger_schedule_run(self, schedule_name: str) -> int:
        query = """
            mutation trigger($scheduleName: String!) {
                triggerBoltRun(scheduleName: $scheduleName){
                    runId
                }
            }
        """

        response = requests.post(
            url=self.get_api_endpoint(),
            json={"query": query, "variables": {"scheduleName": schedule_name}},
            headers=self.get_request_headers(),
        )
        self._raise_for_errors(response)

        run_id = response.json()["data"]["triggerBoltRun"]["runId"]

        return run_id

    def get_schedule_run_status(self, run_id: int) -> str:
        query = """
            query boltRunStatus($runId: Int!) {
                boltRunStatus(runId: $runId) {
                    state
                }
            }
        """

        response = requests.post(
            url=self.get_api_endpoint(),
            json={"query": query, "variables": {"runId": int(run_id)}}, 
            headers=self.get_request_headers(),
        )
        self._raise_for_errors(response)

        state = response.json()["data"]["boltRunStatus"]["state"]

        return state

    @dataclass
    class BoltCommand:
        id: int
        command: str

    def get_bolt_run_commands(self, run_id: int) -> list[BoltCommand]:
        query = """
            query boltRunStatus($runId: Int!) {
                boltRunStatus(runId: $runId) {
                    commands {
                        id
                        command
                    }
                }
            }
        """

        response = requests.post(
            url=self.get_api_endpoint(),
            json={"query": query, "variables": {"runId": int(run_id)}}, 
            headers=self.get_request_headers(),
        )
        self._raise_for_errors(response)

        commands = [self.BoltCommand(id=command["id"], command=command["command"]) for command in response.json()["data"]["boltRunStatus"]["commands"]]

        return sorted(commands, key=lambda command: command.id)
    
    def get_artifact_from_command(self, command: BoltCommand, artifact_path: str) -> int | None:
        self.log.info(f"Searching for artifact {artifact_path!r} in command {command.id!r} - {command.command!r}")

        query = """
            query BoltCommand($commandId: Int!) {
                boltCommand(commandId: $commandId) {
                    resources {
                        id
                        path
                    }
                }
            }
        """

        response = requests.post(
            url=self.get_api_endpoint(),
            json={"query": query, "variables": {"commandId": int(command.id)}}, 
            headers=self.get_request_headers(),
        )
        self._raise_for_errors(response)

        resources = response.json()["data"]["boltCommand"]["resources"]
        for resource in resources:
            if resource["path"] == artifact_path:
                self.log.info(f"Found artifact {artifact_path!r} in command {command.id!r} - {command.command!r}")
                return resource["id"]

        self.log.info(f"Could not find artifact {artifact_path!r} in command {command.id!r} - {command.command!r}")

        return None

    def get_artifact_from_commands(self, artifact_path: str, commands: list[BoltCommand]) -> int | None:
        for command in commands:
            artifact_id = self.get_artifact_from_command(command=command, artifact_path=artifact_path)
            if artifact_id is not None:
                return artifact_id

        return None
    
    def get_artifact_url(self, artifact_id: int) -> str:
        query = """
            query BoltResourceUrl($resourceId: Int!) {
                boltResourceUrl(resourceId: $resourceId) {
                    ok
                    url
                }
            }
        """

        response = requests.post(
            url=self.get_api_endpoint(),
            json={"query": query, "variables": {"resourceId": int(artifact_id)}}, 
            headers=self.get_request_headers(),
        )
        self._raise_for_errors(response)

        url = response.json()["data"]["boltResourceUrl"]["url"]
        return url

    def download_artifact(self, artifact_url: str, file_name: str) -> None:
        response = requests.get(url=artifact_url)
        response.raise_for_status()

        Path(file_name).write_text(response.text)
